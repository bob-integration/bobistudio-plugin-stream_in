#!/usr/bin/env python3
# stream_in — ingest d'un flux réseau (SRT/RTMP) vers le bus MXL.
# ffmpeg décode le flux entrant et le CONFORME au format de sortie configuré (scale/fps/
# désentrelacement) ; le feeder genlock écrit une trame par tick de grille PTP dans TOUS les
# états (attente/connecté/gelé) → signal MXL continu, jamais de trou, y compris pendant les
# redémarrages ffmpeg. Audio décodé en float32 48 kHz 8ch vers un flux MXL AudioWriter.
import os, json, time, signal, threading, subprocess, collections
import numpy as np
from http.server import HTTPServer, BaseHTTPRequestHandler

import bobimxl   # sortie vidéo (Writer) + audio (AudioWriter) sur le domaine MXL

CONFIG         = {config}            # dict injected at deploy
HOSTNAME       = "{hostname}"
PLUGIN_VERSION = "{plugin_version}"

def _as_bool(v, default=False):
    if v is None: return default
    if isinstance(v, str): return v.strip().lower() in ("1", "true", "yes", "on")
    return bool(v)

def _rate_nd(v):
    """Cadence → (num, den) EXACT. Accepte 25, 29.97, "30000/1001". Fractionnaire NTSC = N*1000/1001."""
    try:
        if isinstance(v, str) and "/" in v:
            a, b = v.split("/"); return int(a), int(b)
        f = float(v or 25)
    except Exception:
        return 25, 1
    n = round(f)
    if abs(f - n) < 0.01:
        return (n or 25), 1
    nominal = round(f * 1001.0 / 1000.0)
    return nominal * 1000, 1001

# ─── config ──────────────────────────────────────────────────────────────────
MODE           = str(CONFIG.get("mode") or "srt_listener")
URL            = str(CONFIG.get("url") or "").strip()
LISTEN_PORT    = int(CONFIG.get("listen_port") or 9000)
SRT_LATENCY_MS = int(CONFIG.get("srt_latency_ms") or 200)
SRT_PASSPHRASE = str(CONFIG.get("srt_passphrase") or "")
SRT_STREAMID   = str(CONFIG.get("srt_streamid") or "")
RTMP_APP       = str(CONFIG.get("rtmp_app") or "live").strip("/") or "live"
RTMP_KEY       = str(CONFIG.get("rtmp_key") or "").strip("/")

WIDTH  = int(CONFIG.get("width") or 1920)
HEIGHT = int(CONFIG.get("height") or 1080)
FPS_NUM, FPS_DEN = _rate_nd(CONFIG.get("fps") or 50)
FPS    = max(1, round(FPS_NUM / FPS_DEN))
GENLOCK = _as_bool(CONFIG.get("genlock", True), default=True)
GENLOCK_PHASE_MS = float(CONFIG.get("genlock_phase_ms") or 0.0)
IDLE_MODE = str(CONFIG.get("idle_mode") or "bars")
FREEZE_TIMEOUT_MS = int(CONFIG.get("freeze_timeout_ms") or 1000)

# ─── formats shm (identiques au pipeline MXL, cf. player/avsync) ─────────────
CHROMA    = str(CONFIG.get("chroma") or "422")
BIT_DEPTH = int(CONFIG.get("bit_depth") or 8)
_DEEP = BIT_DEPTH >= 10
_BPS  = 2 if _DEEP else 1
_CW = {{"420": 2, "422": 2, "444": 1}}.get(CHROMA, 2)
_CH = {{"420": 2, "422": 1, "444": 1}}.get(CHROMA, 1)
# pix_fmt ffmpeg du plan shm (planar YUV, LE en 10/12 bits) — sortie rawvideo byte-identique au ring.
PIX_FMT = ({{"420": "yuv420p", "422": "yuv422p", "444": "yuv444p"}}.get(CHROMA, "yuv422p")
           + (("12le" if BIT_DEPTH >= 12 else "10le") if _DEEP else ""))
UV_W = WIDTH // _CW
UV_H = HEIGHT // _CH
Y_SIZE       = WIDTH * HEIGHT * _BPS
UV_SIZE      = UV_W * UV_H * _BPS
V_FRAME_SIZE = Y_SIZE + 2 * UV_SIZE

# Balayage de SORTIE : config scan p|i + field_order tff|bff → contrat Writer MXL (1 grain = 1 champ
# en entrelacé, index champ 2k/2k+1, cf. player). FPS config = cadence TRAME (1080i50 → fps 25).
_SCAN_CFG   = str(CONFIG.get("scan") or "p").strip().lower()
FIELD_ORDER = "bff" if str(CONFIG.get("field_order") or "tff").strip().lower() == "bff" else "tff"
INTERLACED  = _SCAN_CFG == "i"
SCAN = ("interlaced_" + FIELD_ORDER) if INTERLACED else "progressive"
_PLANES = [(Y_SIZE, WIDTH * _BPS), (UV_SIZE, UV_W * _BPS), (UV_SIZE, UV_W * _BPS)]

A_SAMPLE_RATE = 48000
A_CHANNELS    = 8
A_SPMS        = A_SAMPLE_RATE // 1000        # samples per ms (48)

inst = bobimxl.Instance()
VIDEO_SHM = HOSTNAME
AUDIO_SHM = HOSTNAME + "_audio"
AUDIO_FIFO = "/tmp/stream_in_audio.f32"

# ─── état partagé ────────────────────────────────────────────────────────────
metrics_lock = threading.Lock()
metrics = {{"fps": 0.0, "frame_index": 0, "plugin_version": PLUGIN_VERSION,
           "conn_state": "waiting", "mode": MODE, "listen_port": LISTEN_PORT,
           "last_frame_age_ms": None, "ffmpeg_restarts": 0, "video_max_gap_ms": 0.0,
           "in_fps": 0.0}}

frame_lock = threading.Lock()
_last_frame = None            # bytes de la dernière trame conformée reçue
_last_frame_mono = None       # time.monotonic() de réception
_frames_in = 0                # compteur trames reçues (fenêtre in_fps)

audio_lock = threading.Lock()
_audio_buf = collections.deque()   # blocs float32 (n, 8) en attente, borné ~200 ms
_AUDIO_BUF_MAX = A_SAMPLE_RATE // 5

_proc = None                  # process ffmpeg courant (supervisé)
_proc_lock = threading.Lock()
_reconnect_req = threading.Event()
_reconnect_req_audio = threading.Event()   # /reconnect ré-arme l'essai audio
_stderr_tail = collections.deque(maxlen=12)

bus_error = threading.Event()
def _handle_sigbus(signum, frame):
    # shm recréé sous nos pieds (redémarrage domaine) → recréer le Writer, jamais crasher.
    bus_error.set()
signal.signal(signal.SIGBUS, _handle_sigbus)

# ─── grille genlock (grille PTP rationnelle, cf. player) ─────────────────────
_PHASE_NS = round(GENLOCK_PHASE_MS * 1e6)
def _grid_ns(k):
    return round(k * 1_000_000_000 * FPS_DEN / FPS_NUM) + _PHASE_NS
def _grid_k(ns):
    return (ns - _PHASE_NS) * FPS_NUM // (1_000_000_000 * FPS_DEN)
def _grid_now_ns():
    return time.clock_gettime_ns(time.CLOCK_REALTIME)
def _grid_sleep_until(deadline_ns):
    dt = (deadline_ns - _grid_now_ns()) / 1e9
    if dt > 0: time.sleep(dt)

def _split_fields(full_b):
    """TRAME planar pleine → (champ_1er, champ_2nd). tff → 1er champ = lignes paires."""
    bff = FIELD_ORDER == "bff"
    a = bytearray(); b = bytearray(); off = 0
    for (psz, rb) in _PLANES:
        pl = np.frombuffer(full_b, dtype=np.uint8, count=psz, offset=off).reshape(-1, rb)
        ev = pl[0::2]; od = pl[1::2]
        if bff:
            a += od.tobytes(); b += ev.tobytes()
        else:
            a += ev.tobytes(); b += od.tobytes()
        off += psz
    return bytes(a), bytes(b)

def _write_video(w, frame_bytes, fbase):
    try:
        if INTERLACED:
            f0, f1 = _split_fields(frame_bytes)
            for fi, fb in ((2 * fbase, f0), (2 * fbase + 1, f1)):
                _i, gi, vw = w.open_grain(index=fi)
                n = min(len(fb), len(vw)); vw[:n] = np.frombuffer(fb, dtype=np.uint8, count=n)
                w.commit(gi)
        else:
            _i, gi, vw = w.open_grain(index=(fbase if GENLOCK else None))
            n = min(len(frame_bytes), len(vw))
            vw[:n] = np.frombuffer(frame_bytes, dtype=np.uint8, count=n)
            w.commit(gi)
    except Exception:
        pass

# ─── trames d'attente (mire / noir), générées une fois au format de sortie ──
_NP_DT   = np.uint16 if _DEEP else np.uint8
_SCALE   = 1 << (BIT_DEPTH - 8)
_MAXV    = (1 << BIT_DEPTH) - 1

def _yuv601(r, g, b):
    y = 0.257 * r + 0.504 * g + 0.098 * b + 16.0
    u = -0.148 * r - 0.291 * g + 0.439 * b + 128.0
    v = 0.439 * r - 0.368 * g - 0.071 * b + 128.0
    return y, u, v

def _flat_frame(cols_rgb):
    """Barres verticales pleines hauteur → trame planar au format de sortie exact."""
    n = len(cols_rgb)
    yl = np.empty(WIDTH, dtype=np.float32); ul = np.empty(WIDTH, dtype=np.float32)
    vl = np.empty(WIDTH, dtype=np.float32)
    for i, (r, g, b) in enumerate(cols_rgb):
        x0 = WIDTH * i // n; x1 = WIDTH * (i + 1) // n
        y, u, v = _yuv601(r, g, b)
        yl[x0:x1] = y; ul[x0:x1] = u; vl[x0:x1] = v
    def q(line):
        return np.clip(line * _SCALE, 0, _MAXV).astype(_NP_DT)
    yp = np.tile(q(yl), (HEIGHT, 1))
    up = np.tile(q(ul[:: _CW]), (UV_H, 1))
    vp = np.tile(q(vl[:: _CW]), (UV_H, 1))
    return yp.tobytes() + up.tobytes() + vp.tobytes()

# Barres 75 % (blanc, jaune, cyan, vert, magenta, rouge, bleu, noir)
_BARS75 = [(191, 191, 191), (191, 191, 0), (0, 191, 191), (0, 191, 0),
           (191, 0, 191), (191, 0, 0), (0, 0, 191), (16, 16, 16)]
BARS_FRAME  = _flat_frame(_BARS75)
BLACK_FRAME = _flat_frame([(16, 16, 16)])
IDLE_FRAME  = BARS_FRAME if IDLE_MODE == "bars" else BLACK_FRAME

# ─── commande ffmpeg par mode ────────────────────────────────────────────────
def _input_url():
    if MODE == "srt_listener":
        q = "mode=listener&latency=%d" % (SRT_LATENCY_MS * 1000)
        if SRT_PASSPHRASE: q += "&passphrase=" + SRT_PASSPHRASE
        if SRT_STREAMID:   q += "&streamid=" + SRT_STREAMID
        return "srt://0.0.0.0:%d?%s" % (LISTEN_PORT, q)
    if MODE == "srt_caller":
        base = URL.split("?", 1)[0]
        q = "mode=caller&latency=%d" % (SRT_LATENCY_MS * 1000)
        if SRT_PASSPHRASE: q += "&passphrase=" + SRT_PASSPHRASE
        if SRT_STREAMID:   q += "&streamid=" + SRT_STREAMID
        return base + "?" + q
    if MODE == "rtmp_server":
        return "rtmp://0.0.0.0:%d/%s/%s" % (LISTEN_PORT, RTMP_APP, RTMP_KEY or "stream")
    return URL   # rtmp_pull : URL fournie telle quelle

def _ffmpeg_cmd(with_audio=True):
    # bwdif deint=interlaced : ne désentrelace QUE les trames marquées entrelacées (passthrough
    # sinon) → chaîne unique quel que soit le flux entrant. fps=grille de sortie (cadence TRAME).
    vf = ("bwdif=mode=send_frame:parity=auto:deint=interlaced,"
          "scale=%d:%d:flags=bicubic,fps=%d/%d,format=%s,setsar=1"
          % (WIDTH, HEIGHT, FPS_NUM, FPS_DEN, PIX_FMT))
    cmd = ["ffmpeg", "-nostdin", "-hide_banner", "-loglevel", "warning",
           "-analyzeduration", "10M", "-probesize", "10M"]
    if MODE == "rtmp_server":
        cmd += ["-listen", "1"]
    cmd += ["-i", _input_url(),
            "-map", "0:v:0", "-vf", vf, "-f", "rawvideo", "pipe:1"]
    if with_audio:
        # `0:a:0?` tolère l'absence de piste au niveau du mapping, mais une sortie SANS stream
        # fait échouer ffmpeg au démarrage → le superviseur retombe en vidéo seule (cf. bascule).
        cmd += ["-map", "0:a:0?", "-ac", str(A_CHANNELS), "-ar", str(A_SAMPLE_RATE),
                "-f", "f32le", "-y", AUDIO_FIFO]
    return cmd

# ─── lecteurs de sortie ffmpeg (ne bloquent jamais le feeder) ────────────────
def _video_reader(proc):
    global _last_frame, _last_frame_mono, _frames_in
    buf = bytearray()
    stdout = proc.stdout
    while proc.poll() is None:
        try:
            chunk = stdout.read(V_FRAME_SIZE - len(buf) if len(buf) < V_FRAME_SIZE else V_FRAME_SIZE)
        except Exception:
            break
        if not chunk:
            break
        buf += chunk
        while len(buf) >= V_FRAME_SIZE:
            fb = bytes(buf[:V_FRAME_SIZE]); del buf[:V_FRAME_SIZE]
            with frame_lock:
                _last_frame = fb
                _last_frame_mono = time.monotonic()
                _frames_in += 1

def _audio_reader():
    """Ouvre le fifo audio en boucle (rendez-vous bloquant OK : thread dédié, jamais sur le chemin
    vidéo). Blocs f32le 8ch → deque bornée consommée par audio_feeder."""
    blk = A_SPMS * A_CHANNELS * 4 * 10          # 10 ms
    while True:
        try:
            with open(AUDIO_FIFO, "rb") as f:
                while True:
                    data = f.read(blk)
                    if not data:
                        break                    # ffmpeg (écrivain) parti → réouvrir
                    n = len(data) // (A_CHANNELS * 4)
                    if n <= 0: continue
                    arr = np.frombuffer(data, dtype=np.float32,
                                        count=n * A_CHANNELS).reshape(n, A_CHANNELS)
                    with audio_lock:
                        _audio_buf.append(arr)
                        tot = sum(a.shape[0] for a in _audio_buf)
                        while tot > _AUDIO_BUF_MAX and _audio_buf:
                            tot -= _audio_buf.popleft().shape[0]
        except Exception:
            time.sleep(0.5)

def _stderr_drain(proc):
    for line in iter(proc.stderr.readline, b""):
        try:
            s = line.decode("utf-8", "replace").strip()
            if s: _stderr_tail.append(s)
        except Exception:
            pass

# ─── superviseur ffmpeg (poll + restart, backoff) ────────────────────────────
def supervisor():
    global _proc
    restarts = 0
    # Gel prolongé en caller/pull = connexion zombie → kill (le restart re-tente). Les modes
    # écoute (listener/server) restent en attente passive, pas de stall-kill.
    stall_kill_ms = max(10_000, FREEZE_TIMEOUT_MS * 10)
    with_audio = True
    while True:
        try:
            proc = subprocess.Popen(_ffmpeg_cmd(with_audio), stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE, bufsize=V_FRAME_SIZE)
        except Exception as e:
            _stderr_tail.append("spawn: %s" % e)
            time.sleep(2); continue
        with _proc_lock:
            _proc = proc
        threading.Thread(target=_video_reader, args=(proc,), daemon=True).start()
        threading.Thread(target=_stderr_drain, args=(proc,), daemon=True).start()
        got_any = False
        while proc.poll() is None:
            if _reconnect_req.is_set():
                _reconnect_req.clear()
                try: proc.kill()
                except Exception: pass
                break
            with frame_lock:
                lm = _last_frame_mono
            if lm is not None:
                age = (time.monotonic() - lm) * 1000.0
                if age < FREEZE_TIMEOUT_MS: got_any = True
                if (got_any and MODE in ("srt_caller", "rtmp_pull")
                        and age > stall_kill_ms):
                    _stderr_tail.append("stall %.0f ms → restart" % age)
                    try: proc.kill()
                    except Exception: pass
                    break
            time.sleep(0.3)
        try: proc.wait(timeout=5)
        except Exception:
            try: proc.kill()
            except Exception: pass
        restarts += 1
        with metrics_lock:
            metrics["ffmpeg_restarts"] = restarts
        # Flux sans piste audio : la sortie fifo n'a aucun stream et ffmpeg refuse de démarrer
        # → bascule vidéo seule (ré-armé au prochain déploiement/reconnect).
        if with_audio and any("does not contain any stream" in s for s in _stderr_tail):
            with_audio = False
            _stderr_tail.append("pas de piste audio dans le flux → ingest vidéo seule")
        elif _reconnect_req_audio.is_set():
            _reconnect_req_audio.clear()
            with_audio = True
        time.sleep(0.5 if got_any else 2.0)     # backoff léger ; plus long si jamais connecté

# ─── feeder vidéo genlock (MAÎTRE : une trame par tick, toujours) ────────────
def video_feeder():
    global _last_frame_mono
    shm = bobimxl.Writer(inst, VIDEO_SHM, WIDTH, HEIGHT, CHROMA, BIT_DEPTH, FPS_NUM, FPS_DEN,
                         index_mode=("tai" if GENLOCK else "free"), interlace=SCAN)
    idx = 0
    period_ns = round(1e9 * FPS_DEN / FPS_NUM)
    kf = _grid_k(_grid_now_ns()) + 2
    win_cnt = 0; win_t0 = _grid_now_ns(); win_max_gap = 0; last_w = win_t0
    win_in0 = 0
    while True:
        if bus_error.is_set():
            bus_error.clear()
            try: shm.close()
            except Exception: pass
            time.sleep(1)
            shm = bobimxl.Writer(inst, VIDEO_SHM, WIDTH, HEIGHT, CHROMA, BIT_DEPTH,
                                 FPS_NUM, FPS_DEN,
                                 index_mode=("tai" if GENLOCK else "free"), interlace=SCAN)
            kf = _grid_k(_grid_now_ns()) + 2
        G = _grid_ns(kf)
        if GENLOCK:
            _grid_sleep_until(G)
        else:
            time.sleep(max(0.0, period_ns / 1e9))
        with frame_lock:
            frame = _last_frame; lm = _last_frame_mono; fin = _frames_in
        age_ms = None if lm is None else (time.monotonic() - lm) * 1000.0
        if frame is None:
            frame = IDLE_FRAME; conn = "waiting"
        elif age_ms is not None and age_ms > FREEZE_TIMEOUT_MS:
            conn = "frozen"
        else:
            conn = "connected"
        _write_video(shm, frame, kf); idx += 1
        now = _grid_now_ns()
        gap = now - last_w; last_w = now
        if gap > win_max_gap: win_max_gap = gap
        win_cnt += 1
        with metrics_lock:
            metrics["frame_index"] = idx
            metrics["conn_state"] = conn
            metrics["last_frame_age_ms"] = None if age_ms is None else round(age_ms)
            if now - win_t0 >= 1_000_000_000:
                el = (now - win_t0) / 1e9
                metrics["fps"] = round(win_cnt / el, 1)
                metrics["in_fps"] = round((fin - win_in0) / el, 1)
                metrics["video_max_gap_ms"] = round(win_max_gap / 1e6, 1)
        if now - win_t0 >= 1_000_000_000:
            win_cnt = 0; win_t0 = now; win_max_gap = 0; win_in0 = fin
        kf += 1
        nk = _grid_k(_grid_now_ns())
        if kf <= nk:                      # stall système → ré-aligne sans rafale
            kf = nk + 1

# ─── feeder audio (silence cadencé si pas d'audio frais — jamais starver) ────
def audio_feeder():
    aw = bobimxl.AudioWriter(inst, AUDIO_SHM, channels=A_CHANNELS, sample_rate=A_SAMPLE_RATE,
                             index_mode=("tai" if GENLOCK else "free"))
    silence = np.zeros((A_SPMS, A_CHANNELS), dtype=np.float32)
    pending = None                        # reliquat du bloc en cours
    _aclk = time.time if GENLOCK else time.monotonic
    t0 = _aclk(); written_ms = 0
    while True:
        due = int((_aclk() - t0) * 1000)
        guard = 0
        while written_ms < due and guard < 2000:
            need = A_SPMS
            out = np.empty((A_SPMS, A_CHANNELS), dtype=np.float32); pos = 0
            with audio_lock:
                while need > 0:
                    if pending is not None and len(pending):
                        take = min(need, len(pending))
                        out[pos:pos + take] = pending[:take]
                        pending = pending[take:] if take < len(pending) else None
                        pos += take; need -= take
                    elif _audio_buf:
                        pending = _audio_buf.popleft()
                    else:
                        break
            if need > 0:                  # pas assez de frais → compléter en silence
                if pos == 0:
                    out = silence
                else:
                    out[pos:] = 0.0
            try: aw.write(out)
            except Exception: pass
            written_ms += 1; guard += 1
        time.sleep(0.003)

# ─── HTTP : :8080 metrics + :8082 control ────────────────────────────────────
def _state_payload():
    with metrics_lock: m = dict(metrics)
    m.update({{"video_shm": VIDEO_SHM, "audio_shm": AUDIO_SHM,
              "width": WIDTH, "height": HEIGHT, "fps_cfg": FPS,
              "chroma": CHROMA, "bit_depth": BIT_DEPTH, "scan": _SCAN_CFG,
              "mode": MODE, "url": URL, "listen_port": LISTEN_PORT,
              "rtmp_app": RTMP_APP, "rtmp_key": RTMP_KEY,
              "idle_mode": IDLE_MODE, "freeze_timeout_ms": FREEZE_TIMEOUT_MS,
              "genlock": GENLOCK, "last_errors": list(_stderr_tail)}})
    return m

class MetricsHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        with metrics_lock: payload = dict(metrics)
        body = json.dumps(payload).encode()
        self.send_response(200); self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body))); self.end_headers()
        self.wfile.write(body)
    def log_message(self, *a): pass

class ControlHandler(BaseHTTPRequestHandler):
    def _send_json(self, code, obj):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)
    def do_GET(self):
        if self.path == "/state":
            self._send_json(200, _state_payload())
        else:
            self.send_response(404); self.end_headers()
    def do_POST(self):
        if self.path == "/reconnect":
            _reconnect_req_audio.set()
            _reconnect_req.set()
            self._send_json(200, {{"ok": True}})
        else:
            self.send_response(404); self.end_headers()
    def log_message(self, *a): pass

# ─── boot ────────────────────────────────────────────────────────────────────
try:
    if os.path.exists(AUDIO_FIFO): os.unlink(AUDIO_FIFO)
    os.mkfifo(AUDIO_FIFO)
except Exception:
    pass

threading.Thread(target=lambda: HTTPServer(("0.0.0.0", 8080), MetricsHandler).serve_forever(), daemon=True).start()
threading.Thread(target=lambda: HTTPServer(("0.0.0.0", 8082), ControlHandler).serve_forever(), daemon=True).start()
threading.Thread(target=_audio_reader, daemon=True).start()
threading.Thread(target=supervisor, daemon=True).start()
threading.Thread(target=video_feeder, daemon=True).start()
threading.Thread(target=audio_feeder, daemon=True).start()

while True:
    time.sleep(1)
