#!/usr/bin/env python3
# stream_in — ingest d'un flux réseau (SRT/RTMP) vers le bus MXL.
# ffmpeg décode le flux entrant et le CONFORME au format de sortie configuré (scale/fps/
# désentrelacement) ; le feeder genlock écrit une trame par tick de grille PTP dans TOUS les
# états (attente/connecté/gelé) → signal MXL continu, jamais de trou, y compris pendant les
# redémarrages ffmpeg. Audio décodé en float32 48 kHz 8ch vers un flux MXL AudioWriter.
import os, re, json, time, signal, threading, subprocess, collections, urllib.parse
import numpy as np
from http.server import HTTPServer, BaseHTTPRequestHandler

import bobimxl   # sortie vidéo (Writer) + audio (AudioWriter) sur le domaine MXL


def _mxl_lib_state():
    """Variante libmxl réellement chargée (baseline / x86-64-v3) — diagnostic seul, ne doit
    JAMAIS faire échouer /state."""
    try:
        return bobimxl.lib_info()
    except Exception:
        return None

CONFIG         = {config}            # dict injected at deploy
HOSTNAME       = "{hostname}"
PLUGIN_VERSION = "{plugin_version}"

# ─── Niveau de log ─────────────────────────────────────────────────────────
# `log_level` (config_schema du plugin, défaut « info ») filtre les impressions du script.
# Le critère n'est PAS « verbeux vs silencieux » mais ÉVÉNEMENT vs MÉTRIQUE :
#   debug   — le lance-flammes : par trame, par bande, décisions internes
#   info    — ÉVÉNEMENTS rares et signifiants  ← DÉFAUT (toujours visible) : démarrage/
#             arrêt, session ouverte/fermée, changement de format, reconnexion, repli sur
#             un chemin dégradé, entrée qui apparaît/disparaît, rebascule.
#   warning — anomalies et replis subis
#   error   — échecs
# RÈGLE 1 : après une panne, le journal PAR DÉFAUT doit permettre de RECONSTITUER
#   l'histoire. Élever le niveau après coup ne récupère RIEN : ce qui n'a pas été écrit
#   est perdu. On ne coupe donc pas l'information, on coupe la redondance.
# RÈGLE 2 : une MÉTRIQUE PÉRIODIQUE (fps, compteurs) ne se journalise PAS — elle est déjà
#   publiée sur :8080 et échantillonnée par l'orchestrateur. La journaliser duplique la
#   mesure ET consomme la fenêtre de rétention (journal Docker non roté : le bruit purge
#   les lignes utiles anciennes). Au mieux `debug`.
# RÈGLE 3 : un événement qui peut partir EN RAFALE s'AGRÈGE sur une fenêtre et sort en UNE
#   ligne périodique (« N frames lentes sur la dernière minute, pire … ») — le signal
#   reste, le spam disparaît.
# Réglable à chaud, sans redéployer, quand le plugin expose l'endpoint de contrôle :
# POST :8082/log_level {{"level": "debug"}} (exposé aux macros via param_tree/actions).
_LOG_ORDER = {{"debug": 10, "info": 20, "warning": 30, "error": 40}}
LOG_LEVEL = str(CONFIG.get("log_level") or "info").strip().lower()
if LOG_LEVEL not in _LOG_ORDER:
    LOG_LEVEL = "info"
_LOG_MIN = _LOG_ORDER[LOG_LEVEL]


def log(msg, niveau="info"):
    """Impression gatée par le niveau de log courant (défaut du message : « info »)."""
    if _LOG_ORDER.get(niveau, 20) >= _LOG_MIN:
        print(msg, flush=True)


def set_log_level(niveau):
    """Change le niveau à chaud. Renvoie True si le niveau est reconnu."""
    global LOG_LEVEL, _LOG_MIN
    lv = str(niveau or "").strip().lower()
    if lv not in _LOG_ORDER:
        return False
    LOG_LEVEL, _LOG_MIN = lv, _LOG_ORDER[lv]
    return True



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
MODE = str(CONFIG.get("mode") or "srt_listener")

# Le POINT D'ENTRÉE est modifiable à chaud (:8082/source) : il vit dans un dict sous verrou,
# pas dans des constantes. Le format de sortie, lui, est structurel (redéploiement).
src_lock = threading.Lock()
_src = {{
    "url":            str(CONFIG.get("url") or "").strip(),
    "listen_port":    int(CONFIG.get("listen_port") or 9000),
    "srt_latency_ms": int(CONFIG.get("srt_latency_ms") or 200),
    "srt_passphrase": str(CONFIG.get("srt_passphrase") or ""),
    "srt_streamid":   str(CONFIG.get("srt_streamid") or ""),
    "rtmp_app":       str(CONFIG.get("rtmp_app") or "live").strip("/") or "live",
    "rtmp_key":       str(CONFIG.get("rtmp_key") or "").strip("/"),
}}
ENABLED = _as_bool(CONFIG.get("enabled", True), default=True)

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

# ── MODE TRANCHE (chantier latence sous-trame, cf. patch mxl-planar-slices) ─────────────────
# slice_mode=true → la trame de sortie (déjà COMPLÈTE : décodée/conformée par ffmpeg, ou mire/
# noir/gel) est émise PAR BANDES avec commit progressif (validSlices=1..N, pattern delay
# _emit_frame_sliced) → l'aval slice-aware démarre sur la 1ʳᵉ bande au lieu d'attendre la
# trame. Les lignes arrivent du pipe ffmpeg par trame ENTIÈRE : le gain vient de la
# parallélisation écriture/lecture aval, pas du décodage. TOUS les chemins d'émission
# (connecté, mire, noir, gel) passent par _write_video → un seul point à trancher, commit
# FINAL garanti (try/finally). Le contrat genlock/index et le pacing NE CHANGENT PAS.
# Éligibilité : sortie PROGRESSIVE + hauteur divisible (algo delay _out_slice_h) ;
# entrelacé → repli whole-frame. slice_mode absent/False → octet-identique à l'historique.
SLICE_MODE  = _as_bool(CONFIG.get("slice_mode"), default=False)
SLICE_LINES = max(1, int(CONFIG.get("slice_lines") or 36))
_SLICE_TARGET = max(1, 1080 // SLICE_LINES)

def _out_slice_h(oh, ch=1):
    """MODE TRANCHE — slice_height de la sortie (même algo que delay/pyramide) : le plus petit
    diviseur sh de oh avec sh ≥ max(1, oh // _SLICE_TARGET), MULTIPLE de ch (lignes chroma
    entières par bande). Aucun diviseur raisonnable (sh > oh//2) → 0 = inéligible."""
    lo = max(1, oh // _SLICE_TARGET)
    for sh in range(lo, oh // 2 + 1):
        if oh % sh == 0 and sh % ch == 0:
            return sh
    return 0

V_SLICE_H = _out_slice_h(HEIGHT, _CH) if (SLICE_MODE and not INTERLACED) else 0
_V_TOTAL_SLICES = (HEIGHT // V_SLICE_H) if V_SLICE_H else 1
# kwargs Writer : le flowDef porte slice_height quand éligible, sinon inchangé (historique).
_SLICE_KW = {{"slice_height": V_SLICE_H}} if V_SLICE_H else {{}}
_Y_ROW  = WIDTH * _BPS
_UV_ROW = UV_W * _BPS
if SLICE_MODE:
    log("slice_mode %s : sortie %s %s" % ("actif" if V_SLICE_H else "INÉLIGIBLE", HOSTNAME,
        ("tranchée par %d lignes (%d tranches)" % (V_SLICE_H, _V_TOTAL_SLICES)) if V_SLICE_H
        else "(%s) → chemin historique"
        % ("entrelacé" if INTERLACED else "hauteur %d sans diviseur" % HEIGHT)), "info")

def _copy_band(dst, src, a, b):
    """MODE TRANCHE — copie IDENTITÉ des lignes image [a, b) sur les 3 plans (Y|Cb|Cr) du
    payload planar, en OCTETS plats. `b` est toujours un multiple de V_SLICE_H (lui-même
    multiple de _CH) → lignes chroma entières. Renvoie b."""
    if b <= a:
        return a
    dst[a * _Y_ROW:b * _Y_ROW] = src[a * _Y_ROW:b * _Y_ROW]
    ca = a // _CH; cb = b // _CH
    if cb > ca:
        o1 = Y_SIZE; o2 = Y_SIZE + UV_SIZE
        dst[o1 + ca * _UV_ROW:o1 + cb * _UV_ROW] = src[o1 + ca * _UV_ROW:o1 + cb * _UV_ROW]
        dst[o2 + ca * _UV_ROW:o2 + cb * _UV_ROW] = src[o2 + ca * _UV_ROW:o2 + cb * _UV_ROW]
    return b

def _emit_grain_sliced(w, gi, vw, data):
    """MODE TRANCHE — émet une trame déjà COMPLÈTE dans un grain OUVERT, par bandes + commit
    progressif (validSlices=1..N, quasi gratuit). Commit FINAL garanti par le finally (un
    grain laissé partiel ne serait jamais lisible) — y compris sur les chemins mire/gel."""
    try:
        src = np.frombuffer(data, dtype=np.uint8)
        if src.size < V_FRAME_SIZE:
            n = min(src.size, len(vw))
            vw[:n] = src[:n]           # frame courte (défensif) : copie pleine, pas de bandes
            return
        a = 0
        for k in range(1, _V_TOTAL_SLICES + 1):
            a = _copy_band(vw, src, a, min(HEIGHT, k * V_SLICE_H))
            if k < _V_TOTAL_SLICES:
                w.commit(gi, valid_slices=k)   # réveille l'aval à chaque bande
    finally:
        w.commit(gi, valid_slices=None)

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
           "conn_state": "waiting", "mode": MODE,
           "last_frame_age_ms": None, "ffmpeg_restarts": 0, "video_max_gap_ms": 0.0,
           "in_fps": 0.0,
           # Santé du chemin audio : famine (silence inséré), drop (buffer saturé) et
           # ré-ancrage (dérive horloge source ↔ grille TAI) — tous trois s'entendent.
           "audio_starve_blocks": 0, "audio_drop_samples": 0, "audio_reanchors": 0,
           "audio_buf_ms": 0.0, "audio_in_sps": 0.0,
           "enabled": ENABLED, "net_in_kbps": 0.0,
           "slice_mode": bool(V_SLICE_H), "slice_h": V_SLICE_H,
           "total_slices": _V_TOTAL_SLICES}}

frame_lock = threading.Lock()
_last_frame = None            # bytes de la dernière trame conformée reçue
_last_frame_mono = None       # time.monotonic() de réception
_frames_in = 0                # compteur trames reçues (fenêtre in_fps)

audio_lock = threading.Lock()
_audio_buf = collections.deque()   # blocs float32 (n, 8) en attente
_audio_buf_n = 0                   # échantillons dans _audio_buf (compteur, évite un sum() par chunk)
# Capacité et latence sont DEUX choses distinctes. On entre et on sort à 48 kHz, donc le tampon ne
# se vide jamais de lui-même : son occupation en régime établi EST la latence audio (retard sur la
# vidéo). Mais ffmpeg écrit dans le fifo par rafales de plusieurs dizaines de ms : un plafond bas
# écrête ces rafales (drops) puis affame le feeder — il faut donc de la capacité ET un drainage.
#   MAX  : capacité dure, absorbe les rafales d'écriture de ffmpeg.
#   HIGH : au-delà, on draine jusqu'à TARGET — une fois (rafale de démarrage), pas à chaque pic.
# L'occupation mesurée n'oscille que de ~20 ms : HIGH−TARGET = 30 ms suffit à ne jamais déclencher
# le drainage en régime établi, tout en gardant la latence audio à ~2 trames.
_AUDIO_BUF_MAX    = A_SAMPLE_RATE * 200 // 1000
_AUDIO_BUF_HIGH   = A_SAMPLE_RATE *  60 // 1000
_AUDIO_BUF_TARGET = A_SAMPLE_RATE *  30 // 1000
_audio_seen = False                # a-t-on déjà reçu de l'audio ? (sinon la famine est normale)

_proc = None                  # process ffmpeg courant (supervisé)
_proc_lock = threading.Lock()
_reconnect_req = threading.Event()
_reconnect_req_audio = threading.Event()   # /reconnect ré-arme l'essai audio
_stderr_tail = collections.deque(maxlen=12)
_enabled = threading.Event()               # ingest actif (marche/arrêt à chaud)
if ENABLED:
    _enabled.set()

# Format RÉEL de la source, avant conform — parsé des lignes « Stream #0:x » de ffmpeg.
_src_fmt = {{"video": None, "audio": None}}
_RE_VIDEO = re.compile(r"Stream #\d+:\d+.*: Video: ([^,]+).*?(\d{{3,5}})x(\d{{3,5}}).*?([\d.]+) fps")
_RE_AUDIO = re.compile(r"Stream #\d+:\d+.*: Audio: ([^,]+), (\d+) Hz, ([^,]+)")

# Historique 2 min (1 échantillon/s) : débit réseau entrant + événements gel / silence audio.
HIST_SECONDS = 120
_hist = collections.deque(maxlen=HIST_SECONDS)
_hist_lock = threading.Lock()

# ─── journal de connexion (persistant) ──────────────────────────────────────
# L'anneau de 2 min sert au diagnostic à chaud ; pour JUSTIFIER une coupure a posteriori il
# faut une trace horodatée qui survit au redémarrage du script. JSONL append-only, tourné à
# JOURNAL_MAX_BYTES. Horloge murale (CLOCK_REALTIME, disciplinée PTP) : les horodatages sont
# comparables avec ceux des autres équipements de la chaîne.
# /var/lib/bobi = volume d'état persistant du nœud (manifest state_volume). Le rootfs du
# conteneur est RECRÉÉ à chaque déploiement et au boot du nœud : un journal écrit dans
# /opt/script disparaîtrait précisément quand il sert, après une coupure. Repli sur /opt/script
# si le volume n'est pas monté (container créé avant l'ajout de state_volume).
def _journal_path():
    d = "/var/lib/bobi"
    try:
        if os.path.isdir(d) and os.access(d, os.W_OK):
            return os.path.join(d, "stream_in_events.jsonl")
    except Exception:
        pass
    return "/opt/script/stream_in_events.jsonl"

JOURNAL_PATH = _journal_path()
JOURNAL_PERSISTENT = JOURNAL_PATH.startswith("/var/lib/bobi")
JOURNAL_MAX_BYTES = 4 * 1024 * 1024
_journal_lock = threading.Lock()

def journal(event, **fields):
    rec = {{"ts": time.time(), "event": event}}
    rec.update(fields)
    line = json.dumps(rec, ensure_ascii=False)
    with _journal_lock:
        try:
            if (os.path.exists(JOURNAL_PATH)
                    and os.path.getsize(JOURNAL_PATH) > JOURNAL_MAX_BYTES):
                os.replace(JOURNAL_PATH, JOURNAL_PATH + ".1")
            with open(JOURNAL_PATH, "a", encoding="utf-8") as f:
                f.write(line + "\n")
        except Exception:
            pass

def journal_clear():
    """Vide le journal. Trace immédiatement l'effacement : un journal vidé ne doit pas être
    indistinguable d'un journal vierge (il sert de pièce justificative)."""
    with _journal_lock:
        for p in (JOURNAL_PATH, JOURNAL_PATH + ".1"):
            try: os.unlink(p)
            except FileNotFoundError: pass
            except Exception: pass
    journal("journal_cleared")

def journal_read(t_from=None, t_to=None, limit=5000):
    """Événements dans [t_from, t_to] (epoch s), plus anciens d'abord. Lit aussi le fichier
    tourné : une coupure à cheval sur la rotation reste justifiable."""
    out = []
    for path in (JOURNAL_PATH + ".1", JOURNAL_PATH):
        try:
            with open(path, encoding="utf-8") as f:
                for line in f:
                    try: rec = json.loads(line)
                    except Exception: continue
                    ts = rec.get("ts")
                    if not isinstance(ts, (int, float)): continue
                    if t_from is not None and ts < t_from: continue
                    if t_to is not None and ts > t_to: continue
                    out.append(rec)
        except FileNotFoundError:
            continue
        except Exception:
            continue
    out.sort(key=lambda r: r["ts"])
    return out[-int(limit):] if limit else out

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
        elif V_SLICE_H:
            # MODE TRANCHE : émission par bandes de la trame déjà complète (commit progressif).
            # Même open_grain (index genlock inchangé), même pacing — seul le commit change.
            _i, gi, vw = w.open_grain(index=(fbase if GENLOCK else None))
            _emit_grain_sliced(w, gi, vw, frame_bytes)
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
    with src_lock:
        s = dict(_src)
    if MODE == "srt_listener":
        q = "mode=listener&latency=%d" % (s["srt_latency_ms"] * 1000)
        if s["srt_passphrase"]: q += "&passphrase=" + s["srt_passphrase"]
        if s["srt_streamid"]:   q += "&streamid=" + s["srt_streamid"]
        return "srt://0.0.0.0:%d?%s" % (s["listen_port"], q)
    if MODE == "srt_caller":
        base = s["url"].split("?", 1)[0]
        q = "mode=caller&latency=%d" % (s["srt_latency_ms"] * 1000)
        if s["srt_passphrase"]: q += "&passphrase=" + s["srt_passphrase"]
        if s["srt_streamid"]:   q += "&streamid=" + s["srt_streamid"]
        return base + "?" + q
    if MODE == "rtmp_server":
        return "rtmp://0.0.0.0:%d/%s/%s" % (s["listen_port"], s["rtmp_app"],
                                            s["rtmp_key"] or "stream")
    return s["url"]   # rtmp_pull : URL fournie telle quelle

def _ffmpeg_cmd(with_audio=True):
    # bwdif deint=interlaced : ne désentrelace QUE les trames marquées entrelacées (passthrough
    # sinon) → chaîne unique quel que soit le flux entrant. fps=grille de sortie (cadence TRAME).
    vf = ("bwdif=mode=send_frame:parity=auto:deint=interlaced,"
          "scale=%d:%d:flags=bicubic,fps=%d/%d,format=%s,setsar=1"
          % (WIDTH, HEIGHT, FPS_NUM, FPS_DEN, PIX_FMT))
    # -loglevel info : nécessaire pour lire le format réel de la source (lignes « Stream #0:x »).
    # -nostats : à ce niveau ffmpeg écrirait ses lignes de progression en \r sur stderr, ce qui
    # ferait bloquer le readline du drain jusqu'au prochain \n.
    cmd = ["ffmpeg", "-nostdin", "-hide_banner", "-loglevel", "info", "-nostats",
           "-analyzeduration", "10M", "-probesize", "10M"]
    if MODE == "rtmp_server":
        cmd += ["-listen", "1"]
    cmd += ["-i", _input_url(),
            "-map", "0:v:0", "-vf", vf, "-f", "rawvideo", "pipe:1"]
    if with_audio:
        # `0:a:0?` tolère l'absence de piste au niveau du mapping, mais une sortie SANS stream
        # fait échouer ffmpeg au démarrage → le superviseur retombe en vidéo seule (cf. bascule).
        # aresample=async : la source réseau a sa propre horloge (PTS qui sautent, jitter) —
        # ffmpeg comble/compresse pour sortir un flux réellement continu à 48 kHz.
        # flush_packets : sans lui, ffmpeg accumule dans son tampon AVIO et vide le fifo par
        # rafales plus grosses que la latence visée → écrêtage puis famine côté feeder.
        cmd += ["-map", "0:a:0?", "-af", "aresample=async=1:first_pts=0",
                "-ac", str(A_CHANNELS), "-ar", str(A_SAMPLE_RATE),
                "-f", "f32le", "-flush_packets", "1", "-y", AUDIO_FIFO]
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

def _audio_seen_set():
    global _audio_seen
    _audio_seen = True

def _audio_reader():
    """Ouvre le fifo audio en boucle (rendez-vous bloquant OK : thread dédié, jamais sur le chemin
    vidéo). Blocs f32le 8ch → deque bornée consommée par audio_feeder."""
    global _audio_buf_n
    blk = A_SPMS * A_CHANNELS * 4 * 10          # 10 ms
    win_n = 0; win_t0 = time.monotonic()
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
                    _audio_seen_set()
                    dropped = 0
                    with audio_lock:
                        _audio_buf.append(arr); _audio_buf_n += n
                        while _audio_buf_n > _AUDIO_BUF_MAX and _audio_buf:
                            d = _audio_buf.popleft().shape[0]
                            _audio_buf_n -= d; dropped += d
                        tot = _audio_buf_n
                    win_n += n
                    el = time.monotonic() - win_t0
                    with metrics_lock:
                        if dropped:
                            metrics["audio_drop_samples"] += dropped
                        metrics["audio_buf_ms"] = round(tot / float(A_SPMS), 1)
                        if el >= 1.0:
                            metrics["audio_in_sps"] = round(win_n / el)
                    if el >= 1.0:
                        win_n = 0; win_t0 = time.monotonic()
        except Exception:
            time.sleep(0.5)

def _stderr_drain(proc):
    """ffmpeg tourne en -loglevel info : on y pêche le format RÉEL de la source (lignes
    « Stream #0:x ») et on ne garde dans la queue d'erreurs que ce qui mérite l'œil.
    Les sections « Input # » et « Output # » ont la MÊME forme de ligne Stream : sans ce
    garde-fou on décrirait notre propre sortie (rawvideo 8ch) comme étant la source."""
    in_input = False
    for line in iter(proc.stderr.readline, b""):
        try:
            s = line.decode("utf-8", "replace").strip()
        except Exception:
            continue
        if not s:
            continue
        if s.startswith("Input #"):
            in_input = True; continue
        if s.startswith(("Output #", "Stream mapping:")):
            in_input = False; continue
        m = _RE_VIDEO.search(s) if in_input else None
        if m:
            _src_fmt["video"] = {{"codec": m.group(1).split()[0], "width": int(m.group(2)),
                                 "height": int(m.group(3)), "fps": float(m.group(4))}}
            continue
        m = _RE_AUDIO.search(s) if in_input else None
        if m:
            _src_fmt["audio"] = {{"codec": m.group(1), "sample_rate": int(m.group(2)),
                                 "layout": m.group(3).strip()}}
            continue
        low = s.lower()
        if any(k in low for k in ("error", "failed", "invalid", "unable", "does not contain",
                                  "connection", "timed out", "warning")):
            _stderr_tail.append(s)
            journal("ffmpeg_error", message=s[:400])

# ─── débit réseau entrant (proxy du débit de la source) ─────────────────────
# Le conteneur est en macvlan : hors flux d'ingest, son trafic entrant se limite au contrôle
# HTTP (négligeable) — le MXL passe par la shm, pas par le réseau. rx_bytes est donc une
# bonne mesure du débit de la source, sans dépendre de stats que ffmpeg n'expose pas.
def _rx_bytes():
    tot = 0
    try:
        with open("/proc/net/dev") as f:
            for line in f.readlines()[2:]:
                name, _, rest = line.partition(":")
                if name.strip() in ("lo", ""):
                    continue
                tot += int(rest.split()[0])
    except Exception:
        return None
    return tot

def history_sampler():
    """1 échantillon/s : débit entrant + drapeaux gel / silence audio sur la seconde écoulée.
    Alimente la courbe du panneau (diagnostic a posteriori d'une chute ou d'un décrochage)."""
    prev = _rx_bytes(); prev_t = time.monotonic()
    prev_starve = metrics["audio_starve_blocks"]
    while True:
        time.sleep(1.0)
        now = _rx_bytes(); now_t = time.monotonic()
        kbps = 0.0
        if prev is not None and now is not None and now >= prev:
            kbps = round((now - prev) * 8.0 / 1000.0 / max(1e-3, now_t - prev_t), 1)
        prev, prev_t = now, now_t
        with metrics_lock:
            conn = metrics["conn_state"]
            starve = metrics["audio_starve_blocks"]
            metrics["net_in_kbps"] = kbps
        silence = 1 if (starve > prev_starve and _audio_seen) else 0
        prev_starve = starve
        with _hist_lock:
            _hist.append({{"kbps": kbps,
                          "freeze": 1 if conn == "frozen" else 0,
                          "silence": silence,
                          "off": 1 if conn == "off" else 0}})

# ─── superviseur ffmpeg (poll + restart, backoff) ────────────────────────────
def supervisor():
    global _proc
    restarts = 0
    # Gel prolongé en caller/pull = connexion zombie → kill (le restart re-tente). Les modes
    # écoute (listener/server) restent en attente passive, pas de stall-kill.
    stall_kill_ms = max(10_000, FREEZE_TIMEOUT_MS * 10)
    with_audio = True
    while True:
        if not _enabled.is_set():        # ingest coupé : la vidéo continue (mire), pas de ffmpeg
            with frame_lock:
                globals()["_last_frame"] = None
                globals()["_last_frame_mono"] = None
            _enabled.wait()
            _src_fmt["video"] = _src_fmt["audio"] = None
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
            if _reconnect_req.is_set() or not _enabled.is_set():
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
        journal("ffmpeg_restart", n=restarts, got_frames=got_any,
                last_error=(_stderr_tail[-1] if _stderr_tail else None))
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
                         interlace=SCAN,
                         **_SLICE_KW)
    idx = 0
    last_conn = None
    period_ns = round(1e9 * FPS_DEN / FPS_NUM)
    kf = _grid_k(_grid_now_ns()) + 2
    win_cnt = 0; win_t0 = _grid_now_ns(); win_max_gap = 0; last_w = win_t0
    win_in0 = 0
    while True:
        if bus_error.is_set():
            bus_error.clear()
            try: shm.close()
            except Exception: pass
            # GC ENTRE close et reopen (parade générique du chantier tranche) : sans lui, la
            # réouverture PAR NOM peut retomber sur la génération orpheline qu'on vient de lâcher.
            try: inst.garbage_collect()
            except Exception: pass
            time.sleep(1)
            shm = bobimxl.Writer(inst, VIDEO_SHM, WIDTH, HEIGHT, CHROMA, BIT_DEPTH,
                                 FPS_NUM, FPS_DEN,
                                 interlace=SCAN,
                                 **_SLICE_KW)
            kf = _grid_k(_grid_now_ns()) + 2
        G = _grid_ns(kf)
        if GENLOCK:
            _grid_sleep_until(G)
        else:
            time.sleep(max(0.0, period_ns / 1e9))
        with frame_lock:
            frame = _last_frame; lm = _last_frame_mono; fin = _frames_in
        age_ms = None if lm is None else (time.monotonic() - lm) * 1000.0
        if not _enabled.is_set():
            frame = IDLE_FRAME; conn = "off"
        elif frame is None:
            frame = IDLE_FRAME; conn = "waiting"
        elif age_ms is not None and age_ms > FREEZE_TIMEOUT_MS:
            conn = "frozen"
        else:
            conn = "connected"
        if conn != last_conn:
            # Transition d'état = l'événement qui justifie une coupure. Horodatée, avec le
            # format de la source au moment du raccrochage.
            journal("state", state=conn, prev=last_conn,
                    source=_input_url().split("?", 1)[0],
                    src_fmt=(_src_fmt.get("video") or None),
                    frame_age_ms=(None if age_ms is None else round(age_ms)))
            last_conn = conn
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
# Index d'écriture CONTIGU et explicite : AudioWriter.write() sans `index` recalcule
# `mxlGetCurrentIndex() - n` à CHAQUE appel — deux écritures rapprochées retombent alors sur
# le même index (écrasement) ou en sautent (les vieux échantillons du ring ressortent), ce qui
# s'entend en clics. On ancre une fois sur la grille d'échantillons TAI puis on avance de n.
# Granularité d'écriture = 1 ms, comme player/avsync. C'est un CONTRAT avec les consommateurs :
# le streamer lit par blocs de 1 ms et, dès qu'un passage ne trouve pas de bloc complet, il
# injecte du silence pour combler les ms écoulées (script.py:audio_feeder). Écrire par blocs de
# 5 ms fait avancer head_index par sauts de 5 ms → le consommateur tourne à vide 4 passages sur 5
# et bourre du silence : l'audio « saute ». Ne pas augmenter sans revoir les consommateurs.
# NB : le réglage `mtl_audio_ptime` (1 ms / 125 µs) est le packet time RÉSEAU ST 2110-30 du moteur
# 2110_io, pas la granularité du bus — même en 125 µs le chunk shm reste 1 ms. Rien à suivre ici.
A_BLK = A_SPMS                           # 48 samples = 1 ms
A_MAX_LAG = A_SAMPLE_RATE // 4           # 250 ms de dérive tolérée avant ré-ancrage

def _tai_sample_index():
    return bobimxl.now_tai() * A_SAMPLE_RATE // 1_000_000_000

def audio_feeder():
    global _audio_buf_n
    aw = bobimxl.AudioWriter(inst, AUDIO_SHM, channels=A_CHANNELS, sample_rate=A_SAMPLE_RATE)   # l'index est fourni explicitement à write()
    silence = np.zeros((A_BLK, A_CHANNELS), dtype=np.float32)
    pending = None                        # reliquat du bloc en cours
    next_idx = _tai_sample_index() - A_BLK
    while True:
        cur = _tai_sample_index()
        # Drainage : l'occupation du tampon est la latence audio. Au-delà du seuil haut (rafale
        # de démarrage, hoquet réseau) on retombe d'un coup sur la cible plutôt que de traîner
        # ce retard indéfiniment. Les pics normaux (< HIGH) ne déclenchent rien : pas de thrash.
        drained = 0
        with audio_lock:
            if _audio_buf_n > _AUDIO_BUF_HIGH:
                while _audio_buf_n > _AUDIO_BUF_TARGET and _audio_buf:
                    d = _audio_buf.popleft().shape[0]
                    _audio_buf_n -= d; drained += d
                pending = None
        if drained:
            with metrics_lock:
                metrics["audio_drop_samples"] += drained
        # Dérive : l'horloge de la SOURCE n'est pas la grille TAI. Trop en retard (backlog) ou
        # trop en avance (écriture dans le futur) → on ré-ancre, un seul saut plutôt qu'un
        # décalage qui s'accumule.
        if abs(cur - next_idx) > A_MAX_LAG:
            next_idx = cur - A_BLK
            with audio_lock:
                _audio_buf.clear(); _audio_buf_n = 0
            pending = None
            with metrics_lock:
                metrics["audio_reanchors"] += 1
        while next_idx + A_BLK <= cur:
            need = A_BLK
            out = np.empty((A_BLK, A_CHANNELS), dtype=np.float32); pos = 0
            with audio_lock:
                while need > 0:
                    if pending is not None and len(pending):
                        take = min(need, len(pending))
                        out[pos:pos + take] = pending[:take]
                        pending = pending[take:] if take < len(pending) else None
                        pos += take; need -= take
                    elif _audio_buf:
                        pending = _audio_buf.popleft()
                        _audio_buf_n -= pending.shape[0]
                    else:
                        break
            if need > 0:                  # pas assez de frais → compléter en silence
                if pos == 0:
                    out = silence
                else:
                    out[pos:] = 0.0
                if _audio_seen:          # avant le 1er échantillon, le silence est normal
                    with metrics_lock:
                        metrics["audio_starve_blocks"] += 1
            try: aw.write(out, index=next_idx)
            except Exception: pass
            next_idx += A_BLK
        # Période < 1 ms : un seul bloc par passage, donc head_index avance de façon CONTINUE.
        # Dormir plus longtemps ferait avancer la tête par paquets et rouvrirait la fenêtre
        # d'injection de silence chez le consommateur (cf. contrat 1 ms ci-dessus).
        time.sleep(0.0005)

# ─── HTTP : :8080 metrics + :8082 control ────────────────────────────────────
_SRC_INT = ("listen_port", "srt_latency_ms")
_SRC_STR = ("url", "srt_passphrase", "srt_streamid", "rtmp_app", "rtmp_key")

def _apply_source(body):
    """Applique un nouveau point d'entrée à chaud. Renvoie les clés effectivement changées."""
    changed = []
    with src_lock:
        for k in _SRC_INT:
            if k in body:
                try: v = int(body[k])
                except (TypeError, ValueError): continue
                if v != _src[k]: _src[k] = v; changed.append(k)
        for k in _SRC_STR:
            if k in body:
                v = str(body[k] or "")
                if k in ("rtmp_app", "rtmp_key"): v = v.strip("/")
                if k == "rtmp_app" and not v: v = "live"
                if v != _src[k]: _src[k] = v; changed.append(k)
    if changed:
        journal("source_change", changed=changed, source=_input_url().split("?", 1)[0])
        _reconnect_req.set()          # relance ffmpeg sur la nouvelle adresse
    return changed

def _state_payload():
    with metrics_lock: m = dict(metrics)
    with src_lock: s = dict(_src)
    with _hist_lock: hist = list(_hist)
    m.update(s)
    m.update({{"hostname": HOSTNAME, "video_shm": VIDEO_SHM, "audio_shm": AUDIO_SHM,
              "width": WIDTH, "height": HEIGHT, "fps_cfg": FPS,
              "chroma": CHROMA, "bit_depth": BIT_DEPTH, "scan": _SCAN_CFG,
              "mode": MODE, "idle_mode": IDLE_MODE,
              "freeze_timeout_ms": FREEZE_TIMEOUT_MS, "genlock": GENLOCK,
              "source_format": dict(_src_fmt),
              "journal_persistent": JOURNAL_PERSISTENT,
              "log_level": LOG_LEVEL,   # lisible en condition de macro
              "history": hist, "history_seconds": HIST_SECONDS,
              "last_errors": list(_stderr_tail),
              "mxl_lib": _mxl_lib_state()}})
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
        path, _, qs = self.path.partition("?")
        if path == "/state":
            self._send_json(200, _state_payload())
        elif path == "/events":
            q = urllib.parse.parse_qs(qs)
            def _num(k):
                try: return float(q[k][0])
                except Exception: return None
            try: limit = int(q.get("limit", ["5000"])[0])
            except Exception: limit = 5000
            evs = journal_read(_num("from"), _num("to"), max(1, min(50000, limit)))
            self._send_json(200, {{"events": evs, "count": len(evs),
                                  "hostname": HOSTNAME, "mode": MODE}})
        else:
            self.send_response(404); self.end_headers()
    def _body(self):
        n = int(self.headers.get("Content-Length", 0))
        try: return json.loads(self.rfile.read(n)) if n else {{}}
        except Exception: return None
    def do_POST(self):
        if self.path == "/reconnect":
            _reconnect_req_audio.set()
            _reconnect_req.set()
            self._send_json(200, {{"ok": True}})
        elif self.path == "/source":
            body = self._body()
            if body is None:
                self._send_json(400, {{"error": "JSON invalide"}}); return
            self._send_json(200, {{"ok": True, "applied": _apply_source(body)}})
        elif self.path == "/enable":
            body = self._body()
            if body is None:
                self._send_json(400, {{"error": "JSON invalide"}}); return
            want = _as_bool(body.get("enabled"), default=True)
            if want: _enabled.set()
            else:    _enabled.clear()
            with metrics_lock:
                metrics["enabled"] = want
            journal("power", enabled=want)
            self._send_json(200, {{"ok": True, "enabled": want}})
        elif self.path == "/events/clear":
            journal_clear()
            self._send_json(200, {{"ok": True}})
        elif self.path == "/log_level":
            # Verbosité À CHAUD (pas de redéploiement) : instruction d'incident. Le niveau
            # PERSISTANT reste le champ `log_level` du config_schema ; celui-ci est volatil.
            body = self._body()
            if body is None:
                self._send_json(400, {{"error": "JSON invalide"}}); return
            ok = set_log_level(body.get("level") or body.get("log_level"))
            self._send_json(200 if ok else 400, {{"ok": ok, "log_level": LOG_LEVEL}})
        else:
            self.send_response(404); self.end_headers()
    def log_message(self, *a): pass

# ─── boot ────────────────────────────────────────────────────────────────────
try:
    if os.path.exists(AUDIO_FIFO): os.unlink(AUDIO_FIFO)
    os.mkfifo(AUDIO_FIFO)
except Exception:
    pass

journal("script_start", version=PLUGIN_VERSION, mode=MODE, enabled=ENABLED,
        out_format="%dx%d%s%d" % (WIDTH, HEIGHT, _SCAN_CFG, FPS))

threading.Thread(target=lambda: HTTPServer(("0.0.0.0", 8080), MetricsHandler).serve_forever(), daemon=True).start()
threading.Thread(target=lambda: HTTPServer(("0.0.0.0", 8082), ControlHandler).serve_forever(), daemon=True).start()
threading.Thread(target=_audio_reader, daemon=True).start()
threading.Thread(target=supervisor, daemon=True).start()
threading.Thread(target=history_sampler, daemon=True).start()
threading.Thread(target=video_feeder, daemon=True).start()
threading.Thread(target=audio_feeder, daemon=True).start()

while True:
    time.sleep(1)
