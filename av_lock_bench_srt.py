#!/usr/bin/env python3
# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright (C) 2026 BOBI SAS, France
# Auteur : Cyril Mazouer, pour le compte de BOBI SAS
# Distribué sous licence GNU GPL v3 (ou ultérieure) ; voir le fichier LICENSE.
#
# BANC EN LIGNE du verrou A/V — à lancer À LA MAIN, aucune flotte touchée :
#
#   1) fabriquer la source à repères (flash vidéo + salve audio SIMULTANÉS, une fois/s) :
#      ffmpeg -y -f lavfi -i "color=c=black:s=1920x1080:r=25:d=40" \
#             -f lavfi -i "sine=f=1000:r=48000:d=40" \
#             -vf "drawbox=t=fill:c=white:enable='lt(mod(n,25),3)'" \
#             -af "volume=0:enable='not(lt(mod(t,1),0.06))'" \
#             -c:v libx264 -preset ultrafast -g 25 -pix_fmt yuv420p -c:a aac -ac 2 -b:a 128k \
#             -f mpegts src25.ts
#   2) ./venv/bin/python plugins/stream_in/av_lock_bench_srt.py <verrou 0|1> <port> <durée> <retenue>
#
# Le script du plugin tourne TEL QUEL, avec un bobimxl factice qui ENREGISTRE ce qui est écrit.
# On lit ensuite la coordonnée TAI du grain vidéo portant le flash et celle de l'échantillon
# portant la salve : leur écart EST l'écart A/V posé par le plugin — mesuré sans la sonde, dont
# le lecteur audio s'est révélé capable de figer sans le dire (cf. TODO.md).
#
# Ce qu'il ne dit PAS : le comportement sur une vraie liaison (jitter réseau, gigue de la source)
# et le rendu à l'oreille.
"""Fait tourner le SCRIPT DU PLUGIN tel quel, hors conteneur, avec un bobimxl qui ENREGISTRE.

On lui pousse en SRT une source 25p portant un flash vidéo et une salve audio SIMULTANÉS, et on
regarde où le plugin les POSE : la coordonnée TAI du grain vidéo qui contient le flash, et celle
de l'échantillon audio qui contient la salve. Si le verrou fait ce qu'il annonce, les deux
tombent au même instant. L'écart mesuré ICI est celui que la sonde voit en production.
"""
import io, os, sys, time, types, threading, subprocess
import numpy as np

import os.path
PLUG = os.path.join(os.path.dirname(os.path.abspath(__file__)), "script.py")
AV_LOCK = sys.argv[1] in ("1", "true")
PORT = sys.argv[2]
DUREE = float(sys.argv[3]) if len(sys.argv) > 3 else 25.0
MARGE = int(sys.argv[4]) if len(sys.argv) > 4 else 0
W, H = 640, 360
FPS = 50

vid = []          # (index de grain, luma moyenne du plan Y)
aud = []          # (index d'échantillon de départ, |max| du bloc)
lock = threading.Lock()

class FauxWriter:
    def __init__(self, inst, nom, w, h, *a, **k):
        self.w, self.h = w, h
        self.taille = w * h * 2          # yuv422p 8 bits
        self.buf = np.zeros(self.taille, dtype=np.uint8)
    def open_grain(self, index=None, src_index=None):
        self._i = index
        return index, index, self.buf
    def commit(self, gi, valid_slices=None):
        if valid_slices is not None:
            return                        # commit progressif : on ne retient que le final
        with lock:
            vid.append((gi, float(self.buf[: self.w * self.h].mean())))
    def close(self): pass

class FauxAudioWriter:
    def __init__(self, inst, nom, channels=8, sample_rate=48000): pass
    def write(self, samples, index=None):
        with lock:
            aud.append((index, float(np.abs(samples).max())))
        return index

faux = types.ModuleType("bobimxl")
faux.Instance = lambda *a, **k: types.SimpleNamespace(garbage_collect=lambda: None,
                                                     flow_def=lambda **kw: None)
faux.Writer = FauxWriter
faux.AudioWriter = FauxAudioWriter
faux.now_tai = lambda: time.clock_gettime_ns(time.CLOCK_REALTIME)
faux.lib_info = lambda: {"variante": "banc"}
sys.modules["bobimxl"] = faux

cfg = {"mode": "srt_listener", "listen_port": int(PORT), "srt_latency_ms": 120,
       "width": W, "height": H, "fps": FPS, "chroma": "422", "bit_depth": 8, "scan": "p",
       "genlock": True, "idle_mode": "black", "enabled": True, "log_level": "warning",
       "av_lock": AV_LOCK, "av_margin_frames": MARGE}
src = io.open(PLUG, encoding="utf-8").read().format(
    config=repr(cfg), hostname="banc-stream-in", plugin_version="banc")
# Le script pose un handler SIGBUS : il DOIT tourner dans le fil principal. On l'exécute
# jusqu'à sa boucle d'attente finale — les fils de travail, eux, sont déjà lancés.
mod = {"__name__": "banc"}
exec(compile(src[:src.index("while True:\n    time.sleep(1)")], "script.py", "exec"), mod)
time.sleep(3)

pousse = subprocess.Popen(
    ["ffmpeg", "-nostdin", "-hide_banner", "-loglevel", "error", "-re",
     "-i", "src25.ts",
     "-c", "copy", "-f", "mpegts",
     "srt://127.0.0.1:%s?mode=caller&latency=120000" % PORT])
# Trace seconde par seconde : un compteur final ne dit pas QUAND le verrou lâche.
trace = []
t0 = time.time()
_m = mod["metrics"]
prev = None
while time.time() - t0 < DUREE and pousse.poll() is None:
    time.sleep(1)
    with lock:
        nb_a = len(aud)
    c = dict(_m)
    trace.append((round(time.time() - t0), c.get("av_locked"), c.get("av_skew_ms"),
                  c.get("audio_headroom_ms"), c.get("audio_buf_ms"),
                  c.get("audio_starve_blocks"), c.get("audio_late_samples"),
                  c.get("av_relocks"), c.get("audio_in_sps"), c.get("av_glisse_ms"),
                  (c.get("av_fallback") or "")[:18]))
try: pousse.kill()
except Exception: pass
time.sleep(2)

print("   t  verr  skew  marge  tampon  famine   tard  rel  in_sps  glisse  repli")
for r in trace:
    print("  %3s  %-5s %5s %6s %7s %7s %6s %4s %6s %7s  %s"
          % (r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7], r[8], r[9], r[10]))
with lock:
    V = list(vid); A = list(aud)
m = mod["metrics"]
print("  verrou=%s accroché=%s  skew=%s  marge=%s  famine=%s  tard=%s  relocks=%s"
      % (AV_LOCK, m.get("av_locked"), m.get("av_skew_ms"), m.get("audio_headroom_ms"),
         m.get("audio_starve_blocks"), m.get("audio_late_samples"), m.get("av_relocks")))
if not V or not A:
    sys.exit("  aucune écriture (V=%d A=%d)" % (len(V), len(A)))
fond = np.median([l for _i, l in V])
flashs = [i * 1e9 / FPS for i, l in V if l > fond + 60]
salves = [i * 1e9 / 48000.0 for i, a in A if a > 0.01]
print("  écritures : %d grains, %d blocs ; repères vus : %d flashs, %d salves ; ampl. max %.4f"
      % (len(V), len(A), len(flashs), len(salves), max([a for _i, a in A] or [0])))
if not flashs or not salves:
    sys.exit("  repère absent, rien à conclure")
# Les salves durent 20 ms : on ne garde que le PREMIER bloc de chaque salve.
deb = [t for j, t in enumerate(salves) if j == 0 or t - salves[j - 1] > 1e8]
# Idem côté vidéo (un flash peut couvrir deux grains en 25p → 50p).
fl = [t for j, t in enumerate(flashs) if j == 0 or t - flashs[j - 1] > 1e8]
paires = []
for tv in fl:
    ta = min(deb, key=lambda x: abs(x - tv))
    if abs(ta - tv) < 5e8:                       # même repère, pas le voisin
        paires.append((ta - tv) / 1e6)
if not paires:
    sys.exit("  aucun appariement flash/salve")
paires.sort()
med = paires[len(paires) // 2]
print("  %d repères appariés ; écart A/V posé par le plugin : médiane %+.1f ms, "
      "étendue [%+.1f … %+.1f]  (négatif = audio en avance)"
      % (len(paires), med, paires[0], paires[-1]))
