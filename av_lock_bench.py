#!/usr/bin/env python3
# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright (C) 2026 BOBI SAS, France
# Auteur : Cyril Mazouer, pour le compte de BOBI SAS
# Distribué sous licence GNU GPL v3 (ou ultérieure) ; voir le fichier LICENSE.
#
# Banc du VERROU A/V (0.9.0) — à lancer À LA MAIN depuis la racine du dépôt :
#     ./venv/bin/python plugins/stream_in/av_lock_bench.py
#
# Ce qu'il éprouve : la LOGIQUE de la ligne de temps, hors conteneur et hors bus MXL. Le
# script du plugin est rendu (str.format) puis exécuté JUSQU'À la section boot seulement —
# aucun thread ne démarre, `bobimxl` est un factice dont l'horloge TAI est volontairement
# décalée de 37 s pour que le pont d'horloge soit réellement exercé (un pont absent passe
# inaperçu sur un nœud dont l'horloge est déjà en TAI).
#
# Ce qu'il n'éprouve PAS, et qui se mesure sur banc réel : la marge d'arrivée réelle de
# l'audio (`audio_headroom_ms`) et l'écart A/V bout-en-bout, qui se lisent à la sonde.
import io, sys, types, time
import numpy as np

# ── bobimxl factice : horloge TAI décalée de 37 s par rapport à CLOCK_REALTIME, pour
#    éprouver le pont d'horloge (_tai_offset_ns) et pas seulement le cas où il vaut 0.
TAI_OFFSET_NS = 37_000_000_000
fake = types.ModuleType("bobimxl")
fake.now_tai = lambda: time.clock_gettime_ns(time.CLOCK_REALTIME) + TAI_OFFSET_NS
fake.Instance = lambda *a, **k: None
fake.Writer = fake.AudioWriter = lambda *a, **k: None
fake.lib_info = lambda: None
sys.modules["bobimxl"] = fake

src = io.open("plugins/stream_in/script.py", encoding="utf-8").read().format(
    config=repr({"mode": "srt_listener", "fps": 50, "width": 1920, "height": 1080,
                 # explicite : le verrou est DÉSACTIVÉ par défaut depuis la 0.9.9 (cf.
                 # TODO.md), et ce banc éprouve précisément le chemin verrouillé.
                 "av_lock": True}),
    hostname="stream-in-1", plugin_version="0.9.0")
src = src[:src.index("# ─── boot ─")]
M = {"__name__": "si"}
exec(compile(src, "script.py", "exec"), M)

ok = True
def check(label, cond, detail=""):
    global ok
    ok = ok and bool(cond)
    print(("  OK   " if cond else "  ÉCHEC") + "  " + label + ("  " + detail if detail else ""))

R = M["_AV_R"]
check("48000·D/N = 960 échantillons/trame à 50p", R == 960, "R=%d" % R)
check("trame 0 → échantillon 0", M["_samples_at_frame"](0) == 0)
check("trame 100 → échantillon 96000", M["_samples_at_frame"](100) == 96000)

# ── la grille EST celle du bus : c'est elle qui estampille les grains VIDÉO ─────────
# Jusqu'au 2026-10-25, `_grid_now_ns()` rendait CLOCK_REALTIME. Les index vidéo posés par
# `_write_video(index=fbase)` tombaient donc 37 s — 1850 créneaux à 50p — SOUS la grille TAI du
# bus, et tout délai d'étage mesuré en aval dépassait la borne de plausibilité de 10 s. Rien ici
# ne le voyait : les deux assertions qui vivaient à cet endroit ne testaient que le PONT vers
# l'audio, c'est-à-dire la conséquence corrigée à la main, et pas la cause. Mesuré le 25/10 dans
# un conteneur de dell-1 : l'horloge système y vaut l'UTC, l'écart est bien de 37,000 s.
ecart_horloge = M["_grid_now_ns"]() - fake.now_tai()
check("l'horloge de la grille EST celle du bus (TAI)",
      abs(ecart_horloge) <= 2_000_000, "écart=%+.3f s" % (ecart_horloge / 1e9))
k = M["_grid_k"](M["_grid_now_ns"]())
k_tai = fake.now_tai() * 50 // 1_000_000_000
check("la case vidéo posée est la case TAI, pas 1850 créneaux plus bas",
      abs(k - k_tai) <= 1, "k - k_tai = %d" % (k - k_tai))
# Tolérance de 2 échantillons (40 µs) : l'écart d'horloge est relevé à chaud au chargement du
# module (deux lectures consécutives), il jitte donc d'une poignée de ns — sans conséquence,
# D n'est calculé qu'une fois par accrochage.
# Référence prise sur l'horloge du BUS, pas sur `_grid_ns` : comparer `_grid_tai_sample(k)` à
# `_grid_ns(k)` serait tautologique (les deux en dérivent), et l'assertion resterait verte même
# avec la grille remise sur CLOCK_REALTIME — vérifié par mutation le 25/10.
want = fake.now_tai() * 48000 // 1_000_000_000
check("case de grille → index TAI d'échantillon du MÊME instant",
      abs(M["_grid_tai_sample"](k) - want) <= 960, "écart=%d échantillons"
      % (M["_grid_tai_sample"](k) - want))

# ── accrochage du verrou et zone morte ─────────────────────────────────────────────
M["_av_gen"] = 1
M["_av_note"](1, 1000, k)
D = M["_av_d"]
check("verrou accroché sur la 1ʳᵉ trame", D == M["_grid_tai_sample"](k) - 960000)
check("métrique av_locked posée", M["metrics"]["av_locked"] is True)
M["_av_note"](2, 5000, k + 10)          # génération périmée : ne doit rien bouger
check("une trame d'une autre génération ne déplace pas le verrou", M["_av_d"] == D)
M["_av_note"](1, 1001, k + 3)           # trame retardée de 2 cases (jitter)
check("filtre min : une trame en retard ne déplace pas le verrou (avant fenêtre)",
      M["_av_d"] == D)

# ── _fill_locked : le contenu suit la ligne de temps, pas l'ordre d'arrivée ─────────
A_BLK = M["A_BLK"]; buf = M["_audio_buf"]
def bloc(s0, val, n=A_BLK, gen=1):
    a = np.full((n, 8), float(val), dtype=np.float32)
    return (s0, a, gen, 0)
out = np.empty((A_BLK, 8), dtype=np.float32)

buf.clear(); M["_audio_buf_n"] = 0
for i in range(4):
    buf.append(bloc(i * A_BLK, i + 1))
M["_audio_buf_n"] = 4 * A_BLK
got, late, _, _ec = M["_fill_locked"](0, out)
check("case 0 → 1er bloc entier", got == A_BLK and late == 0 and out[0, 0] == 1.0)
got, late, _, _ec = M["_fill_locked"](A_BLK, out)
check("case suivante → bloc suivant", got == A_BLK and out[0, 0] == 2.0)
# saut en avant : les blocs sautés sont JETÉS, pas servis en retard
got, late, _, _ec = M["_fill_locked"](3 * A_BLK, out)
check("saut de case → le bloc dépassé est jeté, pas décalé",
      got == A_BLK and out[0, 0] == 4.0 and late == A_BLK, "late=%d" % late)

# consommation PARTIELLE : blocs de 10 ms tirés par cases de 1 ms
buf.clear(); M["_audio_buf_n"] = 0
buf.append(bloc(0, 7, n=10 * A_BLK)); M["_audio_buf_n"] = 10 * A_BLK
vals = []
for i in range(10):
    g, _l, _a, _e = M["_fill_locked"](i * A_BLK, out)
    vals.append((g, float(out[0, 0])))
check("un bloc de 10 ms sert 10 cases de 1 ms",
      all(v == (A_BLK, 7.0) for v in vals) and not buf, "%r" % (vals[:2],))
check("compteur d'occupation ramené à zéro", M["_audio_buf_n"] == 0,
      "n=%d" % M["_audio_buf_n"])

# TROU : l'audio de la case demandée n'est pas encore arrivé → silence, sans le consommer
buf.clear(); M["_audio_buf_n"] = 0
buf.append(bloc(5 * A_BLK, 9)); M["_audio_buf_n"] = A_BLK
got, late, _, _ec = M["_fill_locked"](0, out)
check("audio pas encore arrivé → 0 utile, rien de jeté", got == 0 and late == 0)
check("le bloc futur est CONSERVÉ pour sa case", len(buf) == 1)
got, _l, _a, _ec = M["_fill_locked"](5 * A_BLK, out)
check("… et servi quand sa case arrive", got == A_BLK and out[0, 0] == 9.0)

# ── signe de l'écart de tête : attente normale vs décrochage ───────────────────────
buf.clear(); M["_audio_buf_n"] = 0
buf.append(bloc(5 * A_BLK, 9)); M["_audio_buf_n"] = A_BLK
_g, _l, _a, ec = M["_fill_locked"](0, out)
check("audio pas encore arrivé → écart de tête POSITIF (attente)", ec == 5 * A_BLK,
      "ec=%s" % ec)
buf.clear(); M["_audio_buf_n"] = 0
buf.append(bloc(0, 9)); M["_audio_buf_n"] = A_BLK
_g, _l, _a, ec = M["_fill_locked"](10 * A_BLK, out)
check("case dépassée → écart de tête NÉGATIF (décrochage)", ec is not None and ec < 0,
      "ec=%s" % ec)

# À CHEVAL sur deux blocs
buf.clear(); M["_audio_buf_n"] = 0
buf.append(bloc(0, 1)); buf.append(bloc(A_BLK, 2)); M["_audio_buf_n"] = 2 * A_BLK
got, _l, _a, _ec = M["_fill_locked"](A_BLK // 2, out)
check("case à cheval : recollement des deux blocs",
      got == A_BLK and out[0, 0] == 1.0 and out[-1, 0] == 2.0)

print("\n" + ("TOUT PASSE" if ok else "IL RESTE DES ÉCHECS"))
sys.exit(0 if ok else 1)
