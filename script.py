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

# ─── dérivation du flux ENTRANT (diagnostic) ─────────────────────────────────
# « hôte:port » → ffmpeg recopie le flux reçu, SANS transcodage, vers cette adresse UDP. C'est
# le seul moyen d'inspecter ce qu'on REÇOIT réellement (relation A/V des pts, cadence annoncée,
# structure GOP) : le flux arrive par une connexion SRT à consommateur unique, personne d'autre
# ne peut le tapper, et une mesure prise APRÈS notre propre chaîne ne distingue pas ce qui vient
# de la source de ce qu'on a ajouté. Vide = désactivé (aucune sortie supplémentaire).
TAP_UDP = str(CONFIG.get("tap_udp") or "").strip()

# ─── verrouillage A/V sur la ligne de temps de la SOURCE ─────────────────────
# Les deux filtres ffmpeg déjà en place SONT la référence temporelle qui manquait :
#   `fps=N/D:start_time=0`            → la trame de sortie n° n est au pts source n·D/N
#   `aresample=async=1:first_pts=0`   → l'échantillon n° s est au pts source s/48000
# Même axe, même origine (ffmpeg normalise les deux streams sur le start_time de l'entrée) :
# trame n ↔ échantillon n·48000·D/N, EXACTEMENT. Le verrou écrit l'audio à la case TAI de la
# trame qui lui correspond, au lieu de le débiter au fil de l'eau.
# Sans lui, la latence audio EST l'occupation du tampon, qui erre entre les seuils de
# drainage : mesuré 2026-08-23 sur liaison SRT réelle, vidéo 517 ms (σ 8), audio 576 ms
# (σ 59), écart A/V de −47 à +187 ms. Réglage d'échappement : av_lock=0 rend le comportement
# historique à l'octet (y compris la commande ffmpeg).
AV_LOCK = _as_bool(CONFIG.get("av_lock", False), default=False)
# Retenue vidéo. Verrouiller l'audio sur l'instant de présentation vidéo lui RETIRE sa marge
# d'arrivée : le tampon ne la porte plus, et tout bloc en retard devient du silence. On
# présente donc la vidéo N trames plus tard — les DEUX essences décalées d'autant, lip-sync
# inchangé — pour rendre cette marge au chemin audio. 0 = aucune retenue ; la marge réellement
# observée se lit dans `audio_headroom_ms` (minimum glissant), c'est elle qui doit décider.
def _av_margin(v):
    # `or 0` serait un piège : la coercion du manifeste rend None pour une clé absente (tout
    # conteneur d'avant la 0.9.0), et la retenue tomberait SILENCIEUSEMENT à 0 au lieu du défaut
    # annoncé. Un 0 EXPLICITE, lui, doit être respecté.
    try:
        return max(0, min(10, int(v)))
    except (TypeError, ValueError):
        return 1
AV_MARGIN_FRAMES = _av_margin(CONFIG.get("av_margin_frames", 1))

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

def _samples_at_frame(n):
    """Numéro d'échantillon audio du DÉBUT de la trame n, sur l'axe de pts partagé par les
    deux sorties ffmpeg. Rationnel exact : 48000·D/N n'est pas entier en NTSC fractionnaire."""
    return n * A_SAMPLE_RATE * FPS_DEN // FPS_NUM

_AV_R        = max(1, A_SAMPLE_RATE * FPS_DEN // FPS_NUM)   # échantillons par trame (≈960)
_AV_DEADBAND = max(1, _AV_R)               # en deçà, on ne déplace pas le verrou (une trame)
_AV_WINDOW_NS = 5_000_000_000              # fenêtre de la médiane (cf. _av_note)

inst = bobimxl.Instance()
VIDEO_SHM = HOSTNAME
AUDIO_SHM = HOSTNAME + "_audio"
# Un fifo PAR GÉNÉRATION ffmpeg. Avec un fifo unique, le reliquat d'une session et les
# premiers blocs de la suivante arrivent dans le même flux d'octets : impossible de dire à
# quelle ligne de temps un bloc appartient — donc impossible de verrouiller sans risquer de
# poser l'audio d'AVANT une reconnexion sur la grille d'APRÈS. Le chemin porte la génération.
AUDIO_FIFO_DIR = "/tmp"
def _audio_fifo_path(gen):
    return "%s/stream_in_audio.%d.f32" % (AUDIO_FIFO_DIR, gen)

# ─── état partagé ────────────────────────────────────────────────────────────
metrics_lock = threading.Lock()
metrics = {{"fps": 0.0, "frame_index": 0, "plugin_version": PLUGIN_VERSION,
           "conn_state": "waiting", "mode": MODE,
           "last_frame_age_ms": None, "ffmpeg_restarts": 0, "video_max_gap_ms": 0.0,
           "in_fps": 0.0,
           # Santé du chemin audio : famine (silence inséré), drop (buffer saturé) et
           # ré-ancrage (dérive horloge source ↔ grille TAI) — tous trois s'entendent.
           "audio_starve_blocks": 0, "audio_drop_samples": 0, "audio_reanchors": 0,
           # QUI jette ? Les trois causes n'ont ni le même sens ni le même remède : le PLAFOND
           # (le lecteur déborde, capacité insuffisante ou consommation en retard), le DRAINAGE
           # (on rend volontairement de la latence installée) et le RÉ-ANCRAGE (on repart de
           # zéro). `audio_drop_samples` les somme et masquait laquelle domine.
           "audio_drop_plafond": 0, "audio_drop_drainage": 0, "audio_drop_reancrage": 0,
           "audio_drop_generation": 0,
           # `audio_buf_ms` est un PIC (relevé juste après un ajout) : il mesure la taille des
           # rafales d'écriture de ffmpeg, PAS la latence. La latence, c'est le BAS DE L'EAU —
           # ce qui reste en permanence dans le tampon. Les confondre a fait dimensionner le
           # plafond sur un pic et jeter 40 % de l'audio (cf. en-tête du tampon).
           "audio_buf_ms": 0.0, "audio_latence_ms": None, "audio_in_sps": 0.0,
           # Verrouillage A/V : `av_locked` dit si le décalage est ÉTABLI (une trame présentée
           # depuis le démarrage de ffmpeg), pas si le réglage est coché — un verrou demandé
           # mais jamais accroché est le cas qu'il faut voir. `audio_headroom_ms` = marge
           # d'arrivée minimale sur la seconde écoulée (négatif = l'audio arrive APRÈS sa case).
           "av_lock": AV_LOCK, "av_locked": False, "av_margin_frames": AV_MARGIN_FRAMES,
           # `av_relocks` : le verrou SUIT une dérive lente (médiane de fenêtre). `av_reanchors` :
           # il a été RELÂCHÉ parce que plus rien n'était servi — un décrochage, pas une dérive.
           # Les confondre masquerait un régime instable derrière un compteur qui « bouge un peu ».
           "av_relocks": 0, "av_reanchors": 0, "audio_late_samples": 0, "audio_headroom_ms": None,
           # `av_skew_ms` : écart entre la ligne de temps DEMANDÉE (la case de la trame) et
           # celle qui ARRIVE (tête du tampon). Zéro = les deux compteurs partagent bien leur
           # origine. Un écart franc et stable dit que l'hypothèse d'origine commune est
           # fausse — c'est la mesure qui tranche, pas le raisonnement.
           "av_skew_ms": None, "av_fallback": None, "av_glisse_ms": None,
           "enabled": ENABLED, "net_in_kbps": 0.0,
           "slice_mode": bool(V_SLICE_H), "slice_h": V_SLICE_H,
           "total_slices": _V_TOTAL_SLICES}}

frame_lock = threading.Lock()
_last_frame = None            # bytes de la dernière trame conformée reçue
_last_frame_mono = None       # time.monotonic() de réception
_frames_in = 0                # compteur trames reçues (fenêtre in_fps)
# Numéro de la trame DANS SA GÉNÉRATION ffmpeg (0 = première trame sortie du filtre fps) et
# génération d'origine. C'est ce numéro, pas l'ordre d'arrivée, qui porte la ligne de temps.
_last_frame_no = None
_last_frame_gen = 0

audio_lock = threading.Lock()
# Blocs en attente : (numéro cumulé du 1er échantillon, float32 (n, 8), génération ffmpeg,
# index TAI d'arrivée). Le numéro CUMULÉ est ce qui rend le verrou A/V possible : il situe le
# bloc sur la ligne de temps de la source, indépendamment de son rang d'arrivée. L'index
# d'arrivée ne sert qu'à publier la marge (`audio_headroom_ms`).
_audio_buf = collections.deque()
_audio_buf_n = 0                   # échantillons dans _audio_buf (compteur, évite un sum() par chunk)
# Capacité et latence sont DEUX choses distinctes. On entre et on sort à 48 kHz, donc le tampon ne
# se vide jamais de lui-même : son occupation en régime établi EST la latence audio (retard sur la
# vidéo). Mais ffmpeg écrit dans le fifo par rafales de plusieurs dizaines de ms : un plafond bas
# écrête ces rafales (drops) puis affame le feeder — il faut donc de la capacité ET un drainage.
#   MAX    : capacité dure, absorbe les rafales d'écriture de ffmpeg.
#   TARGET : la latence VISÉE en régime établi.
# ⚠ Le drainage se décide sur le BAS DE L'EAU, pas sur un pic. L'ancien critère — « occupation
# instantanée > 60 ms → vider jusqu'à 30 ms » — reposait sur l'idée que l'occupation n'oscille
# que d'une vingtaine de ms. MESURÉ FAUX le 2026-08-23 sur une entrée en rafales : elle traverse
# le seuil des dizaines de fois par seconde, le drainage se déclenche à chaque fois, et **83 % de
# l'audio entrant partait au rebut** — suivi d'une famine, puisqu'on venait de jeter ce que le
# passage suivant réclamait. Capacité et latence sont deux choses distinctes, et une occupation
# instantanée ne mesure NI l'une NI l'autre : c'est son MINIMUM sur une fenêtre qui dit la latence
# réellement STOCKÉE (une rafale ne descend pas le minimum, un retard installé le monte). On ne
# jette donc que l'excédent constaté sur ce minimum, au plus une fois par fenêtre.
# La CAPACITÉ doit dépasser la plus grosse rafale d'écriture de ffmpeg, sinon on jette le surplus
# à l'entrée puis on affame en attendant la rafale suivante. MESURÉ le 2026-08-23 au banc sur une
# entrée en rafales, avec un plafond à 200 ms : 40 % de l'audio jeté AU PLAFOND et 40 % des blocs
# comblés au silence, en régime permanent — le tampon collé à 200 ms n'était pas « plein », il
# était ÉCRÊTÉ. (L'ancien drainage, en ramenant l'occupation à 30 ms, ne faisait qu'aggraver.)
# Le plafond passe donc à 1 s dans les DEUX régimes : c'est une capacité, pas une latence — la
# latence, elle, est ramenée à TARGET par le drainage au bas de l'eau. Coût : 1,5 Mo.
# VERROUILLÉ, la capacité doit en plus DÉTENIR L'AVANCE de l'audio sur sa case le temps que la
# case arrive. MESURÉ le 2026-08-23 en rejouant le flux réel : cette avance vaut ~720 ms (la
# branche vidéo — h264 1080p, bwdif, scale — traîne loin derrière la branche audio). Avec 1 s de
# capacité on écrêtait ce que la case suivante allait réclamer : rejets au plafond, famine, et un
# audio finalement posé à côté. 3 s laissent de la marge sans rien coûter d'autre que 4,5 Mo.
_AUDIO_BUF_MAX    = A_SAMPLE_RATE * (3000 if AV_LOCK else 1000) // 1000
_AUDIO_BUF_TARGET = A_SAMPLE_RATE *  30 // 1000
_AUDIO_BAS_EAU_NS = 2_000_000_000          # fenêtre du bas de l'eau
# VERROUILLÉ, ces seuils ne gouvernent plus la latence : elle vient du décalage `_av_d`, et
# l'occupation n'est plus qu'une marge d'arrivée. Le drainage est alors DÉSARMÉ (il jetterait des
# échantillons encore à l'heure) ; MAX reste, c'est une capacité.
_audio_seen = False                # a-t-on déjà reçu de l'audio ? (sinon la famine est normale)

# ─── verrou A/V : un seul entier ─────────────────────────────────────────────
# `_av_d` : index TAI d'échantillon = numéro cumulé de l'échantillon + _av_d. Établi à partir
# d'une paire (trame n, case de grille k) constatée à la présentation, remis à None à chaque
# (re)démarrage ffmpeg — les deux compteurs repartent de zéro ensemble.
av_lock_state = threading.Lock()
_av_d = None                       # CIBLE : ce que dit le dernier constat de présentation
_av_d_app = None                   # APPLIQUÉ : ce que le feeder utilise réellement
# GLISSEMENT. Déplacer le verrou d'un coup jette (ou redemande) tout l'écart en une fois —
# mesuré au banc : 140 ms d'audio au rebut sur UN déplacement, soit un trou franchement audible.
# On rejoint donc la cible par pas d'UN échantillon tous les _AV_GLISSE_BLOCS blocs de 1 ms, soit
# 0,5 % de vitesse : inaudible, et l'écart se résorbe tout seul. Le premier accrochage, lui, est
# immédiat (rien ne joue encore, il n'y a rien à ménager).
_AV_GLISSE_BLOCS = 4
_av_gen = 0                        # génération ffmpeg courante (une par process, cf. superviseur)
_av_win = []                       # mesures de D sur la fenêtre courante (cf. _av_note)
_av_cand = None                    # médiane candidate, à confirmer sur une 2ᵉ fenêtre
_av_win_t0 = 0
# REPLI. Le verrou repose sur une hypothèse : les deux sorties ffmpeg partagent l'origine de
# leur ligne de temps. Si elle est fausse, la case demandée ne rencontre jamais l'audio qui
# arrive et la sortie devient un silence permanent — le pire des échecs, propre en apparence.
# Au bout de _AV_FALLBACK_S sans un seul échantillon servi ALORS QUE l'audio arrive, on repasse
# en cadencement libre (comportement d'avant la 0.9.0), on le JOURNALISE et on le publie.
_AV_FALLBACK_S = 3.0
# Le repli n'est pas définitif : on retente VITE. Mesuré au banc : la rafale de rattrapage qui
# suit une (re)connexion décroche le verrou pendant une poignée de secondes ; avec 10 s d'attente
# avant de retenter, chaque reconnexion coûtait 10 s de plus en cadencement libre pour rien.
_AV_RETRY_S    = 2.0
_av_fallback = None                # None = verrou en vigueur ; sinon la raison, publiée
# RÉ-ANCRAGE RAPIDE. D n'est valable que tant que la relation « trame présentée ↔ case de
# grille » tient. Elle ne tient PAS pendant une rafale de rattrapage (après une reconnexion
# SRT, ffmpeg décode à 3× le temps réel : mesuré in_fps 152 pour une grille à 50) — la case
# demandée décroche alors de l'audio qui arrive en quelques centaines de ms. Dès que plus rien
# n'est servi pendant _AV_REANCHOR_S ALORS QUE l'audio est là, on relâche D : la prochaine
# trame présentée le repose, sur une paire fraîche. C'est la correction lip-sync CORRECTE
# (elle vient de la vidéo), pas un alignement opportuniste sur ce qui traîne dans le tampon.
_AV_REANCHOR_S = 1.0
_AV_ATTENTE_MAX_S = 2.0            # au-delà, une « attente » n'en est plus une

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

def _tai_offset_ns():
    """Écart horloge de la grille genlock (CLOCK_REALTIME) → horloge TAI du bus MXL.
    Les index de grain VIDÉO sont posés depuis la grille, les index AUDIO depuis now_tai() :
    sur un conteneur dont l'horloge n'est pas déjà en TAI, poser l'audio à la case de la
    vidéo sans cette conversion le décalerait de l'écart TAI-UTC entier (37 s)."""
    try:
        return bobimxl.now_tai() - time.clock_gettime_ns(time.CLOCK_REALTIME)
    except Exception:
        return 0

def _grid_tai_sample(k):
    """Case de grille vidéo k → index TAI d'échantillon audio du MÊME instant."""
    return (_grid_ns(k) + _tai_offset_ns()) * A_SAMPLE_RATE // 1_000_000_000

def _av_note(gen, n, k):
    """Constat de présentation : la trame n (de la génération gen) part à la case k.
    Appelée à la PREMIÈRE présentation d'une trame — une trame répétée (source plus lente que
    la grille, hoquet réseau) donnerait une mesure gonflée d'une case."""
    global _av_d, _av_d_app, _av_win, _av_win_t0, _av_cand
    d = _grid_tai_sample(k) - _samples_at_frame(n)
    relock = 0
    with av_lock_state:
        if gen != _av_gen:
            return                     # trame d'une génération ffmpeg périmée : ne rien bouger
        now = _grid_now_ns()
        if _av_d is None:
            _av_d = d; _av_d_app = d      # premier accrochage : immédiat
            _av_win = []; _av_win_t0 = now; relock = -1
        else:
            _av_win.append(d)
            if now - _av_win_t0 >= _AV_WINDOW_NS:
                # MÉDIANE, ni min ni moyenne. Le minimum a été essayé le 2026-08-23 et MESURÉ
                # nuisible : il retient l'alignement le PLUS SERRÉ de la fenêtre (la trame la
                # moins retardée), c'est-à-dire celui qui laisse zéro marge d'arrivée à
                # l'audio — la marge observée tombait de 280 ms à −1 ms en 20 s, puis la
                # famine s'installait. La médiane vise la présentation TYPIQUE ; la marge, elle,
                # est un choix explicite (retenue vidéo), pas un effet de bord de l'estimateur.
                # La moyenne, elle, suivrait chaque trame en retard.
                _av_win.sort()
                med = _av_win[len(_av_win) // 2]
                # CONFIRMATION sur deux fenêtres. Une source 25p remontée en 50p arrive par
                # paires : la médiane bascule entre deux valeurs distantes d'une trame et la
                # phase glisse lentement — mesuré, un déplacement du verrou toutes les ~10 s,
                # chacun un saut audible de 20 ms et 100 ms d'audio jeté. Une dérive RÉELLE,
                # elle, tient sur deux fenêtres d'affilée : on la suit toujours, 5 s plus tard.
                if abs(med - _av_d) > _AV_DEADBAND:
                    if _av_cand is not None and abs(med - _av_cand) <= _AV_DEADBAND:
                        _av_d = med; relock = 1; _av_cand = None
                    else:
                        _av_cand = med
                else:
                    _av_cand = None
                _av_win = []; _av_win_t0 = now
    if relock:
        with metrics_lock:
            metrics["av_locked"] = True
            if relock > 0:
                metrics["av_relocks"] += 1

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

# Dernier (writer, index) vidéo RÉELLEMENT posé, pour `grid_ahead_ms` sur :8080. Relevé ici
# plutôt que calculé par trame : la comparaison à l'horloge se fait à la requête, coût nul.
# Progressif seulement — en entrelacé l'index est une coordonnée de CHAMP sur un flux à la
# cadence TRAME, et `index_time_ns` rendrait un instant faux d'un facteur deux.
_LAST_VID_IDX = [None, 0]

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
            _LAST_VID_IDX[0] = w; _LAST_VID_IDX[1] = _i
            _emit_grain_sliced(w, gi, vw, frame_bytes)
        else:
            _i, gi, vw = w.open_grain(index=(fbase if GENLOCK else None))
            _LAST_VID_IDX[0] = w; _LAST_VID_IDX[1] = _i
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

def _ffmpeg_cmd(with_audio, fifo_path):
    # bwdif deint=interlaced : ne désentrelace QUE les trames marquées entrelacées (passthrough
    # sinon) → chaîne unique quel que soit le flux entrant. fps=grille de sortie (cadence TRAME).
    # start_time=0 (verrou A/V) : la trame n est posée au pts source n·D/N ABSOLU, par
    # remplissage/rognage en tête — le symétrique exact du `first_pts=0` de l'audio. Sans lui,
    # la trame 0 vaut le premier pts VIDÉO tandis que l'échantillon 0 vaut le pts 0 : l'écart
    # de démarrage entre les deux pistes (l'audio ouvre presque toujours avant la vidéo)
    # deviendrait une erreur de lip-sync CONSTANTE, et invisible.
    _fps = "fps=%d/%d%s" % (FPS_NUM, FPS_DEN, ":start_time=0" if AV_LOCK else "")
    vf = ("bwdif=mode=send_frame:parity=auto:deint=interlaced,"
          "scale=%d:%d:flags=bicubic,%s,format=%s,setsar=1"
          % (WIDTH, HEIGHT, _fps, PIX_FMT))
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
                "-f", "f32le", "-flush_packets", "1", "-y", fifo_path]
    if TAP_UDP:
        # Sortie SUPPLÉMENTAIRE en copie de flux : ni décodage ni ré-encodage, donc les pts sont
        # ceux de la source, intacts. UDP et non SRT : sans destinataire, on écrit dans le vide,
        # ce qui ne peut pas faire tomber les deux autres sorties.
        cmd += ["-map", "0:v:0", "-map", "0:a:0?", "-c", "copy",
                "-f", "mpegts", "udp://%s?pkt_size=1316" % TAP_UDP]
    return cmd

# ─── lecteurs de sortie ffmpeg (ne bloquent jamais le feeder) ────────────────
def _video_reader(proc, gen):
    global _last_frame, _last_frame_mono, _frames_in, _last_frame_no, _last_frame_gen
    buf = bytearray()
    n = 0                              # numéro de trame DANS CETTE génération (0 = pts 0)
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
                _last_frame_no = n
                _last_frame_gen = gen
            n += 1

def _audio_seen_set():
    global _audio_seen
    _audio_seen = True

def _audio_reader(gen, path):
    """Une session de lecture par génération ffmpeg (rendez-vous bloquant OK : thread dédié,
    jamais sur le chemin vidéo). Blocs f32le 8ch → deque bornée consommée par audio_feeder,
    chacun étiqueté de son numéro CUMULÉ (0 = pts 0, cf. first_pts=0) et de sa génération.
    Le thread se termine quand plus personne n'écrit : le superviseur « sonne » le fifo après
    la mort de ffmpeg, ce qui débloque un open() resté en attente (flux sans piste audio)."""
    global _audio_buf_n
    blk = A_SPMS * A_CHANNELS * 4 * 10          # 10 ms
    win_n = 0; win_t0 = time.monotonic()
    cum = 0
    try:
        with open(path, "rb") as f:
            while True:
                data = f.read(blk)
                if not data:
                    break                        # tous les écrivains partis → fin de session
                n = len(data) // (A_CHANNELS * 4)
                if n <= 0: continue
                arr = np.frombuffer(data, dtype=np.float32,
                                    count=n * A_CHANNELS).reshape(n, A_CHANNELS)
                _audio_seen_set()
                ai = _tai_sample_index()
                dropped = 0
                with audio_lock:
                    _audio_buf.append((cum, arr, gen, ai)); _audio_buf_n += n
                    while _audio_buf_n > _AUDIO_BUF_MAX and _audio_buf:
                        d = _audio_buf.popleft()[1].shape[0]
                        _audio_buf_n -= d; dropped += d
                    tot = _audio_buf_n
                cum += n
                win_n += n
                el = time.monotonic() - win_t0
                with metrics_lock:
                    if dropped:
                        metrics["audio_drop_samples"] += dropped
                        metrics["audio_drop_plafond"] += dropped
                    metrics["audio_buf_ms"] = round(tot / float(A_SPMS), 1)
                    if el >= 1.0:
                        metrics["audio_in_sps"] = round(win_n / el)
                if el >= 1.0:
                    win_n = 0; win_t0 = time.monotonic()
    except Exception as e:
        log("audio: session %d interrompue : %s" % (gen, e), "warning")

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
def _fifo_close(path, had_audio):
    """Fin de session audio : on « sonne » le fifo (ouverture en écriture puis fermeture) pour
    débloquer un lecteur resté en attente d'un écrivain — ffmpeg peut mourir sans jamais avoir
    ouvert le fifo (flux sans piste audio, échec au démarrage), et le thread resterait pendu
    sur open() pour toujours. ENXIO = plus de lecteur, il n'y a rien à débloquer."""
    if not had_audio:
        return
    try:
        os.close(os.open(path, os.O_WRONLY | os.O_NONBLOCK))
    except Exception:
        pass
    try:
        os.unlink(path)     # le lecteur garde l'inode ouvert le temps de vider ce qui reste
    except Exception:
        pass

def supervisor():
    global _proc, _av_gen, _av_d, _av_d_app, _av_win, _av_cand, _av_fallback
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
        # Nouvelle GÉNÉRATION : les deux compteurs (trames, échantillons) repartent de zéro
        # ENSEMBLE, et le verrou A/V est relâché — il se réaccroche sur la première trame
        # présentée. C'est le seul endroit où la génération avance.
        with av_lock_state:
            _av_gen += 1; gen = _av_gen
            _av_d = None; _av_d_app = None; _av_win = []; _av_cand = None
            _av_fallback = None       # nouvelle génération = nouvelle chance d'accrocher
        with metrics_lock:
            metrics["av_locked"] = False
            metrics["av_fallback"] = None
            metrics["av_skew_ms"] = None
        fifo = _audio_fifo_path(gen)
        if with_audio:
            try:
                os.mkfifo(fifo)
            except FileExistsError:
                pass
            except Exception as e:
                _stderr_tail.append("mkfifo: %s" % e)
            threading.Thread(target=_audio_reader, args=(gen, fifo), daemon=True).start()
        try:
            proc = subprocess.Popen(_ffmpeg_cmd(with_audio, fifo), stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE, bufsize=V_FRAME_SIZE)
        except Exception as e:
            _stderr_tail.append("spawn: %s" % e)
            _fifo_close(fifo, with_audio)
            time.sleep(2); continue
        with _proc_lock:
            _proc = proc
        threading.Thread(target=_video_reader, args=(proc, gen), daemon=True).start()
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
        _fifo_close(fifo, with_audio)
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
    shm = bobimxl.Writer(inst, VIDEO_SHM, WIDTH, HEIGHT, CHROMA, BIT_DEPTH, FPS_NUM, FPS_DEN,
                         interlace=SCAN,
                         **_SLICE_KW)
    idx = 0
    last_conn = None
    hold = collections.deque()        # trames retenues (AV_MARGIN_FRAMES cases)
    last_noted = (None, None)         # (génération, n° de trame) déjà porté au verrou
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
            fno = _last_frame_no; fgen = _last_frame_gen
        age_ms = None if lm is None else (time.monotonic() - lm) * 1000.0
        # RETENUE : on présente la trame qui était la plus fraîche AV_MARGIN_FRAMES cases plus
        # tôt, pour rendre au chemin audio la marge d'arrivée que le verrou lui retire. L'état
        # de connexion, lui, se juge sur l'arrivée COURANTE : la retenue décale l'image, elle
        # ne doit pas vieillir le diagnostic ni déclencher un faux gel.
        if AV_MARGIN_FRAMES:
            hold.append((frame, fno, fgen))
            shown, fno, fgen = hold.popleft() if len(hold) > AV_MARGIN_FRAMES \
                               else (None, None, None)
        else:
            shown = frame
        if not _enabled.is_set():
            shown = IDLE_FRAME; conn = "off"
        elif frame is None or shown is None:
            shown = IDLE_FRAME; conn = "waiting"
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
        _write_video(shm, shown, kf); idx += 1
        # Constat porté au verrou A/V à la PREMIÈRE présentation d'une trame : une trame
        # répétée (source plus lente que la grille, hoquet réseau) partirait une case plus
        # tard et gonflerait la mesure. Rien n'est noté sur une mire ou une trame gelée : ce
        # n'est pas la source qui cadence.
        if AV_LOCK and conn == "connected" and fno is not None and (fgen, fno) != last_noted:
            last_noted = (fgen, fno)
            _av_note(fgen, fno, kf)
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

def _fill_locked(want, out):
    """Remplit `out` avec les échantillons de numéros CUMULÉS [want, want+A_BLK) : la case de
    grille et le contenu sont liés par la ligne de temps de la source, plus par l'ordre
    d'arrivée. Renvoie (utiles, jetés en retard, index TAI d'arrivée du 1er bloc servi)."""
    global _audio_buf_n
    got = 0; late = 0; arrived = None; tete = None
    with audio_lock:
        if _audio_buf:
            tete = _audio_buf[0][0]
        while _audio_buf:                       # sa case est passée : arrivé trop tard
            s0, arr, _g, _ai = _audio_buf[0]
            if s0 + arr.shape[0] <= want:
                _audio_buf.popleft(); _audio_buf_n -= arr.shape[0]; late += arr.shape[0]
            else:
                break
        while got < A_BLK and _audio_buf:
            s0, arr, g, ai = _audio_buf[0]
            if s0 > want + got:                 # pas encore arrivé → trou, complété en silence
                break
            off = want + got - s0
            take = min(A_BLK - got, arr.shape[0] - off)
            out[got:got + take] = arr[off:off + take]
            if arrived is None: arrived = ai
            got += take
            if off + take >= arr.shape[0]:
                _audio_buf.popleft(); _audio_buf_n -= arr.shape[0]
            else:                               # le bloc a encore de la matière pour la suite
                _audio_buf[0] = (s0 + off + take, arr[off + take:], g, ai)
                _audio_buf_n -= (off + take)
    return got, late, arrived, (None if tete is None else tete - want)

def audio_feeder():
    global _audio_buf_n, _av_d, _av_d_app, _av_win, _av_cand, _av_fallback
    aw = bobimxl.AudioWriter(inst, AUDIO_SHM, channels=A_CHANNELS, sample_rate=A_SAMPLE_RATE)   # l'index est fourni explicitement à write()
    silence = np.zeros((A_BLK, A_CHANNELS), dtype=np.float32)
    out = np.empty((A_BLK, A_CHANNELS), dtype=np.float32)
    pending = None                        # reliquat du bloc en cours (chemin LIBRE seulement)
    next_idx = _tai_sample_index() - A_BLK
    hw_min = None; hw_t0 = _grid_now_ns()          # fenêtre 1 s de la marge d'arrivée
    served_t = time.monotonic()                    # dernier échantillon RÉELLEMENT servi
    fallback_t = None                              # date du repli, pour le retenter
    dernier_ecart = None                           # écart de tête du dernier bloc verrouillé
    glisse_n = 0                                   # compteur de blocs du glissement
    cible = None
    bas_eau = 0; bas_t0 = _grid_now_ns()           # bas de l'eau du tampon (cf. drainage)
    latence_ms = None
    while True:
        cur = _tai_sample_index()
        with av_lock_state:
            # TROIS régimes, pas deux. Les confondre a coûté une sortie MUETTE pendant tout le
            # repli (mesuré : ~1000 blocs de silence par seconde là où le repli devait rendre
            # exactement le comportement d'avant la 0.9.0).
            #   "libre"   — verrou désactivé ou en repli : on débite au fil de l'eau
            #   "attente" — verrou demandé, pas encore accroché : silence cadencé, volontaire
            #   "verrou"  — D établi : on tire par la ligne de temps
            gen = _av_gen
            if not AV_LOCK or _av_fallback:
                regime = "libre"; d = None
            elif _av_d is None:
                regime = "attente"; d = None
            else:
                regime = "verrou"; d = _av_d_app; cible = _av_d
        # Le repli n'est pas une condamnation : on retente le verrou périodiquement. Une rafale
        # de rattrapage passe ; une hypothèse fausse, elle, se reverra dans `av_skew_ms`.
        if _av_fallback and fallback_t and time.monotonic() - fallback_t > _AV_RETRY_S:
            with av_lock_state:
                _av_fallback = None
                _av_d = None
            with metrics_lock:
                metrics["av_fallback"] = None
            journal("av_retry")
            fallback_t = None; served_t = time.monotonic()
        drained = 0; stale = 0
        with audio_lock:
            # Reliquat d'une génération ffmpeg périmée (le fifo est par génération : un bloc
            # d'une autre ne peut être qu'antérieur). L'écrire poserait l'audio d'AVANT une
            # reconnexion sur la ligne de temps d'APRÈS.
            while _audio_buf and _audio_buf[0][2] != gen:
                a = _audio_buf.popleft()[1]
                _audio_buf_n -= a.shape[0]; stale += a.shape[0]
            if stale:
                pending = None
            # Drainage : SANS verrou seulement, là où l'occupation du tampon EST la latence
            # audio. Verrouillé, la latence vient de `_av_d` et l'occupation n'est plus qu'une
            # marge d'arrivée : la jeter créerait la famine qu'on cherche justement à éviter.
            # Le critère est le BAS DE L'EAU sur la fenêtre écoulée (cf. en-tête) : une rafale ne
            # le descend pas, un retard installé le monte. On ne jette que l'excédent constaté.
            if regime == "libre":
                if _audio_buf_n < bas_eau:
                    bas_eau = _audio_buf_n
                if _grid_now_ns() - bas_t0 >= _AUDIO_BAS_EAU_NS:
                    trop = bas_eau - _AUDIO_BUF_TARGET
                    while trop > 0 and _audio_buf:
                        s0, arr, g, ai = _audio_buf[0]
                        if arr.shape[0] <= trop:
                            _audio_buf.popleft()
                            _audio_buf_n -= arr.shape[0]
                            drained += arr.shape[0]; trop -= arr.shape[0]
                        else:              # ne jeter QUE l'excédent, pas le bloc entier
                            _audio_buf[0] = (s0 + trop, arr[trop:], g, ai)
                            _audio_buf_n -= trop; drained += trop; trop = 0
                    if drained:
                        pending = None
                    latence_ms = bas_eau / float(A_SPMS)
                    bas_eau = _audio_buf_n; bas_t0 = _grid_now_ns()
            else:
                bas_eau = _audio_buf_n; bas_t0 = _grid_now_ns()
        if latence_ms is not None:
            with metrics_lock:
                metrics["audio_latence_ms"] = round(latence_ms, 1)
            latence_ms = None
        if drained or stale:
            with metrics_lock:
                metrics["audio_drop_samples"] += drained + stale
                metrics["audio_drop_drainage"] += drained
                metrics["audio_drop_generation"] += stale
        # Pacing : `next_idx` suit la grille TAI dans LES DEUX régimes — c'est le contrat de
        # granularité 1 ms avec les consommateurs, indépendant du choix du contenu. Trop loin
        # derrière ou devant (stall système) → un seul saut, pas un décalage qui s'accumule.
        if abs(cur - next_idx) > A_MAX_LAG:
            next_idx = cur - A_BLK
            if regime == "libre":
                with audio_lock:
                    perdu = _audio_buf_n
                    _audio_buf.clear(); _audio_buf_n = 0
                with metrics_lock:
                    metrics["audio_drop_samples"] += perdu
                    metrics["audio_drop_reancrage"] += perdu
            pending = None
            with metrics_lock:
                metrics["audio_reanchors"] += 1
        while next_idx + A_BLK <= cur:
            starve = False
            if regime == "verrou" and cible is not None and d != cible:
                glisse_n += 1
                if glisse_n >= _AV_GLISSE_BLOCS:
                    glisse_n = 0
                    d += 1 if cible > d else -1
                    with av_lock_state:
                        _av_d_app = d
            if regime == "attente":
                # Verrou demandé, pas encore accroché (aucune trame présentée depuis le
                # démarrage de ffmpeg) : silence cadencé, plutôt qu'un audio posé au hasard
                # qu'il faudrait ensuite déplacer.
                blk = silence; starve = _audio_seen
            elif regime == "verrou":
                got, late, arrived, ecart = _fill_locked(next_idx - d, out)
                if got < A_BLK:
                    out[got:] = 0.0
                    starve = _audio_seen
                blk = out
                if late:
                    with metrics_lock:
                        metrics["audio_late_samples"] += late
                if arrived is not None:
                    hm = (next_idx - arrived) / float(A_SPMS)
                    if hw_min is None or hm < hw_min: hw_min = hm
                if got:
                    served_t = time.monotonic()
                dernier_ecart = ecart
            else:
                need = A_BLK; pos = 0
                with audio_lock:
                    while need > 0:
                        if pending is not None and len(pending):
                            take = min(need, len(pending))
                            out[pos:pos + take] = pending[:take]
                            pending = pending[take:] if take < len(pending) else None
                            pos += take; need -= take
                        elif _audio_buf:
                            pending = _audio_buf.popleft()[1]
                            _audio_buf_n -= pending.shape[0]
                        else:
                            break
                blk = out
                if need > 0:                  # pas assez de frais → compléter en silence
                    if pos == 0:
                        blk = silence
                    else:
                        out[pos:] = 0.0
                    starve = _audio_seen
            try: aw.write(blk, index=next_idx)
            except Exception: pass
            if starve:                        # avant le 1er échantillon, le silence est normal
                with metrics_lock:
                    metrics["audio_starve_blocks"] += 1
            next_idx += A_BLK
        now = _grid_now_ns()
        if now - hw_t0 >= 1_000_000_000:
            # MINIMUM de la fenêtre, pas moyenne : c'est le bloc le PLUS juste arrivé qui dit
            # combien de marge il reste avant que la famine commence.
            skew = None
            with audio_lock:
                tete = _audio_buf[0][0] if _audio_buf else None
            if d is not None and tete is not None:
                skew = round((tete - (next_idx - d)) / float(A_SPMS), 1)
            with metrics_lock:
                metrics["audio_headroom_ms"] = None if hw_min is None else round(hw_min, 1)
                metrics["av_skew_ms"] = skew
                # Reste à résorber : non nul = un déplacement de verrou est en cours d'absorption,
                # sans trou.
                metrics["av_glisse_ms"] = (None if (d is None or cible is None)
                                           else round((cible - d) / float(A_SPMS), 1))
            hw_min = None; hw_t0 = now
        # Rien n'est servi alors que l'audio est là. Deux situations se ressemblent ici et
        # n'ont RIEN à voir — c'est le SIGNE de l'écart de tête qui les sépare :
        #   écart > 0 : la case réclame de l'audio PAS ENCORE ARRIVÉ. C'est une attente NORMALE,
        #     bornée (écart/48 ms), et c'est le cas de tout accrochage : le verrou se pose sur
        #     une trame dont l'audio est encore en vol. Ré-ancrer ici est exactement la mauvaise
        #     réponse — mesuré : 10 s de silence à chaque reconnexion, et 83 % de l'audio jeté.
        #   écart ≤ 0 : la case a DÉPASSÉ ce qui arrive, et ce qui arrive part au rebut. Là, la
        #     paire (trame, case) est vraiment périmée et il faut la refaire.
        # Garde-fou : une attente de plus de _AV_ATTENTE_MAX_S n'est plus une attente.
        attente = (dernier_ecart is not None and dernier_ecart > 0
                   and dernier_ecart < _AV_ATTENTE_MAX_S * A_SAMPLE_RATE)
        if (d is not None and _audio_seen and _audio_buf and not attente
                and time.monotonic() - served_t > _AV_REANCHOR_S):
            mort = time.monotonic() - served_t
            if mort > _AV_FALLBACK_S:
                # Ré-ancrer ne suffit pas : sur cette source, la case demandée ne rencontre
                # jamais l'audio qui arrive. On ne reste pas muet — cadencement libre, dit et
                # publié, et retenté dans _AV_RETRY_S.
                _av_fallback = (
                    "aucun échantillon servi en %.1f s malgré les ré-ancrages (écart %s ms) — "
                    "cadencement libre" % (mort, metrics.get("av_skew_ms")))
                with metrics_lock:
                    metrics["av_fallback"] = _av_fallback
                    metrics["av_locked"] = False
                journal("av_fallback", raison=_av_fallback, skew_ms=metrics.get("av_skew_ms"))
                log("verrou A/V : %s" % _av_fallback, "warning")
                fallback_t = time.monotonic(); served_t = fallback_t
            else:
                with av_lock_state:
                    _av_d = None; _av_d_app = None; _av_win = []; _av_cand = None      # la prochaine trame repose D
                with metrics_lock:
                    metrics["av_reanchors"] += 1
                served_t = time.monotonic() - _AV_REANCHOR_S / 2.0   # laisse le temps de reposer
        # Période < 1 ms : un seul bloc par passage, donc head_index avance de façon CONTINUE.
        # Dormir plus longtemps ferait avancer la tête par paquets et rouvrirait la fenêtre
        # d'injection de silence chez le consommateur (cf. contrat 1 ms ci-dessus).
        time.sleep(0.0005)

# ─── HTTP : :8080 metrics + :8082 control ────────────────────────────────────
_SRC_INT = ("listen_port", "srt_latency_ms")
# ─── Ce qui a un EFFET, selon le mode ────────────────────────────────────────────────────────
# Les modes ÉCOUTE se lient à `listen_port` et n'ouvrent jamais `url` ; les modes APPEL/PULL font
# l'inverse. Un champ posé hors de son mode était accepté, persisté, et sans le moindre effet :
# régler « srt://…:10000 » sur une entrée en écoute laissait l'écoute sur son port d'avant, sans
# rien signaler. C'est le champ qui doit refuser, pas l'exploitant qui doit connaître la règle.
_SRC_UTILES = {{
    "srt_listener": ("listen_port", "srt_latency_ms", "srt_passphrase", "srt_streamid"),
    "rtmp_server":  ("listen_port", "rtmp_app", "rtmp_key"),
    "srt_caller":   ("url", "srt_latency_ms", "srt_passphrase", "srt_streamid"),
    "rtmp_pull":    ("url",),
}}
_SRC_STR = ("url", "srt_passphrase", "srt_streamid", "rtmp_app", "rtmp_key")

def _apply_source(body):
    """Applique un nouveau point d'entrée à chaud.

    Renvoie (changed, inertes) : `inertes` = les clés fournies qui n'ont AUCUN effet dans le mode
    courant. L'appelant en fait une erreur — les avaler en silence est ce qui a fait croire qu'un
    port était réglé alors que l'écoute tournait ailleurs."""
    _utiles = _SRC_UTILES.get(MODE) or (_SRC_INT + _SRC_STR)
    inertes = sorted(k for k in body if k in (_SRC_INT + _SRC_STR) and k not in _utiles)
    body = {{k: v for k, v in body.items() if k not in inertes}}
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
    if inertes:
        journal("source_inerte", mode=MODE, ignores=inertes)
    return changed, inertes

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
              "av_lock": AV_LOCK, "av_margin_frames": AV_MARGIN_FRAMES, "tap_udp": TAP_UDP,
              "av_fallback": _av_fallback,
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
        # Avance des index ÉMIS sur la grille TAI (cf. bobimxl.index_report) : seul témoin d'une
        # sortie écrite dans le FUTUR, épisode pendant lequel la cadence reste parfaite et où
        # tout délai d'étage mesuré en aval est refusé sans dire pourquoi (régie légère, 25/10 :
        # deux mélangeurs à 68 s d'avance pendant des heures).
        # ⚠ PORTÉE, et elle n'est pas intuitive : en mode genlock (LE DÉFAUT ici), l'index vidéo
        # est posé par `_write_video` depuis `_grid_k`, et `next_index` n'est jamais appelé — ce
        # rapport reste donc vide et ne dit RIEN du chemin genlock. Publier `index_ahead` seul
        # serait un faux témoin. C'est `grid_ahead_ms` qui mesure ce chemin-là, en comparant
        # l'index RÉELLEMENT posé à la grille TAI du bus ; les deux sont nécessaires.
        payload["index_ahead"] = bobimxl.index_report()
        if _LAST_VID_IDX[0] is not None:
            try:
                payload["grid_ahead_ms"] = round(
                    (_LAST_VID_IDX[0].index_time_ns(_LAST_VID_IDX[1])
                     - bobimxl.now_tai()) / 1e6, 1)
            except Exception:
                pass
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
            _chg, _inertes = _apply_source(body)
            if _inertes:
                # 400 dès qu'un champ est inerte, MÊME si d'autres ont pris : accepter à moitié en
                # rendant « ok » laisse croire que tout est posé. Le message NOMME le champ utile.
                _att = {{"srt_listener": "listen_port", "rtmp_server": "listen_port"}}.get(MODE, "url")
                self._send_json(400, {{
                    "ok": False, "applied": _chg, "ignored": _inertes, "mode": MODE,
                    "error": "en mode %s, %s n'a aucun effet — le champ qui gouverne est « %s »"
                             % (MODE, " et ".join(_inertes), _att)}})
                return
            self._send_json(200, {{"ok": True, "applied": _chg}})
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
# Fifos d'un run précédent (le rootfs peut survivre à un simple redémarrage du script) : les
# numéros de génération repartent de 0, un fifo laissé en place serait réutilisé par la
# nouvelle génération du même numéro. On fait place nette.
try:
    import glob as _glob
    for _f in _glob.glob("%s/stream_in_audio.*.f32" % AUDIO_FIFO_DIR):
        try: os.unlink(_f)
        except Exception: pass
except Exception:
    pass

journal("script_start", version=PLUGIN_VERSION, mode=MODE, enabled=ENABLED,
        out_format="%dx%d%s%d" % (WIDTH, HEIGHT, _SCAN_CFG, FPS),
        av_lock=AV_LOCK, av_margin_frames=AV_MARGIN_FRAMES)

threading.Thread(target=lambda: HTTPServer(("0.0.0.0", 8080), MetricsHandler).serve_forever(), daemon=True).start()
threading.Thread(target=lambda: HTTPServer(("0.0.0.0", 8082), ControlHandler).serve_forever(), daemon=True).start()
threading.Thread(target=supervisor, daemon=True).start()
threading.Thread(target=history_sampler, daemon=True).start()
threading.Thread(target=video_feeder, daemon=True).start()
threading.Thread(target=audio_feeder, daemon=True).start()

while True:
    time.sleep(1)
