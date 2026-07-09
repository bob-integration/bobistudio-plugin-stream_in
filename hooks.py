# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright (C) 2026 BOBI SAS, France
# Auteur : Cyril Mazouer, pour le compte de BOBI SAS
# Distribué sous licence GNU GPL v3 (ou ultérieure) ; voir le fichier LICENSE.


_SOURCE_KEYS = ("url", "listen_port", "rtmp_app", "rtmp_key",
                "srt_latency_ms", "srt_passphrase", "srt_streamid")


def control_action(action, body, params, ctx):
    """POST /api/containers/<vmid>/control/<action>.
    `source` : nouveau point d'entrée (adresse/port/app/clé/options SRT) — persisté puis
               appliqué à chaud (:8082/source relance ffmpeg, la sortie MXL ne se coupe pas).
    `power`  : marche/arrêt de l'ingest — la vidéo continue d'émettre la mire à l'arrêt."""
    new_params = dict(params)
    if action == "source":
        hot = {k: body[k] for k in _SOURCE_KEYS if k in body}
        if not hot:
            return {"params": new_params, "hot_endpoint": None, "hot_body": None}
        for k, v in hot.items():
            new_params[k] = v
        return {"params": new_params, "hot_endpoint": "/source", "hot_body": hot}
    if action == "power":
        want = bool(body.get("enabled"))
        new_params["enabled"] = want
        return {"params": new_params, "hot_endpoint": "/enable", "hot_body": {"enabled": want}}
    return None


def source_shm(params, context):
    """Colonnes lisibles source/shm_out pour le dashboard : la « source » d'un stream_in
    est son point d'entrée réseau (mode + URL ou port d'écoute)."""
    mode = str(params.get("mode") or "srt_listener")
    port = params.get("listen_port") or 9000
    if mode == "srt_listener":
        source = f"srt écoute :{port}"
    elif mode == "rtmp_server":
        app = str(params.get("rtmp_app") or "live")
        source = f"rtmp écoute :{port}/{app}"
    else:
        source = f"{'srt' if mode == 'srt_caller' else 'rtmp'} ← {params.get('url') or '?'}"
    hn = params.get("hostname", "mxl")
    return {"source": source, "shm": f"{hn} · {hn}_audio"}
