# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright (C) 2026 BOBI SAS, France
# Auteur : Cyril Mazouer, pour le compte de BOBI SAS
# Distribué sous licence GNU GPL v3 (ou ultérieure) ; voir le fichier LICENSE.


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
