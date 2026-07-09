// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 BOBI SAS, France
// Auteur : Cyril Mazouer, pour le compte de BOBI SAS
// Distribué sous licence GNU GPL v3 (ou ultérieure) ; voir le fichier LICENSE.

// UI de contrôle embarquée du plugin Stream In (montée dans le shell Sources).
// Contrat : window.MXLPlugins.stream_in.mount(el, vmid, ctx) / unmount().
window.MXLPlugins = window.MXLPlugins || {};
window.MXLPlugins.stream_in = (function () {
    let EL = null, VMID = null, TOAST = () => {};
    let state = null;
    let dockerIp = null;    // IP macvlan du conteneur (containers.docker_ip) — pour l'URL d'ingest
    let pollTimer = null;

    const t = (k) => (window.t ? window.t(k) : k);

    const esc = s => String(s == null ? '' : s).replace(/[&<>"']/g, c => ({
        '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
    }[c]));

    const api = (path, body) => fetch(`/api/containers/${VMID}/plugin${path}`, {
        method: body === undefined ? 'GET' : 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: body === undefined ? undefined : JSON.stringify(body || {}),
    });

    const $ = sel => EL ? EL.querySelector(sel) : null;

    const MODES = {
        srt_listener: 'plugin.stream_in.mode_srt_listener',
        srt_caller:   'plugin.stream_in.mode_srt_caller',
        rtmp_server:  'plugin.stream_in.mode_rtmp_server',
        rtmp_pull:    'plugin.stream_in.mode_rtmp_pull',
    };

    async function fetchDockerIp() {
        try {
            const r = await fetch('/api/containers');
            if (!r.ok) return;
            const rows = await r.json();
            const me = (Array.isArray(rows) ? rows : []).find(c => String(c.vmid) === String(VMID));
            if (me && me.docker_ip) dockerIp = me.docker_ip;
        } catch (e) { /* best-effort : l'URL affichera <ip> tant qu'inconnue */ }
    }

    // URL que la SOURCE distante doit utiliser (modes écoute) — l'IP est celle du conteneur
    // (macvlan, joignable en direct sur le LAN), pas celle du nœud ni de l'orchestrateur.
    function ingestUrl() {
        if (!state) return '';
        const ip = dockerIp || '<ip>';
        if (state.mode === 'srt_listener') {
            let u = `srt://${ip}:${state.listen_port}?mode=caller`;
            if (state.srt_streamid) u += `&streamid=${state.srt_streamid}`;
            return u;
        }
        if (state.mode === 'rtmp_server') {
            return `rtmp://${ip}:${state.listen_port}/${state.rtmp_app || 'live'}/${state.rtmp_key || 'stream'}`;
        }
        return '';
    }

    function render() {
        if (!EL || !state) return;
        const listen = state.mode === 'srt_listener' || state.mode === 'rtmp_server';
        const conn = state.conn_state || 'waiting';
        const scan = state.scan === 'i' ? 'i' : 'p';
        const fmt = `${state.width}×${state.height}${scan}${state.fps_cfg} · ${state.chroma} · ${state.bit_depth} bit`;
        const errs = (state.last_errors || []).slice(-6);
        EL.innerHTML = `
        <div class="si-wrap">
          <div class="si-head">
            <span class="si-badge">Stream In</span>
            <span class="si-state ${esc(conn)}">${esc(t('plugin.stream_in.state_' + conn))}</span>
            <span class="si-meta">${esc(t(MODES[state.mode] || ''))}</span>
            <span class="si-spacer"></span>
            <button class="btn btn-secondary si-reconnect">${esc(t('plugin.stream_in.reconnect'))}</button>
          </div>
          <div class="si-card">
            <div class="si-card-title">${esc(t(listen ? 'plugin.stream_in.ingest_url' : 'plugin.stream_in.remote_url'))}</div>
            <div class="si-url-row">
              <code class="si-url">${esc(listen ? ingestUrl() : (state.url || '—'))}</code>
              ${listen ? `<button class="btn btn-secondary si-copy">${esc(t('plugin.stream_in.copy'))}</button>` : ''}
            </div>
            <div class="si-hint">${esc(t(listen ? 'plugin.stream_in.ingest_hint' : 'plugin.stream_in.remote_hint'))}</div>
          </div>
          <div class="si-card">
            <div class="si-card-title">${esc(t('plugin.stream_in.output_format'))} — ${esc(fmt)}</div>
            <div class="si-stats">
              <div class="si-stat"><b class="si-fps">${esc(state.fps)}</b> <span>${esc(t('plugin.stream_in.fps_out'))}</span></div>
              <div class="si-stat"><b class="si-infps">${esc(state.in_fps)}</b> <span>${esc(t('plugin.stream_in.fps_in'))}</span></div>
              <div class="si-stat"><b class="si-age">${state.last_frame_age_ms == null ? '—' : esc(state.last_frame_age_ms) + ' ms'}</b> <span>${esc(t('plugin.stream_in.frame_age'))}</span></div>
              <div class="si-stat"><b class="si-restarts">${esc(state.ffmpeg_restarts)}</b> <span>${esc(t('plugin.stream_in.restarts'))}</span></div>
            </div>
          </div>
          ${errs.length ? `<div class="si-card"><div class="si-card-title">${esc(t('plugin.stream_in.last_errors'))}</div>
            <div class="si-errors">${errs.map(e => `<div class="si-error">${esc(e)}</div>`).join('')}</div></div>` : ''}
        </div>`;
        const cp = $('.si-copy');
        if (cp) cp.addEventListener('click', () => {
            navigator.clipboard.writeText(ingestUrl()).then(
                () => TOAST(t('plugin.stream_in.copied'), 'success'),
                () => TOAST(t('plugin.stream_in.copy_failed'), 'error'));
        });
        $('.si-reconnect').addEventListener('click', async () => {
            try {
                const r = await api('/reconnect', {});
                if (!r.ok) throw new Error('HTTP ' + r.status);
                TOAST(t('plugin.stream_in.reconnecting'), 'success');
            } catch (e) { TOAST('Reconnect : ' + e.message, 'error'); }
        });
    }

    function updateLive() {
        if (!EL || !state) return;
        const st = $('.si-state');
        const conn = state.conn_state || 'waiting';
        if (!st || !st.classList.contains(conn)) { render(); return; }
        const set = (sel, v) => { const n = $(sel); if (n) n.textContent = v; };
        set('.si-fps', state.fps);
        set('.si-infps', state.in_fps);
        set('.si-age', state.last_frame_age_ms == null ? '—' : state.last_frame_age_ms + ' ms');
        set('.si-restarts', state.ffmpeg_restarts);
    }

    async function refreshState({ fullRebuild = false } = {}) {
        if (VMID == null || !EL) return;
        try {
            const r = await api('/state');
            if (!r.ok) throw new Error('HTTP ' + r.status);
            state = await r.json();
            if (fullRebuild || !$('.si-wrap')) render();
            else updateLive();
        } catch (e) {
            EL.innerHTML = `<div class="meta">${esc(t('plugin.stream_in.unreachable'))} (${esc(e.message)})</div>`;
        }
    }

    function mount(el, vmid, ctx) {
        if (pollTimer) { clearInterval(pollTimer); pollTimer = null; }
        EL = el; VMID = vmid; TOAST = (ctx && ctx.toast) || (() => {});
        state = null; dockerIp = null;
        fetchDockerIp().then(() => refreshState({ fullRebuild: true }));
        pollTimer = setInterval(() => refreshState({ fullRebuild: false }), 1000);
    }
    function unmount() {
        if (pollTimer) { clearInterval(pollTimer); pollTimer = null; }
        EL = null; VMID = null; state = null; dockerIp = null;
    }

    return { mount, unmount };
})();
