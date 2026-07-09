// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 BOBI SAS, France
// Auteur : Cyril Mazouer, pour le compte de BOBI SAS
// Distribué sous licence GNU GPL v3 (ou ultérieure) ; voir le fichier LICENSE.

// UI de contrôle embarquée du plugin Stream In (montée dans le shell Sources).
// Contrat : window.MXLPlugins.stream_in.mount(el, vmid, ctx) / unmount().
window.MXLPlugins = window.MXLPlugins || {};
window.MXLPlugins.stream_in = (function () {
    'use strict';

    const TYPE = 'stream_in';
    const HIST_MAX = 8;                 // sources récentes conservées

    let EL = null, VMID = null, TOAST = () => {};
    let state = null, dockerIp = null;
    let pollTimer = null;
    let recent = [], recentId = null;   // entrée plugin_store {id, value:[...]}
    let editing = false;                // saisie en cours : le poll ne réécrit pas les champs
    let built = false;

    const t = k => (window.t ? window.t(k) : k);
    const esc = s => String(s == null ? '' : s).replace(/[&<>"']/g, c => ({
        '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
    }[c]));
    const $ = sel => (EL ? EL.querySelector(sel) : null);
    const isListen = m => m === 'srt_listener' || m === 'rtmp_server';

    const MODE_KEY = {
        srt_listener: 'plugin.stream_in.mode_srt_listener',
        srt_caller:   'plugin.stream_in.mode_srt_caller',
        rtmp_server:  'plugin.stream_in.mode_rtmp_server',
        rtmp_pull:    'plugin.stream_in.mode_rtmp_pull',
    };

    // ─── API ────────────────────────────────────────────────────────────
    const plug = (path, body) => fetch(`/api/containers/${VMID}/plugin${path}`, {
        method: body === undefined ? 'GET' : 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: body === undefined ? undefined : JSON.stringify(body || {}),
    });

    // Passe par l'orchestrateur : persiste en DB (survit au redeploy) puis pousse à chaud.
    const control = (action, body) => fetch(`/api/containers/${VMID}/control/${action}`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body || {}),
    });

    async function fetchDockerIp() {
        try {
            const r = await fetch('/api/containers');
            if (!r.ok) return;
            const rows = await r.json();
            const me = (Array.isArray(rows) ? rows : []).find(c => String(c.vmid) === String(VMID));
            if (me && me.docker_ip) dockerIp = me.docker_ip;
        } catch (e) { /* l'adresse affichera <ip> tant qu'inconnue */ }
    }

    // ─── Sources récentes (plugin_store, scope = vmid) ──────────────────
    async function loadRecent() {
        try {
            const r = await fetch(`/api/plugins/${TYPE}/store?scope=${VMID}`);
            if (!r.ok) return;
            const rows = await r.json();
            const e = (rows || []).find(x => x.name === 'recent');
            if (e) { recentId = e.id; recent = Array.isArray(e.value) ? e.value : []; }
        } catch (e) { /* historique indisponible : non bloquant */ }
    }

    async function pushRecent(entry) {
        recent = [entry, ...recent.filter(x => x.addr !== entry.addr)].slice(0, HIST_MAX);
        await saveRecent();
    }

    async function saveRecent() {
        try {
            if (recentId == null) {
                const r = await fetch(`/api/plugins/${TYPE}/store`, {
                    method: 'POST', headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ scope: String(VMID), name: 'recent', value: recent }),
                });
                if (r.ok) recentId = (await r.json()).id;
            } else {
                await fetch(`/api/plugins/${TYPE}/store/${recentId}`, {
                    method: 'PATCH', headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ value: recent }),
                });
            }
        } catch (e) { TOAST(t('plugin.stream_in.recent_save_failed'), 'error'); }
    }

    // ─── Adresse ────────────────────────────────────────────────────────
    // Une seule vérité : ce qui est affiché est ce qui se règle. Les modes écoute exposent
    // l'adresse que la source doit viser (IP macvlan du conteneur, jointe directement sur
    // le LAN) ; les modes appel/pull exposent l'URL distante.
    function currentAddr(s) {
        if (!s) return '';
        if (s.mode === 'srt_listener') return `srt://${dockerIp || '<ip>'}:${s.listen_port}`;
        if (s.mode === 'rtmp_server') {
            return `rtmp://${dockerIp || '<ip>'}:${s.listen_port}/${s.rtmp_app || 'live'}/${s.rtmp_key || 'stream'}`;
        }
        return s.url || '';
    }

    function addrHtml(s) {
        if (s.mode === 'srt_listener') {
            return `<span class="si-addr-fix">srt://${esc(dockerIp || '<ip>')}:</span>`
                 + `<input class="si-port" data-src="listen_port" type="text" inputmode="numeric"
                       value="${esc(s.listen_port)}" aria-label="${esc(t('plugin.stream_in.port_aria'))}">`;
        }
        if (s.mode === 'rtmp_server') {
            return `<span class="si-addr-fix">rtmp://${esc(dockerIp || '<ip>')}:</span>`
                 + `<input class="si-port" data-src="listen_port" type="text" inputmode="numeric"
                       value="${esc(s.listen_port)}" aria-label="${esc(t('plugin.stream_in.port_aria'))}">`
                 + `<span class="si-addr-fix">/</span>`
                 + `<input class="si-seg" data-src="rtmp_app" type="text" value="${esc(s.rtmp_app || 'live')}"
                       aria-label="${esc(t('plugin.stream_in.rtmp_app_aria'))}">`
                 + `<span class="si-addr-fix">/</span>`
                 + `<input class="si-seg" data-src="rtmp_key" type="text" value="${esc(s.rtmp_key || '')}"
                       placeholder="stream" aria-label="${esc(t('plugin.stream_in.rtmp_key_aria'))}">`;
        }
        const ph = s.mode === 'srt_caller' ? 'srt://hôte:port' : 'rtmp://hôte/app/clé';
        return `<input class="si-url" data-src="url" type="text" value="${esc(s.url || '')}"
                   placeholder="${ph}" aria-label="${esc(t('plugin.stream_in.url_aria'))}">`;
    }

    function readAddrFields() {
        const out = {};
        EL.querySelectorAll('.si-addr [data-src]').forEach(el => {
            const k = el.dataset.src;
            out[k] = (k === 'listen_port') ? (parseInt(el.value, 10) || 0) : el.value.trim();
        });
        return out;
    }

    async function applyAddr() {
        const body = readAddrFields();
        if ('listen_port' in body && (body.listen_port < 1024 || body.listen_port > 65535)) {
            TOAST(t('plugin.stream_in.port_invalid'), 'error'); return;
        }
        if ('url' in body && !body.url) { TOAST(t('plugin.stream_in.url_required'), 'error'); return; }
        const btn = $('.si-apply');
        if (btn) { btn.disabled = true; btn.textContent = t('plugin.stream_in.applying'); }
        try {
            const r = await control('source', body);
            if (!r.ok) throw new Error('HTTP ' + r.status);
            const res = await r.json();
            TOAST(res.status === 'bascule_a_chaud'
                ? t('plugin.stream_in.applied_hot') : t('plugin.stream_in.applied_redeploy'), 'success');
            editing = false;
            await pushRecent({ addr: currentAddr(Object.assign({}, state, body)), mode: state.mode, src: body });
            await refresh({ rebuild: true });
        } catch (e) {
            TOAST(t('plugin.stream_in.apply_failed') + ' : ' + e.message, 'error');
            if (btn) { btn.disabled = false; btn.textContent = t('plugin.stream_in.apply'); }
        }
    }

    async function togglePower(on) {
        try {
            const r = await control('power', { enabled: on });
            if (!r.ok) throw new Error('HTTP ' + r.status);
            TOAST(on ? t('plugin.stream_in.started') : t('plugin.stream_in.stopped'), 'success');
        } catch (e) {
            TOAST(t('plugin.stream_in.power_failed') + ' : ' + e.message, 'error');
        }
        refresh({ rebuild: true });
    }

    // ─── Courbe de débit ────────────────────────────────────────────────
    // Une seule série (débit entrant) : pas de légende de série, un libellé direct suffit.
    // Les événements discrets (gel, silence) sont des marques de statut, doublées d'un
    // libellé en légende : jamais identifiables par la couleur seule.
    const W = 480, H = 72, PLOT_H = 48, EV_Y = 54;

    function fmtRate(kbps) {
        if (kbps == null) return '—';
        return kbps >= 1000 ? (kbps / 1000).toFixed(1) + ' Mb/s' : Math.round(kbps) + ' kb/s';
    }

    function chartHtml(hist) {
        const n = hist.length;
        if (!n) {
            return `<p class="si-empty">${esc(t('plugin.stream_in.chart_empty'))}</p>`;
        }
        const peak = Math.max(1, ...hist.map(p => p.kbps));
        const top = peak * 1.15;
        const x = i => (n === 1 ? W : (i / (n - 1)) * W);
        const y = v => PLOT_H - (v / top) * (PLOT_H - 4) - 2;

        const pts = hist.map((p, i) => `${x(i).toFixed(1)},${y(p.kbps).toFixed(1)}`);
        const line = `M${pts.join(' L')}`;
        const area = `M${x(0).toFixed(1)},${PLOT_H} L${pts.join(' L')} L${x(n - 1).toFixed(1)},${PLOT_H} Z`;

        const marks = hist.map((p, i) => {
            const px = x(i).toFixed(1);
            if (p.off)     return `<rect class="si-ev-off" x="${px}" y="${EV_Y}" width="2" height="12" rx="1"/>`;
            let out = '';
            if (p.freeze)  out += `<rect class="si-ev-freeze" x="${px}" y="${EV_Y}" width="2" height="5" rx="1"/>`;
            if (p.silence) out += `<rect class="si-ev-silence" x="${px}" y="${EV_Y + 7}" width="2" height="5" rx="1"/>`;
            return out;
        }).join('');

        return `
        <div class="si-chart-head">
          <span class="si-h">${esc(t('plugin.stream_in.chart_title'))}</span>
          <span class="si-chart-peak">${esc(t('plugin.stream_in.chart_peak'))} ${esc(fmtRate(peak))}</span>
        </div>
        <svg class="si-svg" viewBox="0 0 ${W} ${H}" preserveAspectRatio="none"
             role="img" aria-label="${esc(t('plugin.stream_in.chart_aria'))}">
          <path class="si-area" d="${area}"/>
          <path class="si-line" d="${line}" vector-effect="non-scaling-stroke"/>
          <line class="si-base" x1="0" y1="${PLOT_H}" x2="${W}" y2="${PLOT_H}"
                vector-effect="non-scaling-stroke"/>
          ${marks}
          <line class="si-cross" x1="0" y1="0" x2="0" y2="${PLOT_H}" hidden
                vector-effect="non-scaling-stroke"/>
          <circle class="si-dot" r="3.5" hidden/>
          <rect class="si-hit" x="0" y="0" width="${W}" height="${H}" fill="transparent"/>
        </svg>
        <div class="si-legend">
          <span><i class="si-swatch rate"></i>${esc(t('plugin.stream_in.legend_rate'))}</span>
          <span><i class="si-swatch freeze"></i>${esc(t('plugin.stream_in.legend_freeze'))}</span>
          <span><i class="si-swatch silence"></i>${esc(t('plugin.stream_in.legend_silence'))}</span>
        </div>`;
    }

    // Crosshair + infobulle : une courbe HTML est interactive par défaut.
    function bindChart() {
        const svg = $('.si-svg');
        if (!svg || !state || !state.history || !state.history.length) return;
        const hist = state.history, n = hist.length;
        const cross = svg.querySelector('.si-cross');
        const dot = svg.querySelector('.si-dot');
        const fig = $('.si-chart');
        let tip = null;

        const peak = Math.max(1, ...hist.map(p => p.kbps));
        const top = peak * 1.15;

        const hide = () => {
            cross.setAttribute('hidden', ''); dot.setAttribute('hidden', '');
            if (tip) { tip.remove(); tip = null; }
        };

        svg.addEventListener('pointerleave', hide);
        svg.addEventListener('pointermove', ev => {
            const box = svg.getBoundingClientRect();
            const frac = Math.min(1, Math.max(0, (ev.clientX - box.left) / box.width));
            const i = Math.round(frac * (n - 1));
            const p = hist[i];
            const vx = (n === 1 ? W : (i / (n - 1)) * W);
            const vy = PLOT_H - (p.kbps / top) * (PLOT_H - 4) - 2;

            cross.removeAttribute('hidden'); dot.removeAttribute('hidden');
            cross.setAttribute('x1', vx); cross.setAttribute('x2', vx);
            dot.setAttribute('cx', vx); dot.setAttribute('cy', vy);

            if (!tip) { tip = document.createElement('div'); tip.className = 'si-tip'; fig.appendChild(tip); }
            const ago = n - 1 - i;
            let evs = '';
            if (p.off)     evs += `<span class="ev">${esc(t('plugin.stream_in.legend_off'))}</span>`;
            if (p.freeze)  evs += `<span class="ev freeze">${esc(t('plugin.stream_in.legend_freeze'))}</span>`;
            if (p.silence) evs += `<span class="ev silence">${esc(t('plugin.stream_in.legend_silence'))}</span>`;
            tip.innerHTML = `<b>${esc(fmtRate(p.kbps))}</b><br>`
                + `<span class="si-hint">${ago === 0 ? esc(t('plugin.stream_in.now'))
                    : esc(t('plugin.stream_in.ago').replace('{s}', ago))}</span>${evs}`;

            const px = (ev.clientX - box.left);
            tip.style.left = Math.min(box.width - tip.offsetWidth - 4, Math.max(0, px - tip.offsetWidth / 2)) + 'px';
            tip.style.top = '-6px';
            tip.style.transform = 'translateY(-100%)';
        });
    }

    // ─── Journal de connexion ───────────────────────────────────────────
    // L'anneau de 2 min diagnostique à chaud ; le journal justifie une coupure a posteriori.
    // Les horodatages viennent du conteneur (horloge disciplinée PTP), donc comparables avec
    // les autres équipements de la chaîne.
    const EVENT_KEY = {
        state: 'plugin.stream_in.ev_state',
        ffmpeg_restart: 'plugin.stream_in.ev_restart',
        ffmpeg_error: 'plugin.stream_in.ev_error',
        source_change: 'plugin.stream_in.ev_source',
        power: 'plugin.stream_in.ev_power',
        script_start: 'plugin.stream_in.ev_start',
        journal_cleared: 'plugin.stream_in.ev_cleared',
    };

    const pad = n => String(n).padStart(2, '0');
    const toLocalInput = d => `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}`
        + `T${pad(d.getHours())}:${pad(d.getMinutes())}`;
    const stampIso = ts => new Date(ts * 1000).toISOString();
    const stampLocal = ts => new Date(ts * 1000).toLocaleString();

    function eventDetail(e) {
        if (e.event === 'state') {
            const f = e.src_fmt ? ` ${e.src_fmt.width}×${e.src_fmt.height} ${e.src_fmt.codec}` : '';
            return `${t('plugin.stream_in.state_' + e.state)}${f}`;
        }
        if (e.event === 'ffmpeg_restart') return `#${e.n}` + (e.last_error ? ` : ${e.last_error}` : '');
        if (e.event === 'ffmpeg_error') return e.message || '';
        if (e.event === 'source_change') return e.source || '';
        if (e.event === 'power') return e.enabled ? t('plugin.stream_in.ingest_on') : t('plugin.stream_in.ingest_off');
        if (e.event === 'script_start') return `${e.version} ${e.out_format || ''}`;
        return '';
    }

    function journalRange() {
        const f = $('.si-jrn-from'), tt = $('.si-jrn-to');
        const from = f && f.value ? new Date(f.value).getTime() / 1000 : null;
        const to = tt && tt.value ? new Date(tt.value).getTime() / 1000 : null;
        return { from, to };
    }

    async function fetchEvents() {
        const { from, to } = journalRange();
        const qs = new URLSearchParams();
        if (from) qs.set('from', from);
        if (to) qs.set('to', to);
        const r = await plug(`/events?${qs.toString()}`);
        if (!r.ok) throw new Error('HTTP ' + r.status);
        return (await r.json()).events || [];
    }

    async function loadJournal() {
        const out = $('.si-jrn-out');
        out.innerHTML = `<p class="si-empty">${esc(t('plugin.stream_in.jrn_loading'))}</p>`;
        try {
            const evs = await fetchEvents();
            if (!evs.length) {
                out.innerHTML = `<p class="si-empty">${esc(t('plugin.stream_in.jrn_none'))}</p>`;
                $('.si-jrn-export').disabled = true;
                return;
            }
            $('.si-jrn-export').disabled = false;
            const shown = evs.slice(-40).reverse();
            out.innerHTML = `<p class="si-hint">${esc(t('plugin.stream_in.jrn_count')
                    .replace('{n}', evs.length).replace('{k}', shown.length))}</p>`
                + `<table class="si-table"><thead><tr>
                     <th>${esc(t('plugin.stream_in.jrn_when'))}</th>
                     <th>${esc(t('plugin.stream_in.jrn_what'))}</th>
                     <th>${esc(t('plugin.stream_in.jrn_detail'))}</th></tr></thead><tbody>`
                + shown.map(e => `<tr><td>${esc(stampLocal(e.ts))}</td>`
                    + `<td>${esc(t(EVENT_KEY[e.event] || e.event))}</td>`
                    + `<td>${esc(eventDetail(e))}</td></tr>`).join('')
                + '</tbody></table>';
        } catch (e) {
            out.innerHTML = `<p class="si-empty">${esc(t('plugin.stream_in.jrn_failed'))} (${esc(e.message)})</p>`;
        }
    }

    async function exportJournal() {
        try {
            const evs = await fetchEvents();
            if (!evs.length) { TOAST(t('plugin.stream_in.jrn_none'), 'error'); return; }
            const csvCell = v => {
                const s = String(v == null ? '' : v);
                return /[",;\n]/.test(s) ? '"' + s.replace(/"/g, '""') + '"' : s;
            };
            const rows = [['horodatage_iso', 'horodatage_local', 'evenement', 'detail', 'source', 'brut']];
            evs.forEach(e => rows.push([
                stampIso(e.ts), stampLocal(e.ts),
                t(EVENT_KEY[e.event] || e.event), eventDetail(e),
                e.source || '', JSON.stringify(e),
            ].map(csvCell)));
            // BOM : Excel ouvre le CSV en UTF-8 sans mutiler les accents.
            const blob = new Blob(['﻿' + rows.map(r => r.join(';')).join('\r\n')],
                { type: 'text/csv;charset=utf-8' });
            const { from, to } = journalRange();
            const tag = d => (d ? new Date(d * 1000).toISOString().slice(0, 16).replace(/[:T]/g, '') : 'debut');
            const a = document.createElement('a');
            a.href = URL.createObjectURL(blob);
            a.download = `stream_in-${state.hostname || VMID}-${tag(from)}-${tag(to)}.csv`;
            document.body.appendChild(a); a.click();
            setTimeout(() => { URL.revokeObjectURL(a.href); a.remove(); }, 0);
            TOAST(t('plugin.stream_in.jrn_exported').replace('{n}', evs.length), 'success');
        } catch (e) {
            TOAST(t('plugin.stream_in.jrn_failed') + ' : ' + e.message, 'error');
        }
    }

    // Effacement : le journal est une pièce justificative, on confirme et on trace l'effacement.
    async function clearJournal() {
        if (!window.confirm(t('plugin.stream_in.jrn_clear_confirm'))) return;
        try {
            const r = await plug('/events/clear', {});
            if (!r.ok) throw new Error('HTTP ' + r.status);
            TOAST(t('plugin.stream_in.jrn_cleared'), 'success');
            $('.si-jrn-export').disabled = true;
            await loadJournal();
        } catch (e) { TOAST(t('plugin.stream_in.jrn_clear_failed') + ' : ' + e.message, 'error'); }
    }

    function journalHtml() {
        const now = new Date();
        const hourAgo = new Date(now.getTime() - 3600e3);
        return `
        <details class="si-fold si-jrn">
          <summary>${esc(t('plugin.stream_in.journal'))}</summary>
          <div class="si-jrn-body">
            <div class="si-jrn-range">
              <label>${esc(t('plugin.stream_in.jrn_from'))}
                <input type="datetime-local" class="si-jrn-from" value="${toLocalInput(hourAgo)}"></label>
              <label>${esc(t('plugin.stream_in.jrn_to'))}
                <input type="datetime-local" class="si-jrn-to" value="${toLocalInput(now)}"></label>
              <button class="si-btn si-jrn-load">${esc(t('plugin.stream_in.jrn_load'))}</button>
              <button class="si-btn primary si-jrn-export" disabled>${esc(t('plugin.stream_in.jrn_export'))}</button>
              <span class="si-grow"></span>
              <button class="si-btn si-jrn-clear danger">${esc(t('plugin.stream_in.jrn_clear'))}</button>
            </div>
            <p class="si-hint">${esc(t('plugin.stream_in.jrn_hint'))}</p>
            ${state && state.journal_persistent === false
                ? `<p class="si-warn">⚠ ${esc(t('plugin.stream_in.jrn_volatile'))}</p>` : ''}
            <div class="si-jrn-out"></div>
          </div>
        </details>`;
    }

    // ─── Rendu ──────────────────────────────────────────────────────────
    function srcFormatText(s) {
        const v = s.source_format && s.source_format.video;
        if (!v) return `<span class="in none">${esc(t('plugin.stream_in.src_unknown'))}</span>`;
        const a = s.source_format && s.source_format.audio;
        const au = a ? ` ${esc(a.codec)}` : ` ${esc(t('plugin.stream_in.no_audio'))}`;
        return `<span class="in">${v.width}×${v.height} ${Math.round(v.fps)}p ${esc(v.codec)}${au}</span>`;
    }

    function outFormatText(s) {
        const scan = s.scan === 'i' ? 'i' : 'p';
        return `<span class="out">${s.width}×${s.height} ${scan}${s.fps_cfg} ${esc(s.chroma)} ${s.bit_depth} bit</span>`;
    }

    function statsHtml(s) {
        const audioBad = s.audio_starve_blocks > 0 || s.audio_drop_samples > 0 || s.audio_reanchors > 0;
        const cell = (v, lbl, warn) =>
            `<div class="si-stat${warn ? ' warn' : ''}"><b>${esc(v)}</b><small>${esc(lbl)}</small></div>`;
        return `
        <div class="si-stats">
          ${cell(fmtRate(s.net_in_kbps), t('plugin.stream_in.net_in'))}
          ${cell(s.in_fps, t('plugin.stream_in.fps_in'))}
          ${cell(s.fps, t('plugin.stream_in.fps_out'))}
          ${cell(s.last_frame_age_ms == null ? '—' : s.last_frame_age_ms + ' ms', t('plugin.stream_in.frame_age'))}
          ${cell(s.audio_buf_ms + ' ms', t('plugin.stream_in.audio_latency'))}
          ${cell(s.ffmpeg_restarts, t('plugin.stream_in.restarts'), s.ffmpeg_restarts > 0)}
        </div>
        <details class="si-fold">
          <summary>${esc(t('plugin.stream_in.audio_health'))}${audioBad ? ' ⚠' : ''}</summary>
          <table class="si-table">
            <tbody>
              <tr><th>${esc(t('plugin.stream_in.audio_in_sps'))}</th><td>${esc(s.audio_in_sps)}</td></tr>
              <tr><th>${esc(t('plugin.stream_in.audio_starve'))}</th><td>${esc(s.audio_starve_blocks)}</td></tr>
              <tr><th>${esc(t('plugin.stream_in.audio_drop'))}</th><td>${esc(s.audio_drop_samples)}</td></tr>
              <tr><th>${esc(t('plugin.stream_in.audio_reanchor'))}</th><td>${esc(s.audio_reanchors)}</td></tr>
            </tbody>
          </table>
        </details>`;
    }

    function recentHtml(s) {
        const cur = currentAddr(s);
        if (!recent.length) {
            return `<div class="si-recent"><span class="si-recent-lbl">${esc(t('plugin.stream_in.recent'))}</span>`
                 + `<span class="si-empty">${esc(t('plugin.stream_in.recent_empty'))}</span></div>`;
        }
        return `<div class="si-recent"><span class="si-recent-lbl">${esc(t('plugin.stream_in.recent'))}</span>`
            + recent.map((r, i) => {
                const isCur = r.addr === cur;
                return `<span class="si-chip${isCur ? ' current' : ''}">`
                    + (isCur ? esc(r.addr)
                             : `<button class="si-chip-use" data-i="${i}" title="${esc(t('plugin.stream_in.recent_use'))}">${esc(r.addr)}</button>`)
                    + `<button class="si-chip-x" data-del="${i}" aria-label="${esc(t('plugin.stream_in.recent_forget'))}">×</button></span>`;
            }).join('') + '</div>';
    }

    function render() {
        if (!EL || !state) return;
        const s = state;
        const conn = s.conn_state || 'waiting';
        const errs = (s.last_errors || []).slice(-6);

        EL.innerHTML = `
        <div class="si">
          <div class="si-bar">
            <i class="si-led ${esc(conn)}"></i>
            <span class="si-state">${esc(t('plugin.stream_in.state_' + conn))}</span>
            <span class="si-mode">${esc(t(MODE_KEY[s.mode] || ''))}</span>
            <span class="si-grow"></span>
            <label class="si-power">
              <input type="checkbox" class="ios-toggle si-power-cb" ${s.enabled ? 'checked' : ''}>
              <span>${esc(t('plugin.stream_in.ingest'))}</span>
            </label>
            <button class="si-btn si-reconnect">${esc(t('plugin.stream_in.reconnect'))}</button>
          </div>

          <section class="si-sec">
            <h3 class="si-h">${esc(t('plugin.stream_in.entry_point'))}</h3>
            <div class="si-addr">
              ${addrHtml(s)}
              <span class="si-addr-actions">
                ${isListen(s.mode) ? `<button class="si-btn si-copy">${esc(t('plugin.stream_in.copy'))}</button>` : ''}
                <button class="si-btn primary si-apply" disabled>${esc(t('plugin.stream_in.apply'))}</button>
              </span>
            </div>
            <p class="si-hint">${esc(t(isListen(s.mode)
                ? 'plugin.stream_in.ingest_hint' : 'plugin.stream_in.remote_hint'))}</p>
            ${recentHtml(s)}
          </section>

          <section class="si-sec">
            <h3 class="si-h">${esc(t('plugin.stream_in.signal'))}</h3>
            <div class="si-flow">
              ${srcFormatText(s)}
              <span class="arrow">→</span>
              ${outFormatText(s)}
            </div>
            ${statsHtml(s)}
          </section>

          <figure class="si-chart">${chartHtml(s.history || [])}</figure>

          ${journalHtml()}

          ${errs.length ? `<details class="si-fold" open>
            <summary>${esc(t('plugin.stream_in.last_errors'))}</summary>
            <div class="si-errors">${errs.map(e => `<div>${esc(e)}</div>`).join('')}</div>
          </details>` : ''}
        </div>`;

        bindEvents();
        bindChart();
        built = true;
    }

    function bindEvents() {
        const cp = $('.si-copy');
        if (cp) cp.addEventListener('click', () => {
            navigator.clipboard.writeText(currentAddr(state)).then(
                () => TOAST(t('plugin.stream_in.copied'), 'success'),
                () => TOAST(t('plugin.stream_in.copy_failed'), 'error'));
        });

        $('.si-reconnect').addEventListener('click', async () => {
            try {
                const r = await plug('/reconnect', {});
                if (!r.ok) throw new Error('HTTP ' + r.status);
                TOAST(t('plugin.stream_in.reconnecting'), 'success');
            } catch (e) { TOAST(t('plugin.stream_in.reconnect_failed') + ' : ' + e.message, 'error'); }
        });

        $('.si-power-cb').addEventListener('change', e => togglePower(e.target.checked));

        const apply = $('.si-apply');
        EL.querySelectorAll('.si-addr [data-src]').forEach(inp => {
            inp.addEventListener('focus', () => { editing = true; });
            inp.addEventListener('input', () => { editing = true; apply.disabled = false; });
            inp.addEventListener('keydown', ev => {
                if (ev.key === 'Enter') { ev.preventDefault(); applyAddr(); }
                if (ev.key === 'Escape') { editing = false; refresh({ rebuild: true }); }
            });
        });
        apply.addEventListener('click', applyAddr);

        EL.querySelectorAll('.si-chip-use').forEach(b => b.addEventListener('click', () => {
            const r = recent[parseInt(b.dataset.i, 10)];
            if (!r || !r.src) return;
            Object.entries(r.src).forEach(([k, v]) => {
                const el = EL.querySelector(`.si-addr [data-src="${k}"]`);
                if (el) el.value = v;
            });
            $('.si-apply').disabled = false;
            editing = true;
        }));
        EL.querySelectorAll('.si-chip-x').forEach(b => b.addEventListener('click', async () => {
            recent.splice(parseInt(b.dataset.del, 10), 1);
            await saveRecent();
            render();
        }));

        $('.si-jrn-load').addEventListener('click', loadJournal);
        $('.si-jrn-export').addEventListener('click', exportJournal);
        $('.si-jrn-clear').addEventListener('click', clearJournal);
        // Le poll ne doit pas reconstruire le DOM pendant qu'on consulte le journal.
        $('.si-jrn').addEventListener('toggle', e => { editing = e.target.open; });
    }

    // Rafraîchissement léger : ne reconstruit pas le DOM (garde le focus de saisie et
    // l'infobulle de la courbe). La courbe se redessine à chaque seconde de données neuve.
    function updateLive(prev) {
        const s = state;
        const led = $('.si-led');
        if (!led || !led.classList.contains(s.conn_state)) { render(); return; }
        if (prev && prev.enabled !== s.enabled) { render(); return; }
        const set = (sel, v) => { const n = $(sel); if (n) n.textContent = v; };
        const cells = EL.querySelectorAll('.si-stat b');
        if (cells.length === 6) {
            cells[0].textContent = fmtRate(s.net_in_kbps);
            cells[1].textContent = s.in_fps;
            cells[2].textContent = s.fps;
            cells[3].textContent = s.last_frame_age_ms == null ? '—' : s.last_frame_age_ms + ' ms';
            cells[4].textContent = s.audio_buf_ms + ' ms';
            cells[5].textContent = s.ffmpeg_restarts;
        }
        const fig = $('.si-chart');
        if (fig && (!prev || (prev.history || []).length !== (s.history || []).length)) {
            if (!EL.querySelector('.si-tip')) { fig.innerHTML = chartHtml(s.history || []); bindChart(); }
        }
    }

    async function refresh({ rebuild = false } = {}) {
        if (VMID == null || !EL) return;
        try {
            const r = await plug('/state');
            if (!r.ok) throw new Error('HTTP ' + r.status);
            const prev = state;
            state = await r.json();
            if (rebuild || !built || !$('.si')) render();
            else if (!editing) updateLive(prev);
        } catch (e) {
            built = false;
            EL.innerHTML = `<p class="si-empty">${esc(t('plugin.stream_in.unreachable'))} (${esc(e.message)})</p>`;
        }
    }

    // ─── Cycle de vie ───────────────────────────────────────────────────
    function mount(el, vmid, ctx) {
        if (pollTimer) { clearInterval(pollTimer); pollTimer = null; }
        EL = el; VMID = vmid; TOAST = (ctx && ctx.toast) || (() => {});
        state = null; dockerIp = null; recent = []; recentId = null;
        editing = false; built = false;
        Promise.all([fetchDockerIp(), loadRecent()]).then(() => refresh({ rebuild: true }));
        pollTimer = setInterval(() => refresh(), 1000);
    }

    function unmount() {
        if (pollTimer) { clearInterval(pollTimer); pollTimer = null; }
        EL = null; VMID = null; state = null; recent = []; recentId = null; built = false;
    }

    return { mount, unmount };
})();
