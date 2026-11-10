# Stream In — Bobi.Studio's SRT/RTMP network source

*[Version française](README.md)*

Brings an SRT or RTMP stream onto the MXL bus like any other source. Submodule of
[bobi.studio](https://github.com/bob-integration/bobistudio), mounted at `plugins/stream_in/`.

## What it does

Receives a network stream and publishes it on the bus: a video output (`<host>`) and an 8-channel
audio output (`<host>_audio`).

Four modes. In **listener** (SRT) or **server** (RTMP) mode, the source pushes to the address shown
by the plugin. In **caller** (SRT) or **pull** (RTMP) mode, the plugin fetches the stream. The
address shown is the container's own, reachable directly, without port forwarding.

In SRT, the plugin handles the passphrase and the Stream ID.

## Entry point

Port, RTMP application and key, remote URL: everything is edited in the panel. Applying restarts
the connection without redeploying and without cutting the output. The last eight addresses stay
one click away.

The ingest switch stops reception without stopping the container: the output keeps sending the
test pattern at the configured cadence.

## Connection log

State transitions, restarts, errors, source changes and on/off switches are timestamped in a
persistent log, browsable by time range and exportable as CSV. Timestamps come from the
container's clock, disciplined by PTP.

## Output format

The incoming stream is always converted to the configured format — resolution, frame rate,
chroma, bit depth, scan —, with automatic deinterlacing of an interlaced input. The panel shows
the source's actual format and the output format side by side.

Genlock, on by default, locks the output to the PTP grid. When off, the output follows the
stream's arrival rhythm.

## States

**Waiting**: no picture received, the plugin sends the test pattern or black. **Connected**: fresh
pictures. **Frozen**: no picture since the configured threshold, the last one is repeated.
Recovery is automatic; in caller or pull mode, a dead connection is dropped and retried.

Without an audio track in the stream, the plugin writes silence on its audio output.

## A/V sync

An A/V lock places audio on the grid slot of the frame it belongs to. It is **off by default**:
measured on 2026-08-23 on a real SRT link, it gives +275 ms of A/V offset against +38 ms without
it. Two defects have been identified and are being fixed.

## Audio diagnostics

`:8080` publishes the health of the audio path. `audio_latence_ms` is the audio latency;
`audio_buf_ms` is peak occupancy, which measures the size of write bursts (commonly 300 to
400 ms), not latency — do not use it for sizing. Losses are counted by cause
(`audio_drop_plafond`, `_drainage`, `_reancrage`, `_generation`). In steady state, these counters
stay still.

## Slice mode

The video output can be published band by band, removing one frame of latency downstream. An
interlaced source, or a band height that does not divide the frame, falls back to whole frames.
Off by default.

## Licence

GNU GPL v3 or later — see `LICENSE`.
