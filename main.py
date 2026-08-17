#!/usr/bin/env python3
"""
Core2 relay — a rendezvous point so the device and its host can reach each
other from different networks.

The device sits behind NAT and so, usually, does the host. Neither can accept
an inbound connection. Both therefore connect *outward* to this service, which
does nothing but hold two queues and pass bytes between them.

Deliberately dumb: no speech recognition, no synthesis, no storage. Audio is
held in memory only until the other side collects it, and is dropped after a
short TTL. That keeps the privacy property intact — recognition and synthesis
still happen on the user's own machine — and keeps this small enough to run on
the cheapest instance there is.

    POST /host/say?mood=      host  -> device   raw PCM body, queued
    POST /host/mood?m=        host  -> device   expression only
    GET  /device/commands     device pulls the next command (long-poll)
    GET  /device/audio/{id}   device fetches a command's PCM payload
    POST /device/event        device -> host    raw PCM recording
    GET  /host/events         host pulls recordings (long-poll)
    GET  /health              liveness, queue depths

Every route requires ?token= matching RELAY_TOKEN.
"""

import asyncio
import os
import time
import uuid
from collections import deque

from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import JSONResponse

RELAY_VERSION = "2026.08.17-2"   # bump when deploying; see README

TOKEN = os.environ.get("RELAY_TOKEN", "")
TTL_SECONDS = 120          # unclaimed items are dropped
MAX_ITEMS = 8              # per queue; a slow consumer must not grow this
MAX_BYTES = 1_500_000      # ~45 s of 16 kHz mono, generous

app = FastAPI(title="Core2 relay")

# to-device commands, and to-host events
commands: deque = deque()
events: deque = deque()
audio: dict[str, tuple[float, bytes]] = {}

stats = {"started": time.time(), "commands": 0, "events": 0}

# Last thing the device told us about itself. It rides along on the poll the
# device already makes, so there is no extra request to fail independently -
# and "is it alive?" stops being something you infer from queue depths.
device: dict = {}

# And the host. The device was never the fragile end - the PC is, and when its
# daemon dies you speak into a microphone nobody is listening to, with nothing
# to tell you. Recorded on the poll the daemon already makes, same as the
# device's, so there is no extra request to fail on its own.
host: dict = {}


def check(token: str | None) -> None:
    if not TOKEN:
        raise HTTPException(500, "RELAY_TOKEN is not set on the server")
    if token != TOKEN:
        raise HTTPException(403, "bad token")


def sweep() -> None:
    """Drop anything nobody collected. Bounded memory is the whole design."""
    now = time.time()
    for q in (commands, events):
        while q and now - q[0]["ts"] > TTL_SECONDS:
            q.popleft()
        while len(q) > MAX_ITEMS:
            q.popleft()
    for k in [k for k, (ts, _) in audio.items() if now - ts > TTL_SECONDS]:
        audio.pop(k, None)


async def wait_for(q: deque, timeout: float):
    """Long-poll: hold the request open rather than making the client spin."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        sweep()
        if q:
            return q.popleft()
        await asyncio.sleep(0.25)
    return None


# ------------------------------------------------------------------ host side
@app.post("/host/say")
async def host_say(request: Request, token: str = "", mood: str = "speaking"):
    check(token)
    pcm = await request.body()
    if not pcm:
        raise HTTPException(400, "empty body")
    if len(pcm) > MAX_BYTES:
        raise HTTPException(413, "audio too large")

    aid = uuid.uuid4().hex[:12]
    audio[aid] = (time.time(), pcm)
    commands.append({"ts": time.time(), "type": "say", "mood": mood,
                     "id": aid, "bytes": len(pcm)})
    stats["commands"] += 1
    sweep()
    return {"ok": True, "id": aid, "queued": len(commands)}


@app.post("/host/mood")
async def host_mood(token: str = "", m: str = "idle"):
    check(token)
    commands.append({"ts": time.time(), "type": "mood", "mood": m})
    stats["commands"] += 1
    sweep()
    return {"ok": True, "queued": len(commands)}


@app.get("/host/events")
async def host_events(token: str = "", wait: float = 25.0,
                      note: str | None = None):
    """Returns the next recording as raw PCM, or 204 when nothing arrives."""
    check(token)
    host.update({"seen": time.time(), "note": note})
    item = await wait_for(events, min(max(wait, 1.0), 60.0))
    if item is None:
        return Response(status_code=204)
    return Response(
        content=item["pcm"],
        media_type="application/octet-stream",
        headers={"X-Seconds": f"{len(item['pcm']) / 2 / 16000:.2f}"},
    )


# ---------------------------------------------------------------- device side
@app.get("/device/commands")
async def device_commands(token: str = "", wait: float = 25.0,
                          bat: int | None = None, rssi: int | None = None,
                          up: int | None = None, heap: int | None = None,
                          fw: str | None = None):
    check(token)
    if bat is not None or rssi is not None:
        device.update({"seen": time.time(), "battery_pct": bat, "rssi": rssi,
                       "uptime_s": up, "free_heap": heap, "fw": fw})
    item = await wait_for(commands, min(max(wait, 1.0), 60.0))
    if item is None:
        return Response(status_code=204)
    return JSONResponse(item)


@app.get("/device/audio/{aid}")
async def device_audio(aid: str, token: str = ""):
    check(token)
    entry = audio.pop(aid, None)          # single delivery, then freed
    if entry is None:
        raise HTTPException(404, "expired or already collected")
    return Response(content=entry[1], media_type="application/octet-stream")


@app.post("/device/event")
async def device_event(request: Request, token: str = ""):
    check(token)
    pcm = await request.body()
    if not pcm:
        raise HTTPException(400, "empty body")
    if len(pcm) > MAX_BYTES:
        raise HTTPException(413, "recording too large")
    events.append({"ts": time.time(), "pcm": pcm})
    stats["events"] += 1
    sweep()
    return {"ok": True, "queued": len(events)}


# --------------------------------------------------------------------- health
@app.get("/health")
async def health(token: str = ""):
    """Liveness, open to anyone. Device telemetry only with the token.

    Battery, signal and firmware version say something about where the owner
    is and what they are carrying, so they are not for passers-by. The queue
    depths carry no such meaning and stay open, because a liveness check that
    needs a secret is a liveness check nobody runs.

    relay_version is here so a deploy can be confirmed without guessing from
    the presence of a key - which is exactly how the last one was diagnosed,
    an absent key and a null value being indistinguishable from outside.
    """
    sweep()
    body = {
        "ok": True,
        "relay_version": RELAY_VERSION,
        "uptime_s": int(time.time() - stats["started"]),
        "commands_waiting": len(commands),
        "events_waiting": len(events),
        "audio_blobs": len(audio),
        "commands_total": stats["commands"],
        "events_total": stats["events"],
        "token_configured": bool(TOKEN),
    }
    if TOKEN and token == TOKEN:
        body["device"] = ({**device,
                           "seen_s_ago": round(time.time() - device["seen"], 1)}
                          if device else None)
        body["host"] = ({**host,
                         "seen_s_ago": round(time.time() - host["seen"], 1)}
                        if host else None)
    return body


@app.get("/")
async def root():
    return {"service": "core2-relay", "see": "/health"}
