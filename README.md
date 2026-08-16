# Core2 relay

A rendezvous point so the Core2 and its host can reach each other from
different networks. Both sit behind NAT; both connect *outward* to this.

**It does nothing clever on purpose.** No speech recognition, no synthesis, no
storage. Audio lives in memory only until the other side collects it, then is
dropped (120 s TTL, 8 items per queue). Recognition and synthesis stay on the
user's own machine, so the privacy property survives — the relay only ever sees
TLS traffic it forwards.

That also keeps it small enough for the cheapest instance available.

## Deploy

Set `RELAY_TOKEN` to a long random string. Every route requires it as `?token=`.
Without it the service refuses all requests rather than running open.

## Latency

One extra hop each way. From Munich to an EU region that is roughly 20–30 ms,
against a voice pipeline that already takes ~1.5 s — imperceptible.

## Endpoints

| | |
|---|---|
| `POST /host/say?mood=` | host → device, raw PCM body |
| `POST /host/mood?m=` | host → device, expression only |
| `GET /device/commands` | device long-polls for the next command |
| `GET /device/audio/{id}` | device fetches a command's PCM, single delivery |
| `POST /device/event` | device → host, a recording |
| `GET /host/events` | host long-polls for recordings |
| `GET /health` | queue depths and counters |
