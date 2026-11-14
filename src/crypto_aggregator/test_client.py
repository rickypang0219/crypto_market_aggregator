import asyncio
import json
import logging
import os
import socket
import time
from dataclasses import dataclass

import websockets

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("client")

URL = os.environ.get("ADAPTOR_URL", "ws://localhost:8765")
CONFIG_PATH = os.environ.get(
    "SUBS_CONFIG", "subs.json"
)  # on ECS: mounted volume / pulled from S3, not baked in image
CONFIG_INTERVAL = float(os.environ.get("SUBS_CONFIG_INTERVAL", "86400"))  # 1 day
CLIENT_ID = socket.gethostname()


@dataclass(slots=True)
class Heartbeat:
    """One row per stream per second -> DB table `heartbeat`, TTL 1 day.

    Monitor service (NOT this client) checks every minute:
      - heartbeats per client_id < 50       -> client dead or event loop stuck
      - sum(msg_count) per stream == 0      -> stream stale
    """

    ts: int  # client wall clock, ms
    client_id: str
    exchange: str  # exchange_code, e.g. "BINANCE"
    schema: str  # "trade" / "depth1"
    symbol: str
    msg_count: int  # messages since last heartbeat (delta, not cumulative)


def load_subs(path):
    with open(path) as f:
        return {(s["exchange"], s["schema"], s["symbol"]) for s in json.load(f)}


def count_key(sub):
    ex, schema, sym = sub
    return (ex.upper(), schema, sym)  # matches server's exchange_code


subs = load_subs(CONFIG_PATH)  # bad config at boot -> crash loudly
# (exchange_code, schema, symbol) -> count. Pre-seeded so dead streams report 0, not nothing.
counts = {count_key(s): 0 for s in subs}
ws_conn = None  # current live connection, None while disconnected


async def send_op(op, sub):
    ex, schema, sym = sub
    if ws_conn is None:
        return  # reconnect will subscribe everything in `subs`
    try:
        await ws_conn.send(
            json.dumps({"op": op, "exchange": ex, "schema": schema, "symbol": sym})
        )
    except websockets.ConnectionClosed:
        pass  # same: reconnect handles it


async def read_config(interval=CONFIG_INTERVAL):
    global subs
    while True:
        await asyncio.sleep(int(interval))
        try:
            new = load_subs(CONFIG_PATH)
        except json.JSONDecodeError as e:
            log.warning(
                "bad config, keeping old one: %r", e
            )  # bad config at reload -> keep running
            # TODO mock_db.send(FailEvent.CONFIG_INVALID)
            continue
        added, removed = new - subs, subs - new
        subs = new
        for s in removed:
            counts.pop(count_key(s), None)
            await send_op("unsubscribe", s)
        for s in added:
            counts[count_key(s)] = 0
            await send_op("subscribe", s)
        if added or removed:
            log.info("config reloaded: +%s -%s", sorted(added), sorted(removed))


async def heartbeat_loop(interval=1.0):
    global counts
    while True:
        await asyncio.sleep(interval)
        snapshot, counts = (
            counts,
            dict.fromkeys(counts, 0),
        )  # swap, so hot path never waits
        ts = time.time_ns() // 1_000_000
        _beats = [
            Heartbeat(ts, CLIENT_ID, ex, schema, sym, n)
            for (ex, schema, sym), n in snapshot.items()
        ]
        # TODO mock_db.send(beats)  # fire-and-forget (queue.put_nowait / UDP / ILP); drop on failure, never raise


async def main():
    global ws_conn
    # both outside the reconnect loop: they keep running while disconnected
    asyncio.create_task(heartbeat_loop())
    asyncio.create_task(read_config())
    while True:
        try:
            async with websockets.connect(URL) as ws:
                log.info("connected to %s", URL)
                ws_conn = ws
                # (re)subscribe on every connect: a new server has no memory of us
                for s in subs:
                    await send_op("subscribe", s)
                async for raw in ws:
                    msg = json.loads(raw)
                    key = (msg.get("exchange_code"), msg.get("type"), msg.get("symbol"))
                    if key in counts:
                        counts[key] += 1  # hot path: just an int increment
                    print(raw)
            log.warning("server closed connection")
        except (OSError, websockets.ConnectionClosed) as e:
            log.warning("connection failed: %r, retry in 1s", e)
            # TODO mock_db.send(FailEvent.CONNECT_FAILED)  # fire-and-forget, never block this loop;
            #      the monitor service counts / TTL 1d / alerts (e.g. >30 per min)
        ws_conn = None
        await asyncio.sleep(1)


if __name__ == "__main__":
    asyncio.run(main())
