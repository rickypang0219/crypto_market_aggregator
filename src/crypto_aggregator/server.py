"""Minimal price adaptor: ccxt.pro upstream -> normalized ticks -> WebSocket clients.

Client protocol (JSON over WS):
  {"op": "subscribe",   "exchange": "binance",     "schema": "trade",  "symbol": "BTC/USDT"}
  {"op": "subscribe",   "exchange": "hyperliquid", "schema": "depth1", "symbol": "BTC/USDC:USDC"}
  {"op": "unsubscribe", ...same fields...}

Run:  pip install ccxt websockets && python price_adaptor.py
"""

import asyncio
import difflib
import json
import logging
import time

import aiohttp
import ccxt.pro as ccxtpro
import websockets
from ccxt.base.errors import BadSymbol

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("adaptor")

EXCHANGES = {
    "binance": "BINANCE",
    "hyperliquid": "HYPERLIQUID",
}  # ccxt id -> exchange_code
SCHEMAS = ("trade", "depth")


def now_ms():
    return time.time_ns() // 1_000_000


def norm_trade(ex, t, recv_ts):
    return {
        "type": "trade",
        "event_ts": t["timestamp"],
        "recv_ts": recv_ts,
        "symbol": t["symbol"],
        "exchange_code": EXCHANGES[ex],
        "px": t["price"],
        "qx": t["amount"],
        "side": t["side"],  # "buy" / "sell" = aggressor side
    }


def norm_depth(ex, symbol, ob, recv_ts):
    bid = ob["bids"][0] if ob["bids"] else [None, None]
    ask = ob["asks"][0] if ob["asks"] else [None, None]
    return {
        "type": "depth1",
        "event_ts": ob["timestamp"],
        "recv_ts": recv_ts,
        "symbol": symbol,
        "exchange_code": EXCHANGES[ex],
        "px_bid": bid[0],
        "qx_bid": bid[1],
        "px_ask": ask[0],
        "qx_ask": ask[1],
    }


class Client:
    """Per-client queue so one slow consumer can't stall everyone else."""

    def __init__(self, ws):
        self.ws = ws
        self.q = asyncio.Queue(maxsize=10_000)
        self.dropped = 0

    def send(self, data):
        try:
            self.q.put_nowait(data)
        except asyncio.QueueFull:
            self.dropped += 1

    async def writer(self):
        while True:
            await self.ws.send(await self.q.get())


class Hub:
    """Subscription layer: (exchange, schema, symbol) -> one upstream stream, fan-out to N clients."""

    def __init__(self):
        self.ex = {name: getattr(ccxtpro, name)() for name in EXCHANGES}
        self.subs = {}  # key -> set[Client]
        self.tasks = {}  # key -> asyncio.Task

    async def load_markets(self):
        # binance -> GET /api/v3/exchangeInfo (+ futures), hyperliquid -> POST /info {"type": "meta"/"spotMeta"}
        await asyncio.gather(*(ex.load_markets() for ex in self.ex.values()))
        for name, ex in self.ex.items():
            log.info("%s: %d markets loaded", name, len(ex.symbols))

    def check_symbol(self, ex_name, symbol):
        """Return None if tradable, else an error message (with suggestions)."""
        ex = self.ex[ex_name]
        m = ex.markets.get(symbol)
        if m is None:
            close = difflib.get_close_matches(symbol, ex.symbols, n=3, cutoff=0.6)
            return f"{symbol} not found on {ex_name}" + (
                f", did you mean {close}?" if close else ""
            )
        if m.get("active") is False:
            return f"{symbol} on {ex_name} is not active (delisted/halted)"
        return None

    def subscribe(self, client, key):
        self.subs.setdefault(key, set()).add(client)
        if key not in self.tasks:
            log.info("upstream start %s", key)
            self.tasks[key] = asyncio.create_task(self.stream(key))

    def unsubscribe(self, client, key):
        s = self.subs.get(key)
        if s is None:
            return
        s.discard(client)
        if not s:
            log.info("upstream stop %s", key)
            del self.subs[key]
            self.tasks.pop(key).cancel()

    def publish(self, key, msg):
        data = json.dumps(msg)
        for c in self.subs.get(key, ()):
            c.send(data)

    async def stream(self, key):
        ex_name, schema, symbol = key
        ex = self.ex[ex_name]
        last_top = None
        while True:
            try:
                if schema == "trade":
                    trades = await ex.watch_trades(symbol)
                    recv = now_ms()
                    for t in trades:
                        self.publish(key, norm_trade(ex_name, t, recv))
                else:  # depth1
                    ob = await ex.watch_order_book(symbol)
                    msg = norm_depth(ex_name, symbol, ob, now_ms())
                    top = (msg["px_bid"], msg["qx_bid"], msg["px_ask"], msg["qx_ask"])
                    if top != last_top:  # only emit when top of book changes
                        last_top = top
                        self.publish(key, msg)
            except asyncio.CancelledError:
                # last subscriber gone -> tell the exchange to stop sending (else ccxt keeps receiving + buffering)
                try:
                    if schema == "trade":
                        await ex.un_watch_trades(symbol)
                    else:
                        await ex.un_watch_order_book(symbol)
                except ValueError as e:
                    log.warning("%s: unwatch failed: %r", key, e)
                raise
            except BadSymbol as e:
                self.publish(key, {"type": "error", "key": list(key), "msg": str(e)})
                for c in list(self.subs.get(key, ())):
                    self.unsubscribe(c, key)  # last one cancels this task
                return
            except (TimeoutError, aiohttp.ClientError) as e:
                log.warning("%s: %r, retrying", key, e)
                await asyncio.sleep(1)

    async def close(self):
        for ex in self.ex.values():
            await ex.close()


hub = Hub()


async def handler(ws):
    client = Client(ws)
    writer = asyncio.create_task(client.writer())
    keys = set()
    try:
        async for raw in ws:
            try:
                req = json.loads(raw)
                key = (req["exchange"], req["schema"], req["symbol"])
                op = req["op"]
            except (ValueError, KeyError) as e:
                client.send(json.dumps({"type": "error", "msg": f"bad request: {e!r}"}))
                continue
            if key[0] not in EXCHANGES or key[1] not in SCHEMAS:
                client.send(json.dumps({"type": "error", "msg": f"unsupported {key}"}))
            elif op == "subscribe":
                err = hub.check_symbol(key[0], key[2])
                if err:
                    client.send(json.dumps({"type": "error", "msg": err}))
                    continue
                hub.subscribe(client, key)
                keys.add(key)
            elif op == "unsubscribe":
                hub.unsubscribe(client, key)
                keys.discard(key)
    finally:
        for k in keys:
            hub.unsubscribe(client, k)
        writer.cancel()


async def main(host="0.0.0.0", port=8765):
    try:
        await hub.load_markets()
        async with websockets.serve(handler, host, port):
            log.info("listening on ws://%s:%d", host, port)
            await asyncio.Future()
    finally:
        await hub.close()


if __name__ == "__main__":
    asyncio.run(main())
