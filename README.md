# crypto_aggregator

A small Databento-style price adaptor: one WebSocket server that streams normalized tick data from crypto exchanges via [ccxt.pro](https://docs.ccxt.com/docs/pro).

Supported: **Binance**, **Hyperliquid**. Schemas: `trade`, `depth1` (top of book).

## Run

```bash
uv run python -m src.crypto_aggregator.price_adaptor   # server on ws://0.0.0.0:8765
uv run python -m src.crypto_aggregator.test_client     # client (retries until server is up)
```

## Protocol

Send JSON to the server:

```json
{"op": "subscribe",   "exchange": "binance",     "schema": "trade",  "symbol": "BTC/USDT"}
{"op": "subscribe",   "exchange": "hyperliquid", "schema": "depth1", "symbol": "BTC/USDC:USDC"}
{"op": "unsubscribe", "exchange": "binance",     "schema": "trade",  "symbol": "BTC/USDT"}
```

Symbols are ccxt unified symbols (`BTC/USDT` spot, `BTC/USDT:USDT` perp). Unknown or inactive symbols are rejected with suggestions, checked against the exchange's market list (`exchangeInfo` / `meta`) loaded at startup.

## Output

```json
{"type": "trade",  "event_ts": 0, "recv_ts": 0, "symbol": "BTC/USDT", "exchange_code": "BINANCE",
 "px": 0.0, "qx": 0.0, "side": "buy"}
{"type": "depth1", "event_ts": 0, "recv_ts": 0, "symbol": "BTC/USDT", "exchange_code": "BINANCE",
 "px_bid": 0.0, "qx_bid": 0.0, "px_ask": 0.0, "qx_ask": 0.0}
```

- Timestamps in ms. `event_ts` = exchange time, `recv_ts` = adaptor receive time.
- `side` = aggressor side.
- `depth1` is only sent when the top of book changes.
- Errors: `{"type": "error", "msg": "..."}`.

## Design

- **Subscription layer**: one upstream ccxt stream per `(exchange, schema, symbol)`, fanned out to all subscribed clients. When the last subscriber leaves, the stream is cancelled and the exchange is unwatched (no stale buffer on resubscribe).
- **Slow clients**: each client has its own bounded queue; overflow is dropped so one slow consumer can't stall others.
- **Client**: reconnects every 1s forever and resubscribes on each connect.
- **Config**: subscriptions live in `subs.json` (path via `SUBS_CONFIG`), reloaded every `SUBS_CONFIG_INTERVAL` seconds (default 1 day). Only the diff is (un)subscribed, so no restart or redeploy is needed. A bad file at boot crashes the client; a bad file at reload is ignored.

## Client env vars

| Var | Default |
|---|---|
| `ADAPTOR_URL` | `ws://localhost:8765` |
| `SUBS_CONFIG` | `subs.json` |
| `SUBS_CONFIG_INTERVAL` | `86400` |

## Monitoring (TODO)

The client builds a `Heartbeat(ts, client_id, exchange, schema, symbol, msg_count)` per stream every second; the send to the DB is still a TODO. A separate monitor service (not the client) owns counting, TTL (1 day) and alerts:

- fewer than 50 heartbeats/min from a client → client dead or event loop stuck
- `sum(msg_count) == 0` for a stream over 1 min → stream stale
- `FailEvent.CONNECT_FAILED` / `CONFIG_INVALID` events → alert if more than 30/min

All sends must be fire-and-forget and never block the hot path.

## TODO

- [ ] `mock_db.send` → QuestDB (ILP) for ticks and heartbeats
- [ ] Kraken, Bybit, OKX
- [ ] Server-side heartbeat per upstream key (to tell exchange-silent from client-side problems)
