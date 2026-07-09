# coincall-rfq-maker

Async Coincall option RFQ market maker. It listens for RFQs over WebSocket,
prices each leg, risk-gates the resulting quote, and keeps it fresh over REST
until the RFQ fills or terminates.

## Core loop

Receive a Coincall RFQ over WebSocket, price its option legs, pass a
pre-trade risk gate, submit a maker quote via REST, and keep that quote fresh
(cancel/replace on price moves or a timer) until the RFQ fills or terminates.

Prices are refreshed on every >0.1% move in the underlying (or every 2
minutes, whichever comes first) and on new legs. The pricing model
(`BlackScholesModel`, fixed bid/ask vols) is explicitly a stub behind a
`PricingModel` Protocol — a real vol surface can replace it without touching
any caller.

If an already-quoted RFQ later fails the risk gate, the bot withdraws the
existing non-terminal quote. Reconciliation periodically compares local state
with Coincall OPEN RFQ/quote snapshots, adopts compatible open exchange quotes,
cancels orphan open exchange quotes, and gives newly received RFQs 120 seconds
before expiring them solely because they are absent from an OPEN snapshot.

## DRY_RUN warning

`DRY_RUN` defaults to **true**: quote intents are computed, risk-gated, and
logged, but nothing is submitted to the exchange. Set `DRY_RUN=false`
explicitly to let the bot actually create/cancel quotes on Coincall. Note
this is a deliberate change from the old prototype, which had auto-quoting
hardcoded on.

## Configuration

All settings are read from the environment or a `.env` file (see
`.env.example`).

| Setting | Default | Description |
| --- | --- | --- |
| `API_KEY` / `API_SECRET` | *(required)* | Coincall API credentials; startup fails fast with a clear error if missing |
| `TAKER_API_KEY` / `TAKER_API_SECRET` | unset | Optional taker harness credentials; must be set together or omitted |
| `REST_BASE_URL` | `https://betaapi.coincall.com` | REST API base URL |
| `WS_URL` | `wss://betaws.seizeyouralpha.com/options` | WebSocket URL |
| `DRY_RUN` | `true` | Compute and log quotes without submitting them |
| `CANCEL_ALL_ON_START` | `true` | Cancel all existing quotes once at startup |
| `CANCEL_ALL_ON_STOP` | `true` | Attempt cancel-all after every TaskGroup exit, including task failure/crash unwind |
| `HEARTBEAT_INTERVAL_SECONDS` | `5` | WebSocket heartbeat send interval |
| `PRICING_REFRESH_SECONDS` | `5` | Underlying price fetch interval |
| `QUOTE_REFRESH_SECONDS` | `10` | Quote reprice/replace timer |
| `PRICE_MOVE_THRESHOLD` | `0.001` | Fractional underlying move that forces a reprice |
| `MAX_QUOTE_NOTIONAL_USD` | `1000000` | Risk gate: max notional per quote |
| `MAX_LEG_QTY` | `100` | Risk gate: max quantity per leg |
| `MIN_TIME_TO_EXPIRY_HOURS` | `1` | Risk gate: minimum time-to-expiry to quote |
| `STALE_MARKET_DATA_SECONDS` | `30` | Risk gate: reject quotes when any intent leg has missing or stale market-data age |
| `BID_VOL` / `ASK_VOL` | `0.20` / `2.00` | Black-Scholes stub volatilities |
| `RISK_FREE_RATE` | `0.05` | Black-Scholes risk-free rate |
| `DB_PATH` | `rfq_maker.db` | SQLite audit database path |
| `LOG_LEVEL` | `INFO` | Root log level |

## Running

```sh
uv sync
uv run rfq-maker            # dry-run by default
uv run rfq-maker --dry-run  # explicit dry-run
uv run rfq-maker --no-dry-run  # actually submit quotes (use with care)
```

## Maker vs taker harness

`rfq-maker` is the product: it receives RFQs, prices them, submits maker
quotes, requotes on price moves, and records fills. `rfq-taker` is a supported
test harness that drives those paths end to end against an in-process fake
exchange. Live taker REST endpoints are not wired yet.

```sh
uv run rfq-taker --scenario all
uv run rfq-taker --scenario taker_executes
uv run rfq-taker --live  # exits until Coincall taker endpoints/keys are available
```

## Testing

```sh
uv run pytest
uv run ruff check .
uv run ruff format --check .
uv run mypy src
```

Tests never touch the network: REST/WS calls are covered by golden signing
vectors, fixture-driven WS parsing tests, and in-memory fakes for the
lifecycle/orchestration/reconciler tests.
