# kafka-connect-lab

A Kafka Connect **source + sink connector pair** in Java 21, plus a chaos harness that kills real worker
processes and accounts for every record afterwards.

* **`TradeSourceConnector`** streams aggregate trades from Binance's public WebSocket into Kafka. It uses the REST
  `aggTrades?fromId=` endpoint to resume from the stored source offset and to fill any gap the WebSocket leaves
  (disconnects, dropped frames). It supports exactly-once source (KIP-618).
* **`PgSinkConnector`** writes the topic to PostgreSQL in one transaction per batch. It has three delivery modes,
  a dead-letter queue for bad records (KIP-610 errant-record reporter), jittered retries for transient
  database errors, and compare-and-set fencing of zombie tasks.
* **Harness.** It runs a mock Binance-shaped venue whose every trade is known in advance, one KRaft broker, and
  three Connect workers in distributed mode. All are real JVMs. It kill -9s workers, terminates the sink's
  Postgres sessions, cuts WebSockets and drops frames, and injects garbage bytes into the topic. Then it checks
  the table, the topic and the DLQ against ground truth.

~2,700 lines of Java 21 (connectors ~1,300, harness ~1,350) plus ~440 lines of tests: 18 tests, 4 of them against a real Postgres. Plain Kafka 4.1.2, no Confluent components.

```
 Binance / mock venue                     Kafka (KRaft)                            PostgreSQL
 ┌──────────────────┐  WS live    ┌──────────────────────────┐   put()    ┌────────────────────────────┐
 │ /stream?streams= ├────────────▶│ TradeSourceTask × 2      │            │ trades (+ k_partition,     │
 │ /api/v3/aggTrades│◀── REST ────┤  per-symbol sequencer    ├─▶ trades ─▶│          k_offset)         │
 └──────────────────┘ gap-fill /  │  offset = {symbol: aggId}│  (4 parts) │ kc_sink_offsets (CAS)      │
                      resume      └──────────────────────────┘     │      └────────────────────────────┘
                                                                   └─▶ trades-dlq (poison + garbage)
```

## How it works (short)

* **No gaps.** Binance aggTrade ids are dense per symbol, so the task always knows the next id it expects.
  * A live trade *below* it is dropped as a duplicate, *equal* is emitted, *above* is a gap.
  * On a gap, and always on (re)start from the stored offset, the symbol switches to REST catch-up until a
    short page shows it has reached the head.
  * A periodic REST reconcile catches a socket that is open but silent.
* **Sink modes.** Every row stores the Kafka `(partition, offset)` it came from, which lets the harness tell the
  two kinds of duplicate apart.

  | `write.mode` | where the Kafka offset is committed | redelivery after a crash | duplicate produced upstream |
  |---|---|---|---|
  | `unsafe` (control) | consumer group | duplicate row | duplicate row |
  | `offsets` | Postgres, **in the same transaction as the rows**; rewound to in `open()` | skipped | duplicate row |
  | `upsert` | consumer group | absorbed by `ON CONFLICT (symbol, agg_id) DO NOTHING` | absorbed |

* **Fencing.** In `offsets` mode the offset row moves by compare-and-set (`... WHERE next_offset = :expected`).
  A zombie task whose partition has moved updates 0 rows and rolls back its whole batch.
* **DLQ.** Records that fail validation (qty ≤ 0, bad decimal, missing field) are reported through the
  errant-record reporter. The report is awaited *before* the DB commit, so a crash can repeat a DLQ entry but
  never lose one. Bytes that aren't JSON fail in the converter and reach the same DLQ through `errors.tolerance=all`.
* **Metrics.** Per-task JMX counters (`kclab:type=source-task|sink-task`: gaps, backfills, reconnects, DLQ,
  retries, offset conflicts) sit next to the framework's own task metrics. The Prometheus JMX exporter agent on
  each worker serves both at `/metrics`.

Design notes, the delivery-semantics argument and interview Q&A: [docs/NOTES.md](docs/NOTES.md).

## Results

Measured numbers are being added; see `results/`.

## Run it

Needs JDK 21, Maven, a Kafka 4.1.x distribution (`KAFKA_HOME`) and a Postgres database. No Docker.

```bash
. ./env.sh                                   # paths on the author's box; set your own
mvn verify                                   # unit tests + Postgres ITs (set KC_TEST_DB to enable)
java -jar harness/target/harness.jar chaos   # all five scenarios, ~25 min; or name some
java -jar harness/target/harness.jar bench
java -jar harness/target/harness.jar live --duration 600   # real Binance, needs internet
```

To use the plugin in your own worker, copy `connectors/target/connectors-0.1.0-plugin.jar` into a
directory on `plugin.path`. Example configs are in [config/](config/).
