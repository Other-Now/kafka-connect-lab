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

~2,700 lines of Java 21 (connectors ~1,300, harness ~1,350) plus ~440 lines of tests: 19 tests, 5 of them against a real Postgres. Plain Kafka 4.1.2, no Confluent components.

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

### Chaos: every trade accounted for

Each run lasts 150 s and has a venue at 1,000 trades/s and 3 workers. It injects:
* **12 worker kill -9s** (one every 12 s, restarted 4 s later);
* the sink's Postgres sessions terminated every 15 s (8–12 times);
* every WebSocket cut every 20 s, plus 0.2% of frames silently dropped (310–391 per run);
* 20 garbage records written straight into the topic;
* every 1,000th trade is poison (qty 0).

Then the venue freezes, the pipeline drains, and the harness checks the table, the topic and the DLQ.

**Linux CI** (GitHub Actions, 4 vCPU):

| scenario | trades | missing | dup rows: sink redelivery | dup rows: source | dup trades in topic (read_committed / uncommitted) | DLQ: poison | DLQ: garbage |
|---|---|---|---|---|---|---|---|
| at-least-once source → `unsafe` sink (control) | 163,800 | **0** | 2,106 | 53,428 | 53,484 / 53,484 | 160/160 | 21/20 |
| at-least-once source → `offsets` sink | 163,448 | **0** | 0 | 53,084 | 53,140 / 53,140 | 160/160 | 20/20 |
| at-least-once source → `upsert` sink | 164,608 | **0** | 0 | 0 | 45,512 / 45,512 | 164/164 | 21/20 |
| **exactly-once source → `offsets` sink, read_committed** | 160,636 | **0** | 0 | 0 | 0 / 19 | 160/160 | 21/20 |
| exactly-once source → `offsets` sink, **read_uncommitted** | 164,684 | **0** | 0 | 775 | 0 / 776 | 164/164 | 20/20 |

**Local** (Windows laptop, 8 GB, kill = TerminateProcess):

| scenario | trades | missing | dup rows: sink redelivery | dup rows: source | dup trades in topic (read_committed / uncommitted) | DLQ: poison | DLQ: garbage |
|---|---|---|---|---|---|---|---|
| at-least-once source → `unsafe` sink (control) | 180,488 | **0** | 13,231 | 9,030 | 9,038 / 9,038 | 180/180 | 19/19 |
| at-least-once source → `offsets` sink | 190,264 | **0** | 0 | 23,344 | 23,372 / 23,372 | 188/188 | 20/20 |
| at-least-once source → `upsert` sink | 187,188 | **0** | 0 | 0 | 17,334 / 17,334 | 184/184 | 20/20 |
| **exactly-once source → `offsets` sink, read_committed** | 193,432 | **0** | 0 | 0 | 0 / 245 | 192/192 | 20/20 |
| exactly-once source → `offsets` sink, **read_uncommitted** | 182,700 | **0** | 0 | 37 | 0 / 37 | 180/180 | 20/20 |

What the columns say:

* **Missing is 0 in every run.** REST gap-fill recovered every dropped frame, cut socket and killed task. That is
  the at-least-once floor, and it holds in every mode.
* **The two duplicate sources are independent.**
  * Storing offsets in the sink transaction (`offsets`) removes *sink redelivery* (2,106 → 0 on CI,
    13,231 → 0 locally) but does nothing about trades the at-least-once *source* produced twice (53,084 on CI).
  * KIP-618 exactly-once source removes those.
  * `upsert` on the natural key absorbs both kinds, even though the topic held 45,512 duplicates.
* **Exactly-once source only works for a `read_committed` reader.** The same EOS pipeline with a
  `read_uncommitted` sink consumer wrote **775 duplicate rows on CI (37 locally)**: records from transactions
  that were aborted when their worker was killed.
* **The DLQ is at-least-once.** Every poison trade and every garbage record reached it. A converter-failed record
  that is re-read after a crash, before its offset was committed, can land there twice (21 of 20 in three CI runs).
  In at-least-once source runs, the poison trades the source duplicated reach the DLQ twice as well.

### Live: real Binance market data

10 minutes of BTC, ETH, SOL, BNB and XRP aggregate trades through an exactly-once source and the `offsets` sink,
on 2 workers, with a worker kill -9'd every 90 s (6 kills). Binance ids are dense, so correctness is checkable
without the venue's help:

| symbol | aggTrade ids | rows | missing ids | duplicate rows |
|---|---|---|---|---|
| BTCUSDT | 4093118682..4093121982 | 3,301 | 0 | 0 |
| ETHUSDT | 2103725462..2103728583 | 3,122 | 0 | 0 |
| BNBUSDT | 866771585..866773704 | 2,120 | 0 | 0 |
| XRPUSDT | 730305122..730305914 | 793 | 0 | 0 |
| SOLUSDT | 679053711..679054454 | 744 | 0 | 0 |
| **total** | | **10,080** | **0** | **0** |

The last 1,000 BTCUSDT rows match Binance REST exactly on price and quantity (1,000/1,000). The Prometheus scrape
after the run ([results/local/live-binance-metrics.txt](results/local/live-binance-metrics.txt)) shows the resume
path at work: 213 trades backfilled over REST after the last restart, and 59 live duplicates dropped by the
sequencer. This is a quiet market (≈17 trades/s across the five symbols), and it ran on the laptop only, because
GitHub's US runners get HTTP 451 from Binance.

### Throughput (laptop, 1 worker, 4 tasks, 1M trades)

Broker, worker, Postgres and the venue all run on one 8-thread laptop, so these numbers are relative,
not a capacity claim.

| stage | records/s |
|---|---|
| source, REST backfill → Kafka, at-least-once | 18,054 |
| source, REST backfill → Kafka, **exactly-once** | 12,802 (−29%: a transaction commit per poll batch) |
| sink `unsafe` | 15,576 |
| sink `upsert` | 11,789 |
| sink `offsets` | 11,213 (−28% vs unsafe: offset CAS in every transaction) |
| sink `offsets`, reading the exactly-once topic | 9,781 |

All raw results are in [results/](results/), local and CI.

### Bugs the harness found

* **A failed task waits forever.** At a 384 MB worker heap, the 4-task REST backfill hit `OutOfMemoryError` inside
  `AggTrade.fromBinance`. Connect marked the task FAILED and, by design, never restarted it, so the benchmark sat
  waiting until CI cancelled it at 30 minutes. Two fixes: benchmark workers get 1 GB (Kafka's own script defaults
  to 2 GB), and the harness now fails fast with the task's trace instead of waiting on a FAILED task.
  Alerting on task state is part of running Connect.
* **`CREATE TABLE IF NOT EXISTS` is not concurrency-safe.** Four sink tasks starting together on a fresh CI
  database raced in the catalog and one task FAILED. Locally the race reproduces as SQLState 23505 on
  `pg_type_typname_nsp_index`, 3 runs out of 3. The fix is DDL behind `pg_advisory_xact_lock`. The regression test
  starts 8 tasks behind a barrier; it fails every time without the lock and passes with it.
* **KRaft leaves snapshot `.checkpoint` files read-only.** Windows refuses to delete read-only files, so the second
  scenario's cleanup failed with `AccessDeniedException`.
* **The harness JVM never exited.** The mock venue's `HttpServer` executor threads are non-daemon.
* **A "gap" in Kafka offsets is normal.** It's caught in design, not in a run: a sink that treated a jump in Kafka
  offsets as lost data would loop forever on transactional topics, where commit markers occupy offsets, and on the
  first converter-DLQ'd record.

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
