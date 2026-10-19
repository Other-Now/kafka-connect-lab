# kafka-connect-lab — interview notes

Study sheet. The README has the numbers, and this sheet explains why they come out that way.

## 1. Kafka Connect in five sentences

* A **worker** is a JVM that runs **connectors** (config + task splitting) and **tasks** (the threads that move data).
  In distributed mode, workers with the same `group.id` form a group, and the group protocol assigns connectors and tasks.
  Configs, offsets and statuses live in three compacted topics (`config/offset/status.storage.topic`).
* A **source task** returns `SourceRecord`s from `poll()`. Each record carries a *source partition* and a
  *source offset*: opaque maps the worker stores for you and hands back through `OffsetStorageReader` on restart.
* A **sink task** receives `SinkRecord`s in `put()`. The framework commits the *consumer group* offsets
  after `preCommit()`/`flush()`, every `offset.commit.interval.ms`.
* **Converters** (here `JsonConverter` with schemas) turn `Struct`+`Schema` into bytes and back. That happens
  *outside* the connector, which is why a bad byte sequence fails in the framework, not in `put()`.
* **Error handling:** `errors.tolerance=all` + `errors.deadletterqueue.topic.name` routes converter/SMT failures
  to the DLQ. Records that the *connector* rejects in `put()` go there only if it calls
  `context.errantRecordReporter().report()` (KIP-610), which is what this sink does.

## 2. What this repo builds

**Source (`TradeSourceConnector`)**
* `symbols` are split round-robin across `tasks.max` tasks, and each task opens one combined WebSocket.
* Source partition = `{symbol}`, offset = `{aggId}` of the last trade emitted.
* Binance aggTrade ids are **dense per symbol**, so the next expected id is always `last + 1`.
  `SymbolSequencer` decides, for each live trade, to EMIT, DROP as a duplicate, or flag a GAP.
* On a gap, and always on start, the symbol goes into **catch-up**: page REST `aggTrades?fromId=next`
  until a short page arrives. Live trades for that symbol are ignored meanwhile, because REST returns them anyway.
* A periodic REST reconcile (`reconcile.interval.ms`) catches a socket that is "open" but silent.
* Reconnect uses exponential backoff with **full jitter**. A connection *generation* counter stops a dead
  socket's `onClose` and `onError` from both scheduling a reconnect.
* `exactlyOnceSupport()` returns `SUPPORTED`. Offsets are a pure function of what was emitted, so the KIP-618
  transactional producer can commit records and offsets atomically.

**Sink (`PgSinkConnector`), with three `write.mode`s**

| mode | Kafka offsets live in | duplicate trade from upstream | redelivery after crash |
|---|---|---|---|
| `unsafe` | consumer group | duplicate row | duplicate row |
| `offsets` | Postgres, **same txn as the rows** | duplicate row | skipped |
| `upsert` | consumer group | absorbed (`ON CONFLICT DO NOTHING` on `(symbol, agg_id)`) | absorbed |

* `offsets` mode: in `open(partitions)`, read `kc_sink_offsets` and call `context.offset(...)` to rewind the
  consumer, so Postgres, not the consumer group, is the source of truth.
* **Fencing:** the offset row is moved by compare-and-set: `UPDATE ... SET next_offset = :new WHERE next_offset = :expected`.
  If a zombie task, still running after its partition moved, tries to write, it updates 0 rows, the whole batch
  rolls back, and it rewinds. This is unit-tested in `PgSinkTaskIT.zombieWriterIsFencedByStoredOffsetCompareAndSet`.
  Same idea as optimistic concurrency.
* One `INSERT ... SELECT FROM unnest(arrays)` per batch. Unlike a JDBC batch with `reWriteBatchedInserts`, the
  update count is exact, so the task can count upsert conflicts.
* DLQ futures are awaited **before** the DB commit, so a crash can repeat a DLQ entry but never lose one.
  The offsets also advance over invalid records; otherwise a restart would re-report them.
* Transient SQL states (`08` connection, `40` serialization/deadlock, `53` resources, `57` admin shutdown,
  which is what `pg_terminate_backend` produces) are retried in-task with jittered backoff. After `max.retries`
  the task throws `RetriableException`, and the framework pauses and redelivers the same batch.
  Other SQL errors fail the task, because a data bug should not be retried forever.
* It deliberately does **not** treat a jump in Kafka offsets as a gap:
  * transactional topics have **commit markers** that occupy offsets;
  * converter-DLQ'd records never reach `put()`.

  An early design that rewound on "gaps" would have looped forever on the first garbage record.

## 3. Delivery semantics: the part to say out loud

*Exactly-once is per hop.* There are two hops, so there are two ways to duplicate:

1. **Source → Kafka.** At-least-once source: the worker produces records and commits offsets every
   `offset.flush.interval.ms`. A kill -9 between the two means the restarted task re-reads from an older
   offset and produces those trades **again, at new Kafka offsets**. KIP-618 (`exactly.once.source.support=enabled`)
   puts the records and the offset write in one producer transaction, and the restarted task fences the old
   transactional id.
2. **Kafka → sink.** The framework commits consumer offsets after the fact. A kill -9 means the
   **same Kafka records** are delivered again. Storing offsets in the sink's own transaction fixes this.

The verifier tells the two apart, because every row stores `(k_partition, k_offset)`:
* same Kafka coordinates twice = sink redelivery;
* same trade at two different coordinates = source duplicate.

The trap: **EOS source is invisible to a `read_uncommitted` consumer.** The aborted transactions of a killed
task are still in the log, and a default consumer reads them. Sink consumers must run with
`consumer.isolation.level=read_committed` (worker level, or `consumer.override.` per connector).
Scenario `eos-offsets-uncommitted` measures exactly this.

The pragmatic answer: an **idempotent write on a natural key** (`upsert`) gives the end result exactly-once
without either mechanism, because it absorbs both kinds of duplicate. The cost is the redelivery work itself,
and you need a natural key. Offset-in-transaction is the tool when there is no natural key (append-only events)
or when you need to know exactly what was applied.

## 4. Operational knobs worth knowing

* `scheduled.rebalance.max.delay.ms` (default **5 min**): with incremental cooperative rebalancing, a dead worker's
  tasks wait this long for it to come back before they are reassigned. The harness sets 5 s. In production it is
  a trade-off between availability and churn during rolling restarts.
* `offset.flush.interval.ms`: the at-least-once source duplicate window. Smaller means fewer duplicates and
  more offset writes.
* `tasks.max`: the source caps it at the symbol count; for a sink it is effectively capped by partition count.
* A FAILED task is **never restarted by the framework**. That's an operator or alerting job:
  `POST /connectors/<name>/restart?includeTasks=true&onlyFailed=true`.
* Plugin isolation: each `plugin.path` entry gets its own classloader. Never bundle `connect-api` or
  `kafka-clients` in the plugin (they are `provided` here). `plugin.discovery=service_load` needs the
  `META-INF/services` manifests and skips reflective scanning at startup.
* Metrics:
  * the framework publishes `kafka.connect:type=source-task-metrics|sink-task-metrics,...` (poll/put rates,
    batch sizes, offset-commit latency);
  * this plugin adds `kclab:type=source-task|sink-task,...` counters (gaps, backfills, ws reconnects, DLQ,
    retries, offset conflicts);
  * the Prometheus JMX exporter javaagent on each worker turns both into `/metrics`.

  Kafka 4.1 also added KIP-877 `pluginMetrics()` on the task context, which would be the native way to do this
  now. It is not used here, so the plugin keeps working on older workers.

## 5. Likely questions

* **How do you guarantee no gaps after a restart?** The stored offset is the last emitted id. On start the task
  backfills from `id + 1` over REST until it reaches the head, and only then trusts the WebSocket. Dense ids make
  any later gap detectable.
* **What if the venue has no replay API?** Then gaps are unrecoverable, and the honest design is to detect and alarm
  (or snapshot and resync, as an order book does) rather than claim no loss.
* **Why not Debezium/JDBC sink?** I wanted to own the offset and fencing logic to show it. The Confluent JDBC sink
  supports `upsert` with a PK; it doesn't store offsets in the target transaction.
* **How do you scale the sink?** By topic partitions. Key = symbol keeps per-symbol order. More tasks than
  partitions just idle.
* **What about schema evolution?** `JsonConverter` with embedded schemas here; in production, Avro or Protobuf with
  Schema Registry and BACKWARD compatibility. The sink validates fields by name, so adding an optional field is
  safe. **Not built here.**
* **Zombie sink after a GC pause?** Covered by the offset compare-and-set (§2). In `upsert` mode a zombie's write is
  harmless anyway.
* **How did Pacefin's integrations map to this?** Market-data ingestion (stream in, detect gaps by sequence number,
  recover from snapshot) is the source side. Order and position writes to the venue (rate-limited, idempotent,
  reconciled) are the sink side. They were not called connectors internally; this is the mapping, said out loud.

## 6. Honest gaps

* No Schema Registry or Avro. No SMTs of my own. No Testcontainers: tests use real processes and a real Postgres,
  and CI uses a Postgres service container.
* The local box is Windows, and "kill -9" there is `TerminateProcess`. CI runs the same matrix on Linux with SIGKILL.
* Single broker (RF=1), so broker failure is not tested; only worker, DB-session and venue failures are.
* The live Binance run is on this laptop only. US GitHub runners get HTTP 451 from Binance, so CI uses the mock venue.

## 7. Numbers to quote (re-read the README before an interview)

* **Chaos, per run:**
  * Setup: 150 s, 3 workers, **12 kill -9s**, ~160–190k trades.
  * Faults: DB sessions killed, WebSockets cut, ~300–390 frames dropped, 20 garbage records, poison every 1,000th.
  * **0 missing in all 10 runs** (5 scenarios × CI + local).
* **Exactly-once source + offsets-in-transaction, read_committed:** **0 duplicates** (CI 160,636 trades, local 193,432).
* **Controls, CI:**
  * `unsafe`: 2,106 sink-redelivery + 53,428 source duplicate rows.
  * `offsets` alone: 0 sink-side, 53,084 source-side.
  * `upsert`: 0 rows duplicated, even though the topic held 45,512 duplicates.
* **The read_uncommitted trap:** the same EOS pipeline with a read_uncommitted sink consumer wrote **775 duplicate
  rows** on CI (37 locally), all from aborted transactions.
* **Live Binance:** 10 min, 5 symbols, 6 kill -9s, **10,080 trades, 0 missing ids, 0 duplicates**, and 1,000/1,000
  rows match REST exactly.
* **Throughput (laptop, relative only):**
  * EOS source costs **−29%** (18.1k → 12.8k rec/s).
  * The `offsets` sink costs −28% vs plain insert (15.6k → 11.2k rows/s).
* **Story 2:** `CREATE TABLE IF NOT EXISTS` from 4 tasks at once collides in the catalog (23505 on `pg_type`).
  Fixed with a transaction-scoped advisory lock; the regression test fails 3 out of 3 without it.
* **Story:** a worker OOM at 384 MB made a task FAILED, and Connect never restarts a FAILED task, so the benchmark
  hung. The fix is a bigger heap plus fail-fast on task state. Monitoring task state is an operator duty.
