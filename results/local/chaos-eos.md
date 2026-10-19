| scenario | eos_source | sink_isolation | sink_mode | worker_kills | pg_backend_kills | ws_cuts | ws_frames_dropped | trades_generated | missing | dup_rows_sink_redelivery | dup_rows_source | topic_dups_read_committed | topic_dups_read_uncommitted | poison_generated | dlq_poison_distinct | garbage_injected | dlq_garbage | dlq_records |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| eos-offsets | true | read_committed | offsets | 12 | 10 | 8 | 391 | 193432 | 0 | 0 | 0 | 0 | 245 | 192 | 192 | 20 | 20 | 212 |
| eos-offsets-uncommitted | true | read_uncommitted | offsets | 12 | 8 | 8 | 352 | 182700 | 0 | 0 | 37 | 0 | 37 | 180 | 180 | 20 | 20 | 200 |
