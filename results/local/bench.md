preload 250000 trades x 4 symbols, 1 worker, 4 tasks, includes connector start-up

| stage | source | sink_mode | records | seconds | records_per_s |
|---|---|---|---|---|---|
| source backfill → Kafka | alos | - | 1000000 | 55.4 | 18054 |
| Kafka → Postgres | alos | offsets | 999004 | 89.1 | 11213 |
| Kafka → Postgres | alos | upsert | 999004 | 84.7 | 11789 |
| Kafka → Postgres | alos | unsafe | 999004 | 64.1 | 15576 |
| source backfill → Kafka | eos | - | 1000000 | 78.1 | 12802 |
| Kafka → Postgres | eos | offsets | 999004 | 102.1 | 9781 |
