package kclab.source;

import kclab.common.AggTrade;
import kclab.common.TaskMetrics;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Streams aggregate trades for this task's symbols.
 *
 * Source partition = {symbol}, source offset = {aggId} of the last trade emitted. On start the task
 * reads its stored offsets and backfills over REST from aggId+1, so a restart neither skips nor
 * repeats a trade (repeats are still possible in at-least-once mode, if the worker dies between
 * producing records and committing their offsets; exactly.once.source.support=enabled closes that).
 */
public class TradeSourceTask extends SourceTask {
    private static final Logger log = LoggerFactory.getLogger(TradeSourceTask.class);

    private TradeSourceConfig cfg;
    private ExchangeClient client;
    private final Map<String, SymbolSequencer> seq = new LinkedHashMap<>();
    private TaskMetrics metrics;
    private String topic;
    private int restLimit;
    private int backfillMax;
    private int pollMax;
    private long reconcileMs;
    private long lastReconcile;

    @Override public String version() { return TradeSourceConnector.VERSION; }

    @Override
    public void start(Map<String, String> props) {
        cfg = new TradeSourceConfig(props);
        topic = cfg.getString(TradeSourceConfig.TOPIC);
        restLimit = cfg.getInt(TradeSourceConfig.REST_LIMIT);
        backfillMax = cfg.getInt(TradeSourceConfig.BACKFILL_MAX_PER_POLL);
        pollMax = cfg.getInt(TradeSourceConfig.POLL_MAX_RECORDS);
        reconcileMs = cfg.getLong(TradeSourceConfig.RECONCILE_INTERVAL_MS);
        List<String> symbols = cfg.taskSymbols();
        metrics = new TaskMetrics("source-task", props.getOrDefault("name", "trades-source"),
                cfg.getString(TradeSourceConfig.TASK_ID),
                "records-emitted", "live-received", "live-duplicates", "gaps-detected", "backfill-records",
                "backfill-requests", "rest-errors", "ws-reconnects", "ws-dropped-queue-full", "rest-id-skips",
                "last-trade-lag-ms");
        client = new ExchangeClient(cfg.getString(TradeSourceConfig.REST_URL), cfg.getString(TradeSourceConfig.WS_URL),
                symbols, cfg.getLong(TradeSourceConfig.RECONNECT_BACKOFF_MS),
                cfg.getLong(TradeSourceConfig.RECONNECT_BACKOFF_MAX_MS));

        List<Map<String, String>> partitions = new ArrayList<>();
        for (String s : symbols) partitions.add(TradeSchemas.sourcePartition(s));
        Map<Map<String, String>, Map<String, Object>> stored = context.offsetStorageReader().offsets(partitions);
        boolean earliest = "earliest".equals(cfg.getString(TradeSourceConfig.START_FROM));
        for (String s : symbols) {
            Map<String, Object> off = stored == null ? null : stored.get(TradeSchemas.sourcePartition(s));
            long next;
            if (off != null && off.get(TradeSchemas.OFFSET_AGG_ID) != null) {
                next = ((Number) off.get(TradeSchemas.OFFSET_AGG_ID)).longValue() + 1;
                log.info("{}: resuming from stored offset, next aggId {}", s, next);
            } else if (earliest) {
                next = 0;
            } else {
                next = latestWithRetry(s) + 1;
                log.info("{}: no stored offset, starting at latest, next aggId {}", s, next);
            }
            seq.put(s, new SymbolSequencer(next));
        }
        // Subscribe before the first backfill, so trades that happen during it are buffered, not lost.
        client.connect();
        lastReconcile = System.currentTimeMillis();
    }

    private long latestWithRetry(String symbol) {
        for (int attempt = 0; ; attempt++) {
            try {
                return client.latestAggId(symbol);
            } catch (IOException e) {
                if (attempt >= 5) throw new ConnectException("cannot read latest trade for " + symbol, e);
                sleep(200L << attempt);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new ConnectException(e);
            }
        }
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        List<SourceRecord> out = new ArrayList<>();
        long now = System.currentTimeMillis();
        if (now - lastReconcile >= reconcileMs) {
            // Periodic REST check: a WebSocket can stay "open" while delivering nothing.
            for (SymbolSequencer s : seq.values()) s.requireCatchUp();
            lastReconcile = now;
        }

        backfill(out);
        if (out.size() >= backfillMax) return publishGauges(out);

        AggTrade first = out.isEmpty() ? client.live().poll(200, TimeUnit.MILLISECONDS) : client.live().poll();
        List<AggTrade> batch = new ArrayList<>();
        if (first != null) { batch.add(first); client.live().drainTo(batch, pollMax - 1); }
        for (AggTrade t : batch) {
            metrics.add("live-received", 1);
            SymbolSequencer s = seq.get(t.symbol());
            if (s == null) continue;   // not ours (cannot happen with per-task streams)
            switch (s.onLive(t.aggId())) {
                case EMIT -> out.add(TradeSchemas.record(topic, t));
                case DUPLICATE -> metrics.add("live-duplicates", 1);
                case GAP -> metrics.add("gaps-detected", 1);
                case IGNORED_CATCHING_UP -> { }
            }
        }
        // A gap found in this batch is filled on the next poll, before any newer live trade is emitted.
        return publishGauges(out);
    }

    private void backfill(List<SourceRecord> out) {
        for (Map.Entry<String, SymbolSequencer> e : seq.entrySet()) {
            SymbolSequencer s = e.getValue();
            while (s.catchingUp() && out.size() < backfillMax) {
                List<AggTrade> page;
                try {
                    metrics.add("backfill-requests", 1);
                    page = client.aggTrades(e.getKey(), s.next(), restLimit);
                } catch (IOException ex) {
                    metrics.add("rest-errors", 1);
                    log.warn("{}: REST backfill from {} failed: {}", e.getKey(), s.next(), ex.toString());
                    if (out.isEmpty()) throw new RetriableException("REST backfill failed", ex);
                    return;   // emit what we have; retry next poll
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    return;
                }
                for (AggTrade t : page) {
                    long before = s.next();
                    if (s.onBackfill(t.aggId())) {
                        if (t.aggId() != before) metrics.add("rest-id-skips", 1);
                        out.add(TradeSchemas.record(topic, t));
                        metrics.add("backfill-records", 1);
                    }
                }
                if (page.size() < restLimit) s.caughtUp();
            }
        }
    }

    private List<SourceRecord> publishGauges(List<SourceRecord> out) {
        metrics.add("records-emitted", out.size());
        metrics.set("ws-reconnects", client.reconnects.get());
        metrics.set("ws-dropped-queue-full", client.liveDroppedQueueFull.get());
        if (!out.isEmpty()) {
            Struct last = (Struct) out.get(out.size() - 1).value();
            metrics.set("last-trade-lag-ms", System.currentTimeMillis() - last.getInt64("trade_time"));
        }
        return out.isEmpty() ? null : out;
    }

    /** Exposed for tests. */
    Map<String, Long> nextIds() {
        Map<String, Long> m = new HashMap<>();
        seq.forEach((k, v) -> m.put(k, v.next()));
        return m;
    }

    @Override
    public void stop() {
        if (client != null) client.close();
        if (metrics != null) metrics.close();
    }

    private static void sleep(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }
}
