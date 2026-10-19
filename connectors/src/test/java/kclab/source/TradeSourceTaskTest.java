package kclab.source;

import com.sun.net.httpserver.HttpServer;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/** REST-only venue (the WebSocket URL points nowhere): exercises resume and backfill paging. */
class TradeSourceTaskTest {
    static final int TRADES = 2_500;
    HttpServer server;
    String rest;

    @BeforeEach
    void venue() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/api/v3/aggTrades", ex -> {
            Map<String, String> q = query(ex.getRequestURI());
            int limit = Integer.parseInt(q.get("limit"));
            long from = q.containsKey("fromId") ? Long.parseLong(q.get("fromId")) : TRADES - limit;
            StringBuilder b = new StringBuilder("[");
            for (long id = from; id < Math.min(TRADES, from + limit); id++) {
                if (b.length() > 1) b.append(',');
                b.append("{\"a\":").append(id).append(",\"p\":\"1.5\",\"q\":\"2\",\"f\":").append(id)
                        .append(",\"l\":").append(id).append(",\"T\":").append(1000 + id).append(",\"m\":true}");
            }
            byte[] body = b.append(']').toString().getBytes(StandardCharsets.UTF_8);
            ex.sendResponseHeaders(200, body.length);
            ex.getResponseBody().write(body);
            ex.close();
        });
        server.start();
        rest = "http://127.0.0.1:" + server.getAddress().getPort();
    }

    @AfterEach
    void down() { server.stop(0); }

    static Map<String, String> query(URI u) {
        Map<String, String> m = new HashMap<>();
        for (String kv : u.getRawQuery().split("&")) { String[] p = kv.split("=", 2); m.put(p[0], p[1]); }
        return m;
    }

    TradeSourceTask start(Map<String, Object> storedOffset, String startFrom) {
        TradeSourceTask t = new TradeSourceTask();
        t.initialize(new SourceTaskContext() {
            @Override public Map<String, String> configs() { return Map.of(); }
            @Override public org.apache.kafka.common.metrics.PluginMetrics pluginMetrics() { return null; }
            @Override public OffsetStorageReader offsetStorageReader() {
                return new OffsetStorageReader() {
                    @Override public <T> Map<String, Object> offset(Map<String, T> partition) { return storedOffset; }
                    @Override public <T> Map<Map<String, T>, Map<String, Object>> offsets(Collection<Map<String, T>> ps) {
                        Map<Map<String, T>, Map<String, Object>> m = new HashMap<>();
                        if (storedOffset != null) for (Map<String, T> p : ps) m.put(p, storedOffset);
                        return m;
                    }
                };
            }
        });
        t.start(Map.of("name", "test-src", "symbols", "BTCUSDT", TradeSourceConfig.TASK_SYMBOLS, "BTCUSDT",
                "rest.url", rest, "ws.url", "ws://127.0.0.1:1", "start.from", startFrom,
                "backfill.max.per.poll", "1000", "reconnect.backoff.ms", "50"));
        return t;
    }

    static List<Long> drain(TradeSourceTask t) throws InterruptedException {
        List<Long> ids = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            List<SourceRecord> rs = t.poll();
            if (rs == null) continue;
            for (SourceRecord r : rs) {
                ids.add(((Struct) r.value()).getInt64("agg_id"));
                assertEquals(Map.of("aggId", ((Struct) r.value()).getInt64("agg_id")), r.sourceOffset());
            }
        }
        return ids;
    }

    @Test
    void resumesFromStoredOffsetWithoutGapOrRepeat() throws Exception {
        TradeSourceTask t = start(Map.of("aggId", 999L), "latest");
        List<Long> ids = drain(t);
        t.stop();
        assertEquals(TRADES - 1000, ids.size());
        for (int i = 0; i < ids.size(); i++) assertEquals(1000L + i, ids.get(i));
    }

    @Test
    void earliestStartsAtZeroAndLatestStartsAtHead() throws Exception {
        TradeSourceTask e = start(null, "earliest");
        List<Long> all = drain(e);
        e.stop();
        assertEquals(TRADES, all.size());
        assertEquals(0L, all.get(0));

        TradeSourceTask l = start(null, "latest");
        assertEquals(Map.of("BTCUSDT", (long) TRADES), l.nextIds());
        assertTrue(drain(l).isEmpty());
        l.stop();
    }
}
