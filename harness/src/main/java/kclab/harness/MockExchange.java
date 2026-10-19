package kclab.harness;

import com.sun.net.httpserver.HttpServer;
import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A Binance-shaped venue whose every trade is known in advance, so a run can be checked exactly.
 *
 * Trade {@code id} of a symbol has price 100 + (id % 997) / 100 and qty 1, except every
 * {@code poisonEvery}-th id, which has qty "0" and must end up in the DLQ, not the table. Ids are
 * dense from 0. REST {@code /api/v3/aggTrades} replays any range; the WebSocket
 * {@code /stream?streams=...} pushes new trades live, and can be told to drop frames and to cut
 * connections, which is what forces the connector's gap detection and REST backfill to work.
 */
public final class MockExchange implements AutoCloseable {
    final List<String> symbols;
    private final Map<String, Series> series = new LinkedHashMap<>();
    private final double ratePerSymbol;
    private final long poisonEvery;
    volatile double wsDropProb;
    private final HttpServer rest;
    private final java.util.concurrent.ExecutorService restPool = Executors.newFixedThreadPool(4);
    private final Ws ws;
    private final Thread generator;
    private volatile boolean frozen;
    private volatile boolean closed;
    final AtomicLong framesSent = new AtomicLong();
    final AtomicLong framesDropped = new AtomicLong();
    final AtomicLong disconnects = new AtomicLong();
    final AtomicLong restRequests = new AtomicLong();

    /** Append-only trade times per symbol; the rest of a trade is a function of its id. */
    static final class Series {
        private long[] times = new long[1 << 16];
        private volatile int count;
        synchronized void append(long t) {
            if (count == times.length) times = Arrays.copyOf(times, times.length * 2);
            times[count] = t;
            count++;
        }
        int count() { return count; }
        synchronized long time(int id) { return times[id]; }
    }

    public MockExchange(List<String> symbols, int restPort, int wsPort, double ratePerSymbol, long preload,
                        long poisonEvery, double wsDropProb) throws IOException {
        this.symbols = symbols;
        this.ratePerSymbol = ratePerSymbol;
        this.poisonEvery = poisonEvery;
        this.wsDropProb = wsDropProb;
        long t0 = System.currentTimeMillis() - preload;
        for (String s : symbols) {
            Series ser = new Series();
            for (long i = 0; i < preload; i++) ser.append(t0 + i);
            series.put(s, ser);
        }
        rest = HttpServer.create(new InetSocketAddress("127.0.0.1", restPort), 64);
        rest.setExecutor(restPool);
        rest.createContext("/api/v3/aggTrades", ex -> {
            restRequests.incrementAndGet();
            int code = 200;
            String body;
            try {
                Map<String, String> q = query(ex.getRequestURI());
                Series ser = series.get(q.get("symbol"));
                if (ser == null) { code = 400; body = "{\"code\":-1121,\"msg\":\"Invalid symbol.\"}"; }
                else {
                    int limit = Math.min(1000, Integer.parseInt(q.getOrDefault("limit", "500")));
                    int n = ser.count();
                    long from = q.containsKey("fromId") ? Long.parseLong(q.get("fromId")) : Math.max(0, n - limit);
                    StringBuilder b = new StringBuilder(limit * 120).append('[');
                    for (long id = from; id < Math.min(n, from + limit); id++) {
                        if (b.length() > 1) b.append(',');
                        trade(b, q.get("symbol"), id, ser.time((int) id), false);
                    }
                    body = b.append(']').toString();
                }
            } catch (RuntimeException e) {
                code = 400;
                body = "{\"msg\":\"" + e + "\"}";
            }
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            ex.getResponseHeaders().set("Content-Type", "application/json");
            ex.sendResponseHeaders(code, bytes.length);
            try (OutputStream o = ex.getResponseBody()) { o.write(bytes); }
        });
        rest.start();
        ws = new Ws(new InetSocketAddress("127.0.0.1", wsPort));
        ws.setReuseAddr(true);
        ws.start();
        generator = new Thread(this::generate, "mock-exchange-gen");
        generator.setDaemon(true);
        generator.start();
    }

    public boolean isPoison(long id) { return poisonEvery > 0 && id > 0 && id % poisonEvery == 0; }

    /** Number of trades generated so far for a symbol (ids 0..count-1). */
    public int count(String symbol) { return series.get(symbol).count(); }

    public long poisonCount(String symbol) { return poisonEvery > 0 ? (count(symbol) - 1) / poisonEvery : 0; }

    /** Stop generating trades, so the final expected state is fixed. */
    public void freeze() { frozen = true; }

    /** Cut every live WebSocket connection (the client must reconnect and backfill). */
    public void dropAllConnections() {
        for (WebSocket c : ws.getConnections()) { c.closeConnection(1006, "chaos"); disconnects.incrementAndGet(); }
    }

    private void generate() {
        long start = System.nanoTime();
        Map<String, Long> base = new HashMap<>();
        for (String s : symbols) base.put(s, (long) series.get(s).count());
        StringBuilder b = new StringBuilder(256);
        while (!closed) {
            if (!frozen && ratePerSymbol > 0) {
                double elapsed = (System.nanoTime() - start) / 1e9;
                long now = System.currentTimeMillis();
                for (String s : symbols) {
                    Series ser = series.get(s);
                    long target = base.get(s) + (long) (elapsed * ratePerSymbol);
                    while (ser.count() < target) {
                        int id = ser.count();
                        ser.append(now);
                        if (ThreadLocalRandom.current().nextDouble() < wsDropProb) { framesDropped.incrementAndGet(); continue; }
                        b.setLength(0);
                        b.append("{\"stream\":\"").append(s.toLowerCase(Locale.ROOT)).append("@aggTrade\",\"data\":");
                        trade(b, s, id, now, true);
                        b.append('}');
                        ws.publish(s, b.toString());
                    }
                }
            }
            try { Thread.sleep(5); } catch (InterruptedException e) { return; }
        }
    }

    private void trade(StringBuilder b, String symbol, long id, long time, boolean event) {
        b.append('{');
        if (event) b.append("\"e\":\"aggTrade\",\"E\":").append(time).append(",\"s\":\"").append(symbol).append("\",");
        long cents = 10_000 + id % 997;
        b.append("\"a\":").append(id)
                .append(",\"p\":\"").append(cents / 100).append('.').append(String.format("%02d", cents % 100)).append("000000\"")
                .append(",\"q\":\"").append(isPoison(id) ? "0.00000000" : "1.00000000").append('"')
                .append(",\"f\":").append(id * 3).append(",\"l\":").append(id * 3 + 2)
                .append(",\"T\":").append(time).append(",\"m\":").append(id % 2 == 0).append(",\"M\":true}");
    }

    private final class Ws extends WebSocketServer {
        private final Map<WebSocket, Set<String>> subs = new ConcurrentHashMap<>();

        Ws(InetSocketAddress addr) { super(addr); }

        void publish(String symbol, String frame) {
            for (Map.Entry<WebSocket, Set<String>> e : subs.entrySet()) {
                if (e.getValue().contains(symbol) && e.getKey().isOpen()) {
                    try { e.getKey().send(frame); framesSent.incrementAndGet(); } catch (RuntimeException ignored) { }
                }
            }
        }

        @Override public void onOpen(WebSocket conn, ClientHandshake hs) {
            Map<String, String> q = query(URI.create(hs.getResourceDescriptor()));
            Set<String> want = ConcurrentHashMap.newKeySet();
            for (String st : q.getOrDefault("streams", "").split("/")) {
                if (st.endsWith("@aggTrade")) want.add(st.substring(0, st.length() - 9).toUpperCase(Locale.ROOT));
            }
            subs.put(conn, want);
        }

        @Override public void onClose(WebSocket conn, int code, String reason, boolean remote) { subs.remove(conn); }
        @Override public void onMessage(WebSocket conn, String message) { }
        @Override public void onError(WebSocket conn, Exception ex) { if (conn != null) subs.remove(conn); }
        @Override public void onStart() { }
    }

    static Map<String, String> query(URI u) {
        Map<String, String> m = new HashMap<>();
        String raw = u.getRawQuery();
        if (raw == null) return m;
        for (String kv : raw.split("&")) {
            String[] p = kv.split("=", 2);
            m.put(p[0], p.length > 1 ? java.net.URLDecoder.decode(p[1], StandardCharsets.UTF_8) : "");
        }
        return m;
    }

    public String stats() {
        List<String> parts = new ArrayList<>();
        for (String s : symbols) parts.add(s + "=" + count(s));
        return "trades{" + String.join(",", parts) + "} wsSent=" + framesSent + " wsDropped=" + framesDropped
                + " disconnects=" + disconnects + " rest=" + restRequests;
    }

    @Override public void close() {
        closed = true;
        generator.interrupt();
        rest.stop(0);
        restPool.shutdownNow();   // non-daemon threads: without this the harness JVM never exits
        try { ws.stop(1000); } catch (InterruptedException ignored) { Thread.currentThread().interrupt(); }
    }
}
