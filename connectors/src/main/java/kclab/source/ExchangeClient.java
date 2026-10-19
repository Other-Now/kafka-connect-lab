package kclab.source;

import kclab.common.AggTrade;
import kclab.common.Backoff;
import kclab.common.Json;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.WebSocket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Binance-compatible market-data client: REST aggTrades for replay, one combined WebSocket stream
 * for live trades. The live stream reconnects forever with jittered backoff; whatever it misses
 * while down is recovered by the task through REST, so reconnect logic never has to be clever.
 */
public final class ExchangeClient implements AutoCloseable {
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    private final String restUrl;
    private final String wsUrl;
    private final List<String> symbols;
    private final BlockingQueue<AggTrade> live = new LinkedBlockingQueue<>(200_000);
    private final Backoff backoff;
    private final ScheduledExecutorService reconnector = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "kclab-ws-reconnect");
        t.setDaemon(true);
        return t;
    });
    private final AtomicInteger attempts = new AtomicInteger();
    /** Each connection attempt gets a generation; only the current one may trigger a reconnect. */
    private final AtomicLong generation = new AtomicLong();
    final AtomicLong reconnects = new AtomicLong();
    final AtomicLong liveDroppedQueueFull = new AtomicLong();
    private volatile WebSocket ws;
    private volatile boolean closed;

    public ExchangeClient(String restUrl, String wsUrl, List<String> symbols, long backoffMs, long backoffMaxMs) {
        this.restUrl = restUrl.replaceAll("/+$", "");
        this.wsUrl = wsUrl.replaceAll("/+$", "");
        this.symbols = symbols;
        this.backoff = new Backoff(backoffMs, backoffMaxMs);
    }

    /** GET /api/v3/aggTrades?symbol=..&fromId=..&limit=.. ; fromId < 0 means "most recent". */
    public List<AggTrade> aggTrades(String symbol, long fromId, int limit) throws IOException, InterruptedException {
        String q = "/api/v3/aggTrades?symbol=" + symbol + "&limit=" + limit + (fromId >= 0 ? "&fromId=" + fromId : "");
        HttpRequest req = HttpRequest.newBuilder(URI.create(restUrl + q)).timeout(Duration.ofSeconds(10)).GET().build();
        HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() != 200) {
            throw new IOException("aggTrades " + symbol + " HTTP " + resp.statusCode() + ": " + truncate(resp.body()));
        }
        List<?> arr = (List<?>) Json.parse(resp.body());
        List<AggTrade> out = new ArrayList<>(arr.size());
        for (Object o : arr) {
            @SuppressWarnings("unchecked") Map<String, Object> m = (Map<String, Object>) o;
            out.add(AggTrade.fromBinance(symbol, m));
        }
        return out;
    }

    public long latestAggId(String symbol) throws IOException, InterruptedException {
        List<AggTrade> l = aggTrades(symbol, -1, 1);
        return l.isEmpty() ? -1 : l.get(0).aggId();
    }

    public BlockingQueue<AggTrade> live() { return live; }

    public void connect() {
        if (closed) return;
        StringBuilder streams = new StringBuilder();
        for (String s : symbols) {
            if (!streams.isEmpty()) streams.append('/');
            streams.append(s.toLowerCase(Locale.ROOT)).append("@aggTrade");
        }
        URI uri = URI.create(wsUrl + "/stream?streams=" + streams);
        long gen = generation.incrementAndGet();
        http.newWebSocketBuilder().connectTimeout(Duration.ofSeconds(5)).buildAsync(uri, new Listener(gen))
                .whenComplete((socket, err) -> {
                    if (err != null) scheduleReconnect(gen);
                    else { ws = socket; attempts.set(0); }
                });
    }

    private void scheduleReconnect(long gen) {
        // Moving the generation on makes any later callback from the dead socket a no-op.
        if (closed || !generation.compareAndSet(gen, gen + 1)) return;
        reconnects.incrementAndGet();
        long d = backoff.delayMs(attempts.getAndIncrement());
        reconnector.schedule(this::connect, d, TimeUnit.MILLISECONDS);
    }

    private final class Listener implements WebSocket.Listener {
        private final StringBuilder partial = new StringBuilder();
        private final long gen;

        Listener(long gen) { this.gen = gen; }

        @Override public CompletionStage<?> onText(WebSocket w, CharSequence data, boolean last) {
            partial.append(data);
            if (last) {
                String msg = partial.toString();
                partial.setLength(0);
                try {
                    Map<String, Object> env = Json.parseObject(msg);
                    @SuppressWarnings("unchecked")
                    Map<String, Object> d = (Map<String, Object>) env.getOrDefault("data", env);
                    if ("aggTrade".equals(d.get("e"))) {
                        AggTrade t = AggTrade.fromBinance(null, d);
                        if (!live.offer(t)) liveDroppedQueueFull.incrementAndGet();   // REST will fill it
                    }
                } catch (RuntimeException ignored) {
                    // malformed frame: treat like a missed one; the sequencer sees the gap
                }
            }
            w.request(1);
            return null;
        }

        @Override public CompletionStage<?> onClose(WebSocket w, int code, String reason) {
            scheduleReconnect(gen);
            return null;
        }

        @Override public void onError(WebSocket w, Throwable error) { scheduleReconnect(gen); }
    }

    /** Test/chaos hook: drop the socket so the reconnect + gap-fill path runs. */
    public void abortLive() {
        long gen = generation.get();
        WebSocket w = ws;
        if (w != null) w.abort();
        scheduleReconnect(gen);
    }

    @Override public void close() {
        closed = true;
        reconnector.shutdownNow();
        WebSocket w = ws;
        if (w != null) w.abort();
    }

    private static String truncate(String s) { return s.length() > 200 ? s.substring(0, 200) : s; }
}
