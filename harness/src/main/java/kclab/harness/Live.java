package kclab.harness;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

/**
 * Real Binance market data (public, no key): WebSocket + REST from data-*.binance.vision.
 * Workers are kill -9'd on a schedule. Binance aggTrade ids are dense per symbol, so "no gaps,
 * no duplicates" is checkable without knowing the venue's state: for every symbol the table
 * must hold exactly max - min + 1 distinct ids, each exactly once.
 */
final class Live {
    private Live() { }

    static void main(Args a) throws Exception {
        List<String> symbols = List.of(a.str("symbols", "BTCUSDT,ETHUSDT,SOLUSDT,BNBUSDT,XRPUSDT").split(","));
        int durationS = a.integer("duration", 600), killEveryS = a.integer("kill-every", 90), downS = a.integer("down", 10);
        String db = a.str("db", System.getenv().getOrDefault("KC_DB", "jdbc:postgresql://localhost:5433/kc"));
        String user = a.str("db-user", "kc"), pass = a.str("db-pass", "kc");
        String table = "trades_live";
        try (Connection c = DriverManager.getConnection(db, user, pass); Statement s = c.createStatement()) {
            s.execute("DROP TABLE IF EXISTS " + table);
            s.execute("CREATE TABLE IF NOT EXISTS kc_sink_offsets (connector text NOT NULL, topic text NOT NULL, "
                    + "kpartition int NOT NULL, next_offset bigint NOT NULL, PRIMARY KEY (connector, topic, kpartition))");
            s.execute("DELETE FROM kc_sink_offsets WHERE connector = 'live-sink'");
        }
        Random rnd = new Random(7);
        Map<String, Object> out = new LinkedHashMap<>();
        try (Cluster cl = new Cluster(Path.of(a.str("kafka-home", System.getenv("KAFKA_HOME"))),
                Path.of(a.str("work", "work")).resolve("live"),
                Path.of(a.str("plugin", "connectors/target/connectors-0.1.0-plugin.jar")), Chaos.jmxAgent(a))) {
            cl.startBroker();
            cl.createTopic("trades", 4);
            cl.createTopic("trades-dlq", 1);
            cl.startWorkers(a.integer("workers", 2), true, true, Map.of());
            ConnectRest rest = new ConnectRest(() -> cl.anyAlive().url());
            rest.putConfig("live-source", Pipelines.source(symbols, a.str("rest-url", "https://data-api.binance.vision"),
                    a.str("ws-url", "wss://data-stream.binance.vision"), "latest", 2, true));
            rest.putConfig("live-sink", Pipelines.sink(db, user, pass, table, "offsets", 2));
            rest.awaitRunning("live-source", 60_000);
            rest.awaitRunning("live-sink", 60_000);
            System.out.println("live pipeline RUNNING on Binance " + symbols);

            int kills = 0;
            long end = System.currentTimeMillis() + durationS * 1000L, nextKill = System.currentTimeMillis() + killEveryS * 1000L;
            while (System.currentTimeMillis() < end) {
                if (System.currentTimeMillis() >= nextKill) {
                    List<Cluster.Worker> alive = cl.workers().stream().filter(Cluster.Worker::alive).toList();
                    Cluster.Worker w = alive.get(rnd.nextInt(alive.size()));
                    w.kill();
                    kills++;
                    System.out.println("kill -9 worker " + w.index + " (kill " + kills + ", " + rows(db, user, pass, table) + " rows so far)");
                    Thread.sleep(downS * 1000L);
                    w.start();
                    nextKill = System.currentTimeMillis() + killEveryS * 1000L;
                }
                Thread.sleep(500);
            }
            for (Cluster.Worker w : cl.workers()) w.awaitRest(90_000);
            String metrics = scrape(cl);   // before pausing, while tasks still hold their counters
            rest.pause("live-source");
            long last = -1, stable = System.currentTimeMillis();
            while (System.currentTimeMillis() - stable < 20_000) {
                long n = rows(db, user, pass, table);
                if (n != last) { last = n; stable = System.currentTimeMillis(); }
                Thread.sleep(1000);
            }

            out.put("duration_s", durationS);
            out.put("worker_kills", kills);
            long totalRows = 0, totalGaps = 0, totalDups = 0;
            Map<String, String> per = new TreeMap<>();
            try (Connection c = DriverManager.getConnection(db, user, pass); Statement s = c.createStatement();
                 ResultSet rs = s.executeQuery("SELECT symbol, min(agg_id), max(agg_id), count(*), count(DISTINCT agg_id) FROM "
                         + table + " GROUP BY symbol ORDER BY symbol")) {
                while (rs.next()) {
                    long min = rs.getLong(2), max = rs.getLong(3), n = rs.getLong(4), d = rs.getLong(5);
                    long gaps = (max - min + 1) - d, dups = n - d;
                    totalRows += n; totalGaps += gaps; totalDups += dups;
                    per.put(rs.getString(1), "ids " + min + ".." + max + ", rows " + n + ", missing " + gaps + ", dups " + dups);
                }
            }
            out.put("rows", totalRows);
            out.put("missing_ids", totalGaps);
            out.put("duplicate_rows", totalDups);
            out.putAll(per);
            out.put("rest_crosscheck", restCrossCheck(db, user, pass, table, symbols.get(0),
                    a.str("rest-url", "https://data-api.binance.vision")));
            Path dir = Path.of(a.str("out", "results/local"));
            Files.createDirectories(dir);
            Files.writeString(dir.resolve("live-binance.json"), Report.json(out));
            Files.writeString(dir.resolve("live-binance-metrics.txt"), metrics);
            System.out.println(Report.json(out));
        }
    }

    /** Take the last 1000 ids we stored for one symbol and compare price/qty with what Binance REST says now. */
    static String restCrossCheck(String db, String user, String pass, String table, String symbol, String restUrl) throws Exception {
        long from;
        Map<Long, String> ours = new LinkedHashMap<>();
        try (Connection c = DriverManager.getConnection(db, user, pass); Statement s = c.createStatement()) {
            from = Verify.one(s, "SELECT greatest(min(agg_id), max(agg_id) - 999) FROM " + table + " WHERE symbol = '" + symbol + "'");
            try (ResultSet rs = s.executeQuery("SELECT agg_id, price::text, qty::text FROM " + table + " WHERE symbol = '"
                    + symbol + "' AND agg_id >= " + from + " ORDER BY agg_id")) {
                while (rs.next()) ours.put(rs.getLong(1), rs.getString(2) + "/" + rs.getString(3));
            }
        }
        String body = HttpClient.newHttpClient().send(HttpRequest.newBuilder(URI.create(restUrl
                + "/api/v3/aggTrades?symbol=" + symbol + "&fromId=" + from + "&limit=" + ours.size())).build(),
                HttpResponse.BodyHandlers.ofString()).body();
        int match = 0, n = 0;
        for (Object o : (List<?>) kclab.common.Json.parse(body)) {
            Map<?, ?> m = (Map<?, ?>) o;
            n++;
            String want = new java.math.BigDecimal((String) m.get("p")) + "/" + new java.math.BigDecimal((String) m.get("q"));
            if (want.equals(ours.get(((Number) m.get("a")).longValue()))) match++;
        }
        return symbol + ": " + match + " of " + n + " REST trades match the table exactly (price/qty)";
    }

    static long rows(String db, String user, String pass, String table) { return Bench.count(db, user, pass, table); }

    static String scrape(Cluster cl) {
        StringBuilder b = new StringBuilder();
        for (Cluster.Worker w : cl.workers()) {
            try {
                String body = HttpClient.newHttpClient().send(HttpRequest.newBuilder(URI.create("http://localhost:"
                        + (Cluster.JMX_EXPORTER_PORT_BASE + w.index) + "/metrics")).build(), HttpResponse.BodyHandlers.ofString()).body();
                List<String> keep = new ArrayList<>();
                for (String line : body.split("\n")) if (line.startsWith("kclab_")) keep.add(line);
                b.append("# worker ").append(w.index).append('\n').append(String.join("\n", keep)).append('\n');
            } catch (Exception e) {
                b.append("# worker ").append(w.index).append(": ").append(e).append('\n');
            }
        }
        return b.toString();
    }
}
