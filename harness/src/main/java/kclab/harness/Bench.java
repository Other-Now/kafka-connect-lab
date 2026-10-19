package kclab.harness;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Throughput, no chaos. The venue starts with a preloaded history, so the source runs entirely on
 * the REST backfill path (the resume path), and each sink mode then drains the full topic.
 * One worker, so numbers are per-worker. Run once with at-least-once and once with EOS source.
 */
final class Bench {
    private Bench() { }

    static void main(Args a) throws Exception {
        long preload = a.integer("preload", 250_000);
        List<String> symbols = List.of(a.str("symbols", "BTCUSDT,ETHUSDT,SOLUSDT,BNBUSDT").split(","));
        String db = a.str("db", System.getenv().getOrDefault("KC_DB", "jdbc:postgresql://localhost:5433/kc"));
        String user = a.str("db-user", "kc"), pass = a.str("db-pass", "kc");
        long total = preload * symbols.size();
        long poisonTotal = symbols.size() * ((preload - 1) / 1000);
        List<Map<String, Object>> rows = new ArrayList<>();

        for (boolean eos : new boolean[]{false, true}) {
            String tag = eos ? "eos" : "alos";
            try (MockExchange venue = new MockExchange(symbols, 18900, 18901, 0, preload, 1000, 0);
                 Cluster cl = new Cluster(Path.of(a.str("kafka-home", System.getenv("KAFKA_HOME"))),
                         Path.of(a.str("work", "work")).resolve("bench-" + tag),
                         Path.of(a.str("plugin", "connectors/target/connectors-0.1.0-plugin.jar")), null)) {
                // At 384m the worker OOM'd in the 4-task backfill, and the FAILED task was never restarted.
                cl.workerHeap = a.str("worker-heap", "1g");
                cl.startBroker();
                cl.createTopic("trades", 4);
                cl.createTopic("trades-dlq", 1);
                cl.startWorkers(1, eos, true, Map.of());
                ConnectRest rest = new ConnectRest(() -> cl.anyAlive().url());

                long t0 = System.nanoTime();
                rest.putConfig("bench-source", Pipelines.source(symbols, "http://127.0.0.1:18900", "ws://127.0.0.1:18901",
                        "earliest", 4, eos));
                long n = awaitTopic(total, rest, "bench-source");
                double s = (System.nanoTime() - t0) / 1e9;
                rows.add(row("source backfill → Kafka", tag, "-", n, s));
                System.out.printf("source %s: %d records in %.1f s = %.0f rec/s (%s)%n", tag, n, s, n / s, venue.stats());

                for (String mode : eos ? List.of("offsets") : List.of("offsets", "upsert", "unsafe")) {
                    String table = "bench_" + tag + "_" + mode;
                    try (Connection c = DriverManager.getConnection(db, user, pass); Statement st = c.createStatement()) {
                        st.execute("DROP TABLE IF EXISTS " + table);
                        st.execute("CREATE TABLE IF NOT EXISTS kc_sink_offsets (connector text NOT NULL, topic text NOT NULL, "
                                + "kpartition int NOT NULL, next_offset bigint NOT NULL, PRIMARY KEY (connector, topic, kpartition))");
                        st.execute("DELETE FROM kc_sink_offsets WHERE connector = 'bench-sink-" + tag + "-" + mode + "'");
                    }
                    t0 = System.nanoTime();
                    rest.putConfig("bench-sink-" + tag + "-" + mode, Pipelines.sink(db, user, pass, table, mode, 4));
                    long want = total - poisonTotal;
                    long got;
                    long deadline = System.currentTimeMillis() + 600_000;
                    while ((got = count(db, user, pass, table)) < want) {
                        failIfNotRunning(rest, "bench-sink-" + tag + "-" + mode, deadline);
                        Thread.sleep(200);
                    }
                    s = (System.nanoTime() - t0) / 1e9;
                    rows.add(row("Kafka → Postgres", tag, mode, got, s));
                    System.out.printf("sink %s/%s: %d rows in %.1f s = %.0f rows/s%n", tag, mode, got, s, got / s);
                    rest.delete("bench-sink-" + tag + "-" + mode);
                }
            }
        }
        Path out = Path.of(a.str("out", "results/local"));
        Files.createDirectories(out);
        String md = Report.table(new String[]{"stage", "source", "sink_mode", "records", "seconds", "records_per_s"}, rows);
        Files.writeString(out.resolve("bench.md"), "preload " + preload + " trades x " + symbols.size()
                + " symbols, 1 worker, 4 tasks, includes connector start-up\n\n" + md);
        System.out.println(md);
    }

    static Map<String, Object> row(String stage, String src, String mode, long n, double s) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("stage", stage);
        m.put("source", src);
        m.put("sink_mode", mode);
        m.put("records", n);
        m.put("seconds", String.format("%.1f", s));
        m.put("records_per_s", String.format("%.0f", n / s));
        return m;
    }

    /** Count committed records until the topic holds {@code total}. */
    /** A FAILED task never restarts by itself: fail the bench with its trace instead of waiting forever. */
    static void failIfNotRunning(ConnectRest rest, String name, long deadline) throws Exception {
        if (System.currentTimeMillis() > deadline) throw new IllegalStateException(name + ": timed out");
        String st = rest.state(name);
        if (st.contains("FAILED")) throw new IllegalStateException(name + ": " + st);
    }

    static long awaitTopic(long total, ConnectRest rest, String name) throws Exception {
        Map<String, Object> p = new HashMap<>();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Cluster.bootstrap());
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        p.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        long n = 0;
        try (KafkaConsumer<byte[], byte[]> c = new KafkaConsumer<>(p)) {
            List<TopicPartition> tps = new ArrayList<>();
            for (int i = 0; i < 4; i++) tps.add(new TopicPartition("trades", i));
            c.assign(tps);
            c.seekToBeginning(tps);
            long deadline = System.currentTimeMillis() + 600_000, lastCheck = 0;
            while (n < total) {
                n += c.poll(Duration.ofMillis(200)).count();
                if (System.currentTimeMillis() - lastCheck > 2000) {
                    failIfNotRunning(rest, name, deadline);
                    lastCheck = System.currentTimeMillis();
                }
            }
        }
        return n;
    }

    static long count(String db, String user, String pass, String table) {
        try (Connection c = DriverManager.getConnection(db, user, pass); Statement s = c.createStatement()) {
            return Verify.one(s, "SELECT count(*) FROM " + table);
        } catch (Exception e) {
            return 0;   // table not created yet
        }
    }
}
