package kclab.harness;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;

/**
 * One chaos scenario end to end: venue → source connector → Kafka → sink connector → Postgres,
 * while workers are kill -9'd, the sink's database connections are terminated, the venue's
 * WebSockets are cut and frames dropped, and garbage bytes are written straight into the topic.
 * Then the venue is frozen, the pipeline drains, and every trade is accounted for.
 */
public final class Chaos {

    public record Scenario(String name, boolean eosSource, boolean readCommitted, String sinkMode) {
        static Scenario of(String name) {
            return switch (name) {
                case "alos-unsafe" -> new Scenario(name, false, false, "unsafe");
                case "alos-offsets" -> new Scenario(name, false, false, "offsets");
                case "alos-upsert" -> new Scenario(name, false, false, "upsert");
                case "eos-offsets" -> new Scenario(name, true, true, "offsets");
                case "eos-offsets-uncommitted" -> new Scenario(name, true, false, "offsets");
                default -> throw new IllegalArgumentException("unknown scenario " + name);
            };
        }
    }

    public static final List<String> ALL = List.of("alos-unsafe", "alos-offsets", "alos-upsert", "eos-offsets",
            "eos-offsets-uncommitted");

    final Args a;
    final String db, dbUser, dbPass;

    Chaos(Args a) {
        this.a = a;
        db = a.str("db", System.getenv().getOrDefault("KC_DB", "jdbc:postgresql://localhost:5433/kc"));
        dbUser = a.str("db-user", "kc");
        dbPass = a.str("db-pass", "kc");
    }

    Map<String, Object> run(Scenario sc) throws Exception {
        int durationS = a.integer("duration", 150);
        int killEveryS = a.integer("kill-every", 12);
        int downS = a.integer("down", 4);
        int workersN = a.integer("workers", 3);
        double rate = a.dbl("rate", 250);
        int garbageN = a.integer("garbage", 20);
        List<String> symbols = List.of(a.str("symbols", "BTCUSDT,ETHUSDT,SOLUSDT,BNBUSDT").split(","));
        String table = "trades_" + sc.name.replace('-', '_');
        Path work = Path.of(a.str("work", "work")).resolve(sc.name);
        Random rnd = new Random(a.integer("seed", 42));
        log(sc, "start: eosSource=" + sc.eosSource + " readCommitted=" + sc.readCommitted + " sink=" + sc.sinkMode);

        try (Connection c = DriverManager.getConnection(db, dbUser, dbPass); Statement s = c.createStatement()) {
            s.execute("DROP TABLE IF EXISTS " + table);
            s.execute("CREATE TABLE IF NOT EXISTS kc_sink_offsets (connector text NOT NULL, topic text NOT NULL, "
                    + "kpartition int NOT NULL, next_offset bigint NOT NULL, PRIMARY KEY (connector, topic, kpartition))");
            s.execute("DELETE FROM kc_sink_offsets WHERE connector = 'sink-" + sc.name + "'");
        }

        Map<String, Object> out = new LinkedHashMap<>();
        try (MockExchange venue = new MockExchange(symbols, 18900, 18901, rate, 0, a.integer("poison-every", 1000),
                a.dbl("ws-drop", 0.002));
             Cluster cl = new Cluster(Path.of(a.str("kafka-home", System.getenv("KAFKA_HOME"))), work,
                     Path.of(a.str("plugin", "connectors/target/connectors-0.1.0-plugin.jar")), jmxAgent(a))) {
            cl.startBroker();
            cl.createTopic("trades", 4);
            cl.createTopic("trades-dlq", 1);
            cl.startWorkers(workersN, sc.eosSource, sc.readCommitted, Map.of());
            ConnectRest rest = new ConnectRest(() -> cl.anyAlive().url());

            Map<String, String> src = new LinkedHashMap<>();
            src.put("connector.class", "kclab.source.TradeSourceConnector");
            src.put("tasks.max", "2");
            src.put("symbols", String.join(",", symbols));
            src.put("topic", "trades");
            src.put("rest.url", "http://127.0.0.1:18900");
            src.put("ws.url", "ws://127.0.0.1:18901");
            src.put("start.from", "earliest");
            src.put("reconcile.interval.ms", "10000");
            if (sc.eosSource) {
                src.put("exactly.once.support", "required");
                src.put("transaction.boundary", "poll");
            }
            Map<String, String> sink = new LinkedHashMap<>();
            sink.put("connector.class", "kclab.sink.PgSinkConnector");
            sink.put("tasks.max", "2");
            sink.put("topics", "trades");
            sink.put("connection.url", db);
            sink.put("connection.user", dbUser);
            sink.put("connection.password", dbPass);
            sink.put("table", table);
            sink.put("write.mode", sc.sinkMode);
            sink.put("errors.tolerance", "all");
            sink.put("errors.deadletterqueue.topic.name", "trades-dlq");
            sink.put("errors.deadletterqueue.topic.replication.factor", "1");
            sink.put("errors.deadletterqueue.context.headers.enable", "true");
            sink.put("errors.retry.timeout.ms", "60000");
            String srcName = "source-" + sc.name, sinkName = "sink-" + sc.name;
            rest.putConfig(srcName, src);
            rest.putConfig(sinkName, sink);
            rest.awaitRunning(srcName, 60_000);
            rest.awaitRunning(sinkName, 60_000);
            log(sc, "pipeline RUNNING on " + workersN + " workers");

            int kills = 0, pgKills = 0, wsCuts = 0, garbage = 0, restarts = 0;
            long t0 = System.currentTimeMillis(), end = t0 + durationS * 1000L;
            long nextKill = t0 + killEveryS * 1000L, nextPg = t0 + 7_000, nextWs = t0 + 9_000;
            long garbageGap = Math.max(1, durationS * 1000L / Math.max(1, garbageN));
            long nextGarbage = t0 + garbageGap / 2;
            Map<Cluster.Worker, Long> restartAt = new LinkedHashMap<>();
            try (KafkaProducer<byte[], byte[]> junk = new KafkaProducer<>(Map.of(
                    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Cluster.bootstrap(),
                    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class,
                    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class))) {
                while (System.currentTimeMillis() < end) {
                    long now = System.currentTimeMillis();
                    if (now >= nextKill) {
                        List<Cluster.Worker> alive = cl.workers().stream().filter(Cluster.Worker::alive).toList();
                        if (alive.size() > 1) {
                            Cluster.Worker w = alive.get(rnd.nextInt(alive.size()));
                            w.kill();
                            kills++;
                            restartAt.put(w, now + downS * 1000L);
                            log(sc, "kill -9 worker " + w.index + " (kill " + kills + ")");
                        }
                        nextKill = now + killEveryS * 1000L;
                    }
                    for (var it = restartAt.entrySet().iterator(); it.hasNext(); ) {
                        var e = it.next();
                        if (now >= e.getValue()) { e.getKey().start(); it.remove(); }
                    }
                    if (now >= nextPg) { pgKills += terminateSinkBackends(sinkName); nextPg = now + 15_000; }
                    if (now >= nextWs) { venue.dropAllConnections(); wsCuts++; nextWs = now + 20_000; }
                    if (now >= nextGarbage && garbage < garbageN) {
                        junk.send(new ProducerRecord<>("trades", rnd.nextInt(4), null,
                                ("not-json-" + garbage + "{").getBytes(StandardCharsets.UTF_8)));
                        garbage++;
                        nextGarbage = now + garbageGap;
                    }
                    Thread.sleep(100);
                }
                junk.flush();
            }
            for (var e : restartAt.keySet()) e.start();
            venue.freeze();
            for (Cluster.Worker w : cl.workers()) w.awaitRest(90_000);
            log(sc, "chaos done: kills=" + kills + " pgKills=" + pgKills + " wsCuts=" + wsCuts + " garbage=" + garbage
                    + "; venue " + venue.stats());

            // Drain: wait until the table holds every valid trade, or stops changing.
            long expectedValid = 0;
            for (String s : symbols) expectedValid += venue.count(s) - venue.poisonCount(s);
            long last = -1, stableSince = System.currentTimeMillis(), drainStart = System.currentTimeMillis();
            while (true) {
                if (!"RUNNING".equals(rest.state(srcName)) || !"RUNNING".equals(rest.state(sinkName))) {
                    restarts += 1;
                    rest.restartFailed(srcName);
                    rest.restartFailed(sinkName);
                }
                long n = Verify.distinctRows(db, dbUser, dbPass, table);
                if (n != last) { last = n; stableSince = System.currentTimeMillis(); }
                if (n >= expectedValid && System.currentTimeMillis() - stableSince > 15_000) break;
                if (System.currentTimeMillis() - stableSince > 90_000) { log(sc, "drain stalled at " + n + "/" + expectedValid); break; }
                Thread.sleep(1000);
            }
            long drainMs = System.currentTimeMillis() - drainStart;

            Verify.TableCheck t = Verify.checkTable(db, dbUser, dbPass, table, venue);
            Verify.TopicScan committed = Verify.scanTopic("trades", true, venue);
            Verify.TopicScan uncommitted = Verify.scanTopic("trades", false, venue);
            Verify.TopicScan dlq = Verify.scanTopic("trades-dlq", false, venue);
            long generated = 0, poison = 0;
            for (String s : symbols) { generated += venue.count(s); poison += venue.poisonCount(s); }

            out.put("scenario", sc.name);
            out.put("eos_source", sc.eosSource);
            out.put("sink_isolation", sc.readCommitted ? "read_committed" : "read_uncommitted");
            out.put("sink_mode", sc.sinkMode);
            out.put("duration_s", durationS);
            out.put("worker_kills", kills);
            out.put("pg_backend_kills", pgKills);
            out.put("ws_cuts", wsCuts);
            out.put("ws_frames_dropped", venue.framesDropped.get());
            out.put("garbage_injected", garbage);
            out.put("trades_generated", generated);
            out.put("poison_generated", poison);
            out.put("table_rows", t.rows());
            out.put("table_distinct", t.distinct());
            out.put("missing", t.missing());
            out.put("dup_rows_sink_redelivery", t.sinkRedeliveryDups());
            out.put("dup_rows_source", t.sourceDups());
            out.put("unexpected_rows", t.unexpected());
            out.put("topic_dups_read_committed", committed.duplicates());
            out.put("topic_dups_read_uncommitted", uncommitted.duplicates());
            out.put("topic_missing", generated - committed.distinctTrades());
            out.put("dlq_records", dlq.records());
            out.put("dlq_poison_distinct", dlq.poisonDistinct());
            out.put("dlq_garbage", dlq.garbage());
            out.put("task_restarts_needed", restarts);
            out.put("drain_ms", drainMs);
            log(sc, "RESULT " + out);
        }
        return out;
    }

    /** Kill the sink's Postgres sessions (same role may signal its own backends). */
    int terminateSinkBackends(String sinkName) {
        try (Connection c = DriverManager.getConnection(db, dbUser, dbPass); Statement s = c.createStatement()) {
            return (int) Verify.one(s, "SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity WHERE application_name LIKE 'kclab-sink-"
                    + sinkName + "-%'");
        } catch (Exception e) {
            return 0;
        }
    }

    static Path jmxAgent(Args a) {
        String p = a.str("jmx-agent", System.getenv("KC_JMX_AGENT"));
        return p == null || p.isEmpty() || !Files.exists(Path.of(p)) ? null : Path.of(p);
    }

    static void log(Scenario sc, String msg) {
        System.out.printf(Locale.ROOT, "[%tT] %-24s %s%n", System.currentTimeMillis(), sc.name, msg);
    }

    public static void main(Args a) throws Exception {
        List<String> names = a.positional.isEmpty() ? ALL : a.positional;
        Path results = Path.of(a.str("out", "results/local"));
        Files.createDirectories(results);
        List<Map<String, Object>> rows = new ArrayList<>();
        Chaos ch = new Chaos(a);
        for (String n : names) {
            Map<String, Object> r = ch.run(Scenario.of(n));
            rows.add(r);
            Files.writeString(results.resolve("chaos-" + n + ".json"), Report.json(r));
        }
        Files.writeString(results.resolve("chaos-" + a.str("label", "summary") + ".md"), Report.chaosTable(rows));
        System.out.println(Report.chaosTable(rows));
    }
}
