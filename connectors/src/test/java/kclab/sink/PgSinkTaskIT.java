package kclab.sink;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static kclab.sink.TradeRowTest.sink;
import static kclab.sink.TradeRowTest.trade;
import static org.junit.jupiter.api.Assertions.*;

/** Real Postgres. KC_TEST_DB=jdbc:postgresql://localhost:5433/kc_test (user/password kc unless KC_TEST_USER/PASS). */
@EnabledIfEnvironmentVariable(named = "KC_TEST_DB", matches = ".+")
class PgSinkTaskIT {
    static final String URL = System.getenv("KC_TEST_DB");
    static final String USER = System.getenv().getOrDefault("KC_TEST_USER", "kc");
    static final String PASS = System.getenv().getOrDefault("KC_TEST_PASS", "kc");
    static final TopicPartition TP = new TopicPartition("trades", 0);

    static final class Ctx implements SinkTaskContext {
        final Map<TopicPartition, Long> rewinds = new HashMap<>();
        final List<SinkRecord> dlq = new ArrayList<>();
        @Override public Map<String, String> configs() { return Map.of(); }
        @Override public void offset(Map<TopicPartition, Long> offsets) { rewinds.putAll(offsets); }
        @Override public void offset(TopicPartition tp, long offset) { rewinds.put(tp, offset); }
        @Override public void timeout(long timeoutMs) { }
        @Override public Set<TopicPartition> assignment() { return Set.of(TP); }
        @Override public void pause(TopicPartition... partitions) { }
        @Override public void resume(TopicPartition... partitions) { }
        @Override public void requestCommit() { }
        @Override public org.apache.kafka.common.metrics.PluginMetrics pluginMetrics() { return null; }
        @Override public ErrantRecordReporter errantRecordReporter() {
            return (rec, err) -> { dlq.add(rec); return CompletableFuture.completedFuture(null); };
        }
    }

    @BeforeEach
    void clean() throws Exception {
        try (Connection c = DriverManager.getConnection(URL, USER, PASS); Statement s = c.createStatement()) {
            s.execute("DROP TABLE IF EXISTS it_offsets_tbl, it_upsert_tbl, it_unsafe_tbl, " + PgWriter.OFFSETS_TABLE);
        }
    }

    PgSinkTask task(Ctx ctx, String mode, String table) { return task(ctx, mode, table, "0"); }

    PgSinkTask task(Ctx ctx, String mode, String table, String taskId) {
        PgSinkTask t = new PgSinkTask();
        t.initialize(ctx);
        t.start(Map.of("name", "it-" + mode, "connection.url", URL, "connection.user", USER,
                "connection.password", PASS, "table", table, "write.mode", mode, PgSinkConnector.TASK_ID, taskId));
        return t;
    }

    static List<SinkRecord> batch(long fromOffset, int n) {
        List<SinkRecord> l = new ArrayList<>();
        for (int i = 0; i < n; i++) l.add(sink(trade(fromOffset + i, "10.5", "1"), fromOffset + i));
        return l;
    }

    static long q(String sql) throws Exception {
        try (Connection c = DriverManager.getConnection(URL, USER, PASS); Statement s = c.createStatement();
             ResultSet rs = s.executeQuery(sql)) {
            rs.next();
            return rs.getLong(1);
        }
    }

    @Test
    void offsetsModeRewindsToPostgresAndSkipsRedelivery() throws Exception {
        Ctx c1 = new Ctx();
        PgSinkTask t1 = task(c1, "offsets", "it_offsets_tbl");
        t1.open(List.of(TP));
        assertTrue(c1.rewinds.isEmpty(), "nothing stored yet");
        t1.put(batch(0, 100));
        t1.stop();   // "crash" before the framework commits anything

        Ctx c2 = new Ctx();
        PgSinkTask t2 = task(c2, "offsets", "it_offsets_tbl");
        t2.open(List.of(TP));
        assertEquals(Map.of(TP, 100L), c2.rewinds, "rewound to Postgres, not to the consumer group");
        t2.put(batch(50, 100));   // framework redelivers 50..99 anyway, plus 100..149 new
        t2.stop();
        assertEquals(150, q("SELECT count(*) FROM it_offsets_tbl"));
        assertEquals(150, q("SELECT count(DISTINCT agg_id) FROM it_offsets_tbl"));
    }

    @Test
    void zombieWriterIsFencedByStoredOffsetCompareAndSet() throws Exception {
        Ctx ca = new Ctx(), cb = new Ctx();
        PgSinkTask a = task(ca, "offsets", "it_offsets_tbl");
        a.open(List.of(TP));
        a.put(batch(0, 10));
        PgSinkTask b = task(cb, "offsets", "it_offsets_tbl");   // partition moved to b; a is now a zombie
        b.open(List.of(TP));
        b.put(batch(10, 10));
        a.put(batch(10, 10));   // a still believes next is 10
        assertEquals(20L, ca.rewinds.get(TP), "zombie must be rewound to the stored offset");
        assertEquals(20, q("SELECT count(*) FROM it_offsets_tbl"), "zombie's batch rolled back");
        a.stop();
        b.stop();
    }

    @Test
    void upsertAbsorbsDuplicatesAndUnsafeDoesNot() throws Exception {
        PgSinkTask u = task(new Ctx(), "upsert", "it_upsert_tbl");
        u.put(batch(0, 50));
        u.put(batch(25, 50));
        u.stop();
        assertEquals(75, q("SELECT count(*) FROM it_upsert_tbl"));

        PgSinkTask x = task(new Ctx(), "unsafe", "it_unsafe_tbl");
        x.put(batch(0, 50));
        x.put(batch(25, 50));
        x.stop();
        assertEquals(100, q("SELECT count(*) FROM it_unsafe_tbl"), "control: 25 duplicate rows");
    }

    @Test
    void manyTasksStartingTogetherOnAFreshTableAllSucceed() throws Exception {
        // Regression: 4 sink tasks racing CREATE TABLE IF NOT EXISTS failed one task in CI.
        try (Connection c = DriverManager.getConnection(URL, USER, PASS); Statement s = c.createStatement()) {
            s.execute("DROP TABLE IF EXISTS it_race_tbl");
        }
        int n = 8;
        java.util.concurrent.CyclicBarrier go = new java.util.concurrent.CyclicBarrier(n);
        List<CompletableFuture<PgSinkTask>> starts = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            String id = Integer.toString(i);
            starts.add(CompletableFuture.supplyAsync(() -> {
                try { go.await(); } catch (Exception e) { throw new IllegalStateException(e); }
                return task(new Ctx(), "upsert", "it_race_tbl", id);
            }));
        }
        for (CompletableFuture<PgSinkTask> f : starts) f.get().stop();
        try (Connection c = DriverManager.getConnection(URL, USER, PASS); Statement s = c.createStatement()) {
            s.execute("DROP TABLE IF EXISTS it_race_tbl");
        }
    }

    @Test
    void invalidRecordsGoToDlqAndStillAdvanceOffsets() throws Exception {
        Ctx c = new Ctx();
        PgSinkTask t = task(c, "offsets", "it_offsets_tbl");
        t.open(List.of(TP));
        List<SinkRecord> b = batch(0, 10);
        b.set(3, sink(trade(3, "10", "0"), 3));
        b.set(7, sink(trade(7, "garbage", "1"), 7));
        t.put(b);
        t.stop();
        assertEquals(2, c.dlq.size());
        assertEquals(8, q("SELECT count(*) FROM it_offsets_tbl"));
        assertEquals(10, q("SELECT next_offset FROM kc_sink_offsets WHERE connector = 'it-offsets'"));
    }
}
