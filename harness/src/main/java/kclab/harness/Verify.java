package kclab.harness;

import kclab.common.Json;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Ground truth checks: what is in the topic, the DLQ and the table, against what the venue generated. */
public final class Verify {
    private Verify() { }

    /** Counts in a topic: records, distinct (symbol, agg_id), non-JSON records. */
    public record TopicScan(long records, long distinctTrades, long garbage, long poisonDistinct) {
        long duplicates() { return records - garbage - distinctTrades; }
    }

    public static TopicScan scanTopic(String topic, boolean readCommitted, MockExchange venue) {
        Map<String, Object> p = new HashMap<>();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Cluster.bootstrap());
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        p.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, readCommitted ? "read_committed" : "read_uncommitted");
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        p.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 5000);
        long records = 0, garbage = 0;
        Set<String> seen = new HashSet<>();
        Set<String> poison = new HashSet<>();
        try (KafkaConsumer<byte[], byte[]> c = new KafkaConsumer<>(p)) {
            List<TopicPartition> tps = c.partitionsFor(topic).stream().map(i -> new TopicPartition(topic, i.partition())).toList();
            c.assign(tps);
            c.seekToBeginning(tps);
            Map<TopicPartition, Long> end = c.endOffsets(tps);
            while (true) {
                boolean done = true;
                for (TopicPartition tp : tps) if (c.position(tp) < end.get(tp)) done = false;
                if (done) break;
                for (ConsumerRecord<byte[], byte[]> r : c.poll(Duration.ofMillis(500))) {
                    records++;
                    String key = tradeKey(r.value());
                    if (key == null) { garbage++; continue; }
                    seen.add(key);
                    if (venue != null && venue.isPoison(Long.parseLong(key.substring(key.indexOf(':') + 1)))) poison.add(key);
                }
            }
        }
        return new TopicScan(records, seen.size(), garbage, poison.size());
    }

    /** "SYMBOL:aggId" from a JsonConverter envelope {schema, payload}, or null if not a trade. */
    static String tradeKey(byte[] v) {
        if (v == null) return null;
        try {
            Map<String, Object> m = Json.parseObject(new String(v, StandardCharsets.UTF_8));
            Object payload = m.getOrDefault("payload", m);
            Map<?, ?> t = (Map<?, ?>) payload;
            return t.get("symbol") + ":" + ((Number) t.get("agg_id")).longValue();
        } catch (RuntimeException e) {
            return null;
        }
    }

    /** Table-side result. Duplicate rows are split by cause using the stored Kafka coordinates. */
    public record TableCheck(long rows, long distinct, long missing, long sinkRedeliveryDups, long sourceDups,
                             long unexpected) { }

    public static TableCheck checkTable(String url, String user, String pass, String table, MockExchange venue)
            throws SQLException {
        try (Connection c = DriverManager.getConnection(url, user, pass); Statement s = c.createStatement()) {
            long rows = one(s, "SELECT count(*) FROM " + table);
            long distinct = one(s, "SELECT count(*) FROM (SELECT DISTINCT symbol, agg_id FROM " + table + ") d");
            // Same Kafka record written twice: the sink re-applied a batch after a crash.
            long sinkDups = one(s, "SELECT coalesce(sum(c - 1), 0) FROM (SELECT count(*) c FROM " + table
                    + " GROUP BY k_partition, k_offset HAVING count(*) > 1) d");
            // Same trade at two different Kafka offsets: the source produced it twice.
            long sourceDups = one(s, "SELECT coalesce(sum(n - 1), 0) FROM (SELECT count(DISTINCT (k_partition, k_offset)) n FROM "
                    + table + " GROUP BY symbol, agg_id HAVING count(DISTINCT (k_partition, k_offset)) > 1) d");
            long expected = 0, unexpected = 0;
            for (String sym : venue.symbols) {
                long n = venue.count(sym);
                expected += n - venue.poisonCount(sym);
                unexpected += one(s, "SELECT count(*) FROM " + table + " WHERE symbol = '" + sym + "' AND (agg_id >= " + n
                        + " OR qty = 0)");
            }
            return new TableCheck(rows, distinct, expected - (distinct - unexpected), sinkDups, sourceDups, unexpected);
        }
    }

    static long one(Statement s, String sql) throws SQLException {
        try (ResultSet rs = s.executeQuery(sql)) { rs.next(); return rs.getLong(1); }
    }

    static long distinctRows(String url, String user, String pass, String table) {
        try (Connection c = DriverManager.getConnection(url, user, pass); Statement s = c.createStatement()) {
            return one(s, "SELECT count(*) FROM (SELECT DISTINCT symbol, agg_id FROM " + table + ") d");
        } catch (SQLException e) {
            return -1;
        }
    }
}
