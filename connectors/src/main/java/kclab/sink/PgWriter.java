package kclab.sink;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/** All Postgres access for the sink. One connection per task, opened lazily, dropped on error. */
final class PgWriter implements AutoCloseable {
    static final String OFFSETS_TABLE = "kc_sink_offsets";

    /** Another writer moved the stored offset since we read it: we are a zombie or were rewound. */
    static final class OffsetConflict extends Exception {
        OffsetConflict(String m) { super(m); }
    }

    private final PgSinkConfig cfg;
    private final String table;
    private final PgSinkConfig.Mode mode;
    private final String appName;
    private Connection conn;

    PgWriter(PgSinkConfig cfg, String appName) {
        this.cfg = cfg;
        this.table = cfg.table();
        this.mode = cfg.mode();
        this.appName = appName;
    }

    private Connection conn() throws SQLException {
        if (conn == null || conn.isClosed()) {
            Properties p = new Properties();
            p.setProperty("user", cfg.getString(PgSinkConfig.USER));
            p.setProperty("password", cfg.getPassword(PgSinkConfig.PASSWORD).value());
            p.setProperty("ApplicationName", appName);   // lets the chaos harness find and kill our backends
            p.setProperty("connectTimeout", "5");
            p.setProperty("socketTimeout", "30");
            p.setProperty("tcpKeepAlive", "true");
            conn = DriverManager.getConnection(cfg.getString(PgSinkConfig.URL), p);
            conn.setAutoCommit(false);
        }
        return conn;
    }

    void ensureSchema() throws SQLException {
        // upsert relies on the natural key; the other modes deliberately have no unique constraint,
        // so a duplicate delivery shows up as a duplicate row that the harness can count.
        String key = mode == PgSinkConfig.Mode.UPSERT ? ", PRIMARY KEY (symbol, agg_id)" : "";
        try (Statement s = conn().createStatement()) {
            s.execute("CREATE TABLE IF NOT EXISTS " + table + " ("
                    + "symbol text NOT NULL, agg_id bigint NOT NULL, price numeric NOT NULL, qty numeric NOT NULL, "
                    + "first_trade_id bigint NOT NULL, last_trade_id bigint NOT NULL, trade_time timestamptz NOT NULL, "
                    + "buyer_maker boolean NOT NULL, k_topic text NOT NULL, k_partition int NOT NULL, k_offset bigint NOT NULL, "
                    + "written_at timestamptz NOT NULL DEFAULT clock_timestamp()" + key + ")");
            s.execute("CREATE TABLE IF NOT EXISTS " + OFFSETS_TABLE + " (connector text NOT NULL, topic text NOT NULL, "
                    + "kpartition int NOT NULL, next_offset bigint NOT NULL, PRIMARY KEY (connector, topic, kpartition))");
            conn.commit();
        } catch (SQLException e) {
            rollbackQuietly();
            // Two tasks racing CREATE TABLE IF NOT EXISTS can collide on the catalog; the loser retries.
            if (!"23505".equals(e.getSQLState()) && !"42P07".equals(e.getSQLState())) throw e;
        }
    }

    Map<TopicPartition, Long> loadOffsets(String connector, Collection<TopicPartition> parts) throws SQLException {
        Map<TopicPartition, Long> out = new HashMap<>();
        try (PreparedStatement ps = conn().prepareStatement(
                "SELECT next_offset FROM " + OFFSETS_TABLE + " WHERE connector = ? AND topic = ? AND kpartition = ?")) {
            for (TopicPartition tp : parts) {
                ps.setString(1, connector);
                ps.setString(2, tp.topic());
                ps.setInt(3, tp.partition());
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) out.put(tp, rs.getLong(1));
                }
            }
            conn.commit();
        } catch (SQLException e) {
            rollbackQuietly();
            throw e;
        }
        return out;
    }

    /**
     * Writes the rows and (in offsets mode) moves each partition's stored offset from
     * {@code expected} to {@code next}, all in one transaction. Returns rows actually inserted.
     */
    int write(List<SinkRecord> recs, List<TradeRow> rows, String connector,
              Map<TopicPartition, Long> expected, Map<TopicPartition, Long> next)
            throws SQLException, OffsetConflict {
        try {
            Connection c = conn();
            int inserted = 0;
            if (!rows.isEmpty()) inserted = insertRows(c, recs, rows);
            if (mode == PgSinkConfig.Mode.OFFSETS) {
                for (Map.Entry<TopicPartition, Long> e : next.entrySet()) {
                    moveOffset(c, connector, e.getKey(), expected.get(e.getKey()), e.getValue());
                }
            }
            c.commit();
            return inserted;
        } catch (SQLException | OffsetConflict | RuntimeException e) {
            rollbackQuietly();
            throw e;
        }
    }

    private int insertRows(Connection c, List<SinkRecord> recs, List<TradeRow> rows) throws SQLException {
        int n = rows.size();
        String[] sym = new String[n], topic = new String[n];
        Long[] agg = new Long[n], first = new Long[n], last = new Long[n], time = new Long[n], koff = new Long[n];
        java.math.BigDecimal[] price = new java.math.BigDecimal[n], qty = new java.math.BigDecimal[n];
        Boolean[] bm = new Boolean[n];
        Integer[] kpart = new Integer[n];
        for (int i = 0; i < n; i++) {
            TradeRow r = rows.get(i);
            SinkRecord k = recs.get(i);
            sym[i] = r.symbol(); agg[i] = r.aggId(); price[i] = r.price(); qty[i] = r.qty();
            first[i] = r.firstTradeId(); last[i] = r.lastTradeId(); time[i] = r.tradeTime(); bm[i] = r.buyerMaker();
            topic[i] = k.topic(); kpart[i] = k.kafkaPartition(); koff[i] = k.kafkaOffset();
        }
        String conflict = mode == PgSinkConfig.Mode.UPSERT ? " ON CONFLICT (symbol, agg_id) DO NOTHING" : "";
        // One statement per batch: unnest() turns 11 arrays into rows. Unlike a JDBC batch with
        // reWriteBatchedInserts, the update count is exact, so upsert conflicts can be counted.
        String sql = "INSERT INTO " + table + " (symbol, agg_id, price, qty, first_trade_id, last_trade_id, trade_time, "
                + "buyer_maker, k_topic, k_partition, k_offset) "
                + "SELECT s, a, p, q, f, l, to_timestamp(t / 1000.0), bm, kt, kp, ko FROM unnest("
                + "?::text[], ?::bigint[], ?::numeric[], ?::numeric[], ?::bigint[], ?::bigint[], ?::bigint[], "
                + "?::boolean[], ?::text[], ?::int[], ?::bigint[]) AS u(s, a, p, q, f, l, t, bm, kt, kp, ko)" + conflict;
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setArray(1, c.createArrayOf("text", sym));
            ps.setArray(2, c.createArrayOf("int8", agg));
            ps.setArray(3, c.createArrayOf("numeric", price));
            ps.setArray(4, c.createArrayOf("numeric", qty));
            ps.setArray(5, c.createArrayOf("int8", first));
            ps.setArray(6, c.createArrayOf("int8", last));
            ps.setArray(7, c.createArrayOf("int8", time));
            ps.setArray(8, c.createArrayOf("bool", bm));
            ps.setArray(9, c.createArrayOf("text", topic));
            ps.setArray(10, c.createArrayOf("int4", kpart));
            ps.setArray(11, c.createArrayOf("int8", koff));
            return ps.executeUpdate();
        }
    }

    /**
     * Compare-and-set on the stored offset. If someone else advanced it (a zombie task that kept
     * running after its partition was reassigned, or a second copy of us), the update matches no
     * row and the whole batch rolls back instead of writing the same records twice.
     */
    private void moveOffset(Connection c, String connector, TopicPartition tp, Long expected, long next)
            throws SQLException, OffsetConflict {
        int n;
        if (expected == null) {
            try (PreparedStatement ps = c.prepareStatement("INSERT INTO " + OFFSETS_TABLE
                    + " (connector, topic, kpartition, next_offset) VALUES (?, ?, ?, ?) ON CONFLICT DO NOTHING")) {
                ps.setString(1, connector); ps.setString(2, tp.topic()); ps.setInt(3, tp.partition()); ps.setLong(4, next);
                n = ps.executeUpdate();
            }
        } else {
            try (PreparedStatement ps = c.prepareStatement("UPDATE " + OFFSETS_TABLE
                    + " SET next_offset = ? WHERE connector = ? AND topic = ? AND kpartition = ? AND next_offset = ?")) {
                ps.setLong(1, next); ps.setString(2, connector); ps.setString(3, tp.topic()); ps.setInt(4, tp.partition());
                ps.setLong(5, expected);
                n = ps.executeUpdate();
            }
        }
        if (n != 1) throw new OffsetConflict(tp + ": stored offset is no longer " + expected);
    }

    /** Transient = worth retrying on a fresh connection: connection loss, admin shutdown, serialization, deadlock. */
    static boolean isTransient(SQLException e) {
        String st = e.getSQLState();
        if (st == null) return true;   // pgjdbc I/O failures sometimes carry no state
        return st.startsWith("08") || st.startsWith("40") || st.startsWith("53") || st.startsWith("57");
    }

    void reset() {
        if (conn != null) {
            try { conn.close(); } catch (SQLException ignored) { }
            conn = null;
        }
    }

    private void rollbackQuietly() {
        try { if (conn != null && !conn.isClosed()) conn.rollback(); } catch (SQLException ignored) { }
    }

    @Override public void close() { reset(); }
}
