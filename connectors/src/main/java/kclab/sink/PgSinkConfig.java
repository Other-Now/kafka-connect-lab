package kclab.sink;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

import java.util.Map;

public final class PgSinkConfig extends AbstractConfig {
    public static final String URL = "connection.url";
    public static final String USER = "connection.user";
    public static final String PASSWORD = "connection.password";
    public static final String TABLE = "table";
    public static final String MODE = "write.mode";
    public static final String MAX_RETRIES = "max.retries";
    public static final String RETRY_BACKOFF_MS = "retry.backoff.ms";
    public static final String RETRY_BACKOFF_MAX_MS = "retry.backoff.max.ms";

    /**
     * offsets: Kafka offsets are stored in Postgres in the same transaction as the rows, and the
     *          consumer is rewound to them on assignment. Exactly-once for this hop.
     * upsert:  INSERT .. ON CONFLICT (symbol, agg_id) DO NOTHING; offsets are the framework's.
     *          Redelivery happens but is harmless, and it also absorbs upstream duplicates.
     * unsafe:  plain INSERT, framework offsets. The control: every redelivery is a duplicate row.
     */
    public enum Mode { OFFSETS, UPSERT, UNSAFE }

    public static final ConfigDef DEF = new ConfigDef()
            .define(URL, Type.STRING, ConfigDef.NO_DEFAULT_VALUE, Importance.HIGH, "JDBC URL, jdbc:postgresql://host:port/db")
            .define(USER, Type.STRING, "", Importance.HIGH, "Database user.")
            .define(PASSWORD, Type.PASSWORD, "", Importance.HIGH, "Database password.")
            .define(TABLE, Type.STRING, "trades", Importance.MEDIUM, "Target table (created if missing).")
            .define(MODE, Type.STRING, "offsets", ConfigDef.ValidString.in("offsets", "upsert", "unsafe"),
                    Importance.HIGH, "Delivery mode: offsets | upsert | unsafe.")
            .define(MAX_RETRIES, Type.INT, 10, ConfigDef.Range.atLeast(0), Importance.MEDIUM,
                    "In-task retries of a transient database error before handing the batch back to the framework.")
            .define(RETRY_BACKOFF_MS, Type.LONG, 100L, Importance.LOW, "Retry backoff base.")
            .define(RETRY_BACKOFF_MAX_MS, Type.LONG, 5_000L, Importance.LOW, "Retry backoff cap.");

    public PgSinkConfig(Map<?, ?> props) { super(DEF, props); }

    public Mode mode() { return Mode.valueOf(getString(MODE).toUpperCase(java.util.Locale.ROOT)); }

    public String table() {
        String t = getString(TABLE);
        if (!t.matches("[A-Za-z_][A-Za-z0-9_]{0,62}")) throw new IllegalArgumentException("bad table name " + t);
        return t;
    }
}
