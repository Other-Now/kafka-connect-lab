package kclab.source;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

import java.util.List;
import java.util.Map;

public final class TradeSourceConfig extends AbstractConfig {
    public static final String SYMBOLS = "symbols";
    public static final String TOPIC = "topic";
    public static final String REST_URL = "rest.url";
    public static final String WS_URL = "ws.url";
    public static final String START_FROM = "start.from";
    public static final String REST_LIMIT = "rest.limit";
    public static final String BACKFILL_MAX_PER_POLL = "backfill.max.per.poll";
    public static final String RECONCILE_INTERVAL_MS = "reconcile.interval.ms";
    public static final String RECONNECT_BACKOFF_MS = "reconnect.backoff.ms";
    public static final String RECONNECT_BACKOFF_MAX_MS = "reconnect.backoff.max.ms";
    public static final String POLL_MAX_RECORDS = "poll.max.records";

    /** Internal: set by the connector on each task's config. */
    public static final String TASK_SYMBOLS = "task.symbols";
    public static final String TASK_ID = "task.id";

    public static final ConfigDef DEF = new ConfigDef()
            .define(SYMBOLS, Type.LIST, ConfigDef.NO_DEFAULT_VALUE, Importance.HIGH,
                    "Exchange symbols, e.g. BTCUSDT,ETHUSDT. Split across tasks.max tasks.")
            .define(TOPIC, Type.STRING, "trades", Importance.HIGH, "Topic to write trades to.")
            .define(REST_URL, Type.STRING, "https://data-api.binance.vision", Importance.HIGH,
                    "REST base URL; GET /api/v3/aggTrades is the gap-fill / resume path.")
            .define(WS_URL, Type.STRING, "wss://data-stream.binance.vision", Importance.HIGH,
                    "WebSocket base URL; /stream?streams=<sym>@aggTrade/... is the live path.")
            .define(START_FROM, Type.STRING, "latest", ConfigDef.ValidString.in("latest", "earliest"),
                    Importance.MEDIUM, "Where a symbol with no stored offset starts.")
            .define(REST_LIMIT, Type.INT, 1000, ConfigDef.Range.between(1, 1000), Importance.LOW,
                    "Trades per REST page.")
            .define(BACKFILL_MAX_PER_POLL, Type.INT, 5000, ConfigDef.Range.atLeast(1), Importance.LOW,
                    "Upper bound on REST-backfilled records returned by one poll().")
            .define(RECONCILE_INTERVAL_MS, Type.LONG, 30_000L, Importance.LOW,
                    "Every interval, check REST even if the WebSocket looks healthy (catches a silently dead stream).")
            .define(RECONNECT_BACKOFF_MS, Type.LONG, 200L, Importance.LOW, "WebSocket reconnect backoff base.")
            .define(RECONNECT_BACKOFF_MAX_MS, Type.LONG, 10_000L, Importance.LOW, "WebSocket reconnect backoff cap.")
            .define(POLL_MAX_RECORDS, Type.INT, 2000, ConfigDef.Range.atLeast(1), Importance.LOW,
                    "Max live records returned by one poll().")
            .define(TASK_SYMBOLS, Type.LIST, "", Importance.LOW, "Internal.")
            .define(TASK_ID, Type.STRING, "0", Importance.LOW, "Internal.");

    public TradeSourceConfig(Map<?, ?> props) { super(DEF, props); }

    public List<String> symbols() { return getList(SYMBOLS); }
    public List<String> taskSymbols() { return getList(TASK_SYMBOLS); }
}
