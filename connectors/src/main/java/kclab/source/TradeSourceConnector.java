package kclab.source;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.ExactlyOnceSupport;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Splits the symbol list round-robin across at most tasks.max tasks; one WebSocket per task. */
public class TradeSourceConnector extends SourceConnector {
    public static final String VERSION = "0.1.0";
    private Map<String, String> props;

    @Override public void start(Map<String, String> props) {
        new TradeSourceConfig(props);   // fail fast on bad config
        this.props = props;
    }

    @Override public Class<? extends Task> taskClass() { return TradeSourceTask.class; }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<String> symbols = new TradeSourceConfig(props).symbols();
        int n = Math.max(1, Math.min(maxTasks, symbols.size()));
        List<List<String>> groups = new ArrayList<>();
        for (int i = 0; i < n; i++) groups.add(new ArrayList<>());
        for (int i = 0; i < symbols.size(); i++) groups.get(i % n).add(symbols.get(i));
        List<Map<String, String>> out = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            Map<String, String> t = new HashMap<>(props);
            t.put(TradeSourceConfig.TASK_SYMBOLS, String.join(",", groups.get(i)));
            t.put(TradeSourceConfig.TASK_ID, Integer.toString(i));
            out.add(t);
        }
        return out;
    }

    /**
     * Offsets are a pure function of what was emitted (no external commit), so the framework's
     * transactional producer (KIP-618) can write records and offsets atomically.
     */
    @Override public ExactlyOnceSupport exactlyOnceSupport(Map<String, String> props) {
        return ExactlyOnceSupport.SUPPORTED;
    }

    @Override public void stop() { }
    @Override public ConfigDef config() { return TradeSourceConfig.DEF; }
    @Override public String version() { return VERSION; }
}
