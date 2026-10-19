package kclab.sink;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Partition assignment is the consumer group's job; every task gets the same config plus an id. */
public class PgSinkConnector extends SinkConnector {
    public static final String VERSION = "0.1.0";
    static final String TASK_ID = "task.id";
    private Map<String, String> props;

    @Override public void start(Map<String, String> props) {
        new PgSinkConfig(props).table();   // fail fast
        this.props = props;
    }

    @Override public Class<? extends Task> taskClass() { return PgSinkTask.class; }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> out = new ArrayList<>();
        for (int i = 0; i < maxTasks; i++) {
            Map<String, String> t = new HashMap<>(props);
            t.put(TASK_ID, Integer.toString(i));
            out.add(t);
        }
        return out;
    }

    @Override public void stop() { }
    @Override public ConfigDef config() { return PgSinkConfig.DEF; }
    @Override public String version() { return VERSION; }
}
