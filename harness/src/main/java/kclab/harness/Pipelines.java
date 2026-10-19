package kclab.harness;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Connector configs shared by chaos, bench and live runs. */
final class Pipelines {
    private Pipelines() { }

    static Map<String, String> source(List<String> symbols, String restUrl, String wsUrl, String startFrom,
                                      int tasks, boolean eos) {
        Map<String, String> c = new LinkedHashMap<>();
        c.put("connector.class", "kclab.source.TradeSourceConnector");
        c.put("tasks.max", Integer.toString(tasks));
        c.put("symbols", String.join(",", symbols));
        c.put("topic", "trades");
        c.put("rest.url", restUrl);
        c.put("ws.url", wsUrl);
        c.put("start.from", startFrom);
        c.put("reconcile.interval.ms", "10000");
        if (eos) {
            c.put("exactly.once.support", "required");
            c.put("transaction.boundary", "poll");
        }
        return c;
    }

    static Map<String, String> sink(String db, String user, String pass, String table, String mode, int tasks) {
        Map<String, String> c = new LinkedHashMap<>();
        c.put("connector.class", "kclab.sink.PgSinkConnector");
        c.put("tasks.max", Integer.toString(tasks));
        c.put("topics", "trades");
        c.put("connection.url", db);
        c.put("connection.user", user);
        c.put("connection.password", pass);
        c.put("table", table);
        c.put("write.mode", mode);
        c.put("errors.tolerance", "all");
        c.put("errors.deadletterqueue.topic.name", "trades-dlq");
        c.put("errors.deadletterqueue.topic.replication.factor", "1");
        c.put("errors.deadletterqueue.context.headers.enable", "true");
        c.put("errors.retry.timeout.ms", "60000");
        return c;
    }
}
