package kclab.harness;

import java.util.List;
import java.util.Map;

final class Report {
    private Report() { }

    static String json(Map<String, Object> m) {
        StringBuilder b = new StringBuilder("{\n");
        int i = 0;
        for (Map.Entry<String, Object> e : m.entrySet()) {
            b.append("  \"").append(e.getKey()).append("\": ");
            Object v = e.getValue();
            if (v instanceof Number || v instanceof Boolean) b.append(v);
            else b.append('"').append(v).append('"');
            b.append(++i < m.size() ? ",\n" : "\n");
        }
        return b.append("}\n").toString();
    }

    static String chaosTable(List<Map<String, Object>> rows) {
        String[] cols = {"scenario", "eos_source", "sink_isolation", "sink_mode", "worker_kills", "pg_backend_kills",
                "ws_cuts", "ws_frames_dropped", "trades_generated", "missing", "dup_rows_sink_redelivery",
                "dup_rows_source", "topic_dups_read_committed", "topic_dups_read_uncommitted", "poison_generated",
                "dlq_poison_distinct", "garbage_injected", "dlq_garbage", "dlq_records"};
        return table(cols, rows);
    }

    static String table(String[] cols, List<Map<String, Object>> rows) {
        StringBuilder b = new StringBuilder("|");
        for (String c : cols) b.append(' ').append(c).append(" |");
        b.append("\n|");
        for (String c : cols) b.append("---|");
        b.append('\n');
        for (Map<String, Object> r : rows) {
            b.append('|');
            for (String c : cols) b.append(' ').append(r.getOrDefault(c, "")).append(" |");
            b.append('\n');
        }
        return b.toString();
    }
}
