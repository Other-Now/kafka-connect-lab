package kclab.harness;

import kclab.common.Json;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/** The bits of the Connect REST API the harness uses. Any live worker forwards to the leader. */
public final class ConnectRest {
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build();
    private final Supplier<String> base;

    public ConnectRest(Supplier<String> base) { this.base = base; }

    private HttpResponse<String> send(String method, String path, String body) throws IOException, InterruptedException {
        IOException last = null;
        for (int attempt = 0; attempt < 30; attempt++) {
            try {
                HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(base.get() + path)).timeout(Duration.ofSeconds(30))
                        .header("Content-Type", "application/json");
                b.method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body));
                HttpResponse<String> r = http.send(b.build(), HttpResponse.BodyHandlers.ofString());
                // 409 = rebalance in progress; 5xx while a worker we just killed is still the leader
                if (r.statusCode() == 409 || r.statusCode() >= 500) { Thread.sleep(1000); continue; }
                return r;
            } catch (IOException | IllegalStateException e) {
                last = e instanceof IOException io ? io : new IOException(e);
                Thread.sleep(1000);
            }
        }
        throw new IOException(method + " " + path + " kept failing", last);
    }

    public void putConfig(String name, Map<String, String> cfg) throws IOException, InterruptedException {
        StringBuilder b = new StringBuilder("{");
        for (Map.Entry<String, String> e : cfg.entrySet()) {
            if (b.length() > 1) b.append(',');
            b.append('"').append(esc(e.getKey())).append("\":\"").append(esc(e.getValue())).append('"');
        }
        HttpResponse<String> r = send("PUT", "/connectors/" + name + "/config", b.append('}').toString());
        if (r.statusCode() / 100 != 2) throw new IOException("PUT config " + name + ": " + r.statusCode() + " " + r.body());
    }

    public void delete(String name) throws IOException, InterruptedException { send("DELETE", "/connectors/" + name, null); }
    public void pause(String name) throws IOException, InterruptedException { send("PUT", "/connectors/" + name + "/pause", null); }

    /** "RUNNING" only when the connector and all its tasks are RUNNING; otherwise a short description. */
    public String state(String name) throws IOException, InterruptedException {
        HttpResponse<String> r = send("GET", "/connectors/" + name + "/status", null);
        if (r.statusCode() != 200) return "HTTP " + r.statusCode();
        Map<String, Object> st = Json.parseObject(r.body());
        String c = (String) ((Map<?, ?>) st.get("connector")).get("state");
        List<?> tasks = (List<?>) st.get("tasks");
        if (!"RUNNING".equals(c) || tasks.isEmpty()) return "connector " + c + ", tasks " + tasks.size();
        StringBuilder bad = new StringBuilder();
        for (Object o : tasks) {
            Map<?, ?> t = (Map<?, ?>) o;
            if (!"RUNNING".equals(t.get("state"))) {
                bad.append(" task ").append(t.get("id")).append('=').append(t.get("state"));
                if (t.get("trace") != null) bad.append(" (").append(firstLine((String) t.get("trace"))).append(')');
            }
        }
        return bad.isEmpty() ? "RUNNING" : bad.toString().trim();
    }

    public void awaitRunning(String name, long timeoutMs) throws IOException, InterruptedException {
        long end = System.currentTimeMillis() + timeoutMs;
        String s = "";
        while (System.currentTimeMillis() < end) {
            s = state(name);
            if (s.equals("RUNNING")) return;
            Thread.sleep(500);
        }
        throw new IllegalStateException(name + " not RUNNING after " + timeoutMs + " ms: " + s);
    }

    /** Restart FAILED tasks (a failed task is the operator's problem in Connect: it never self-restarts). */
    public int restartFailed(String name) throws IOException, InterruptedException {
        HttpResponse<String> r = send("POST", "/connectors/" + name + "/restart?includeTasks=true&onlyFailed=true", null);
        return r.statusCode();
    }

    private static String firstLine(String s) { int i = s.indexOf('\n'); return i < 0 ? s : s.substring(0, i); }
    private static String esc(String s) { return s.replace("\\", "\\\\").replace("\"", "\\\""); }
}
