package kclab.harness;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;

import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.Socket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * Real processes, not an embedded cluster: one KRaft broker and N Connect workers in distributed
 * mode, each its own JVM started from the Kafka distribution. "Kill" is destroyForcibly(), which is
 * SIGKILL on Linux and TerminateProcess on Windows: no shutdown hooks, no final offset flush.
 */
public final class Cluster implements AutoCloseable {
    public static final int BROKER_PORT = 19092;
    public static final int CONTROLLER_PORT = 19093;
    public static final int WORKER_PORT_BASE = 18083;
    public static final int JMX_EXPORTER_PORT_BASE = 19400;

    private final Path kafkaHome;
    private final Path work;
    private final Path pluginDir;
    private final Path jmxAgent;   // optional Prometheus JMX exporter javaagent jar
    private final String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
    private Process broker;
    private final List<Worker> workers = new ArrayList<>();
    /** Worker heap. Kafka's connect-distributed script defaults to 2G; chaos runs 3 workers on a laptop. */
    String workerHeap = "384m";

    public final class Worker {
        final int index;
        final Path props;
        Process proc;
        int starts;

        Worker(int index, Path props) { this.index = index; this.props = props; }
        public int restPort() { return WORKER_PORT_BASE + index; }
        public boolean alive() { return proc != null && proc.isAlive(); }
        public String url() { return "http://localhost:" + restPort(); }

        public void start() throws IOException {
            List<String> cmd = new ArrayList<>(List.of(java, "-Xms128m", "-Xmx" + workerHeap));
            if (jmxAgent != null) {
                cmd.add("-javaagent:" + jmxAgent + "=" + (JMX_EXPORTER_PORT_BASE + index) + ":" + work.resolve("jmx-exporter.yaml"));
            }
            cmd.addAll(List.of(log4j("connect-log4j2.yaml"), "-Dkafka.logs.dir=" + work.resolve("logs/worker-" + index),
                    "-cp", kafkaHome.resolve("libs") + File.separator + "*",
                    "org.apache.kafka.connect.cli.ConnectDistributed", props.toString()));
            proc = new ProcessBuilder(cmd).redirectErrorStream(true)
                    .redirectOutput(ProcessBuilder.Redirect.appendTo(work.resolve("worker-" + index + ".out").toFile()))
                    .start();
            starts++;
        }

        public void kill() throws InterruptedException {
            if (proc != null) { proc.destroyForcibly(); proc.waitFor(); }
        }

        public boolean awaitRest(long timeoutMs) throws InterruptedException {
            long end = System.currentTimeMillis() + timeoutMs;
            while (System.currentTimeMillis() < end) {
                if (!alive()) return false;
                try {
                    HttpURLConnection c = (HttpURLConnection) URI.create(url() + "/").toURL().openConnection();
                    c.setConnectTimeout(500);
                    c.setReadTimeout(1000);
                    if (c.getResponseCode() == 200) return true;
                } catch (IOException ignored) { }
                Thread.sleep(250);
            }
            return false;
        }
    }

    public Cluster(Path kafkaHome, Path work, Path pluginJar, Path jmxAgent) throws IOException {
        this.kafkaHome = kafkaHome;
        this.work = work;
        this.jmxAgent = jmxAgent;
        deleteTree(work);
        Files.createDirectories(work.resolve("logs"));
        pluginDir = work.resolve("plugins/kclab");
        Files.createDirectories(pluginDir);
        Files.copy(pluginJar, pluginDir.resolve(pluginJar.getFileName()));
        if (jmxAgent != null) {
            Files.writeString(work.resolve("jmx-exporter.yaml"), String.join("\n",
                    "lowercaseOutputName: true",
                    "rules:",
                    "  - pattern: 'kclab<type=(.+), connector=\"?([^,\"]+)\"?, task=(\\d+)><>([a-z-]+)'",
                    "    name: kclab_$1_$4",
                    "    labels: {connector: \"$2\", task: \"$3\"}",
                    "    type: GAUGE",
                    "  - pattern: 'kafka.connect<type=(.+)-metrics, connector=(.+), task=(\\d+)><>(.+-total|.+-rate|.+-avg|.+-max|.+-ms|.+-count)'",
                    "    name: kafka_connect_$1_$4",
                    "    labels: {connector: \"$2\", task: \"$3\"}",
                    "  - pattern: 'kafka.connect<type=connect-worker-metrics><>(.+)'",
                    "    name: kafka_connect_worker_$1",
                    ""));
        }
    }

    private String log4j(String file) {
        return "-Dlog4j2.configurationFile=" + kafkaHome.resolve("config").resolve(file).toUri();
    }

    public void startBroker() throws Exception {
        Path props = work.resolve("server.properties");
        Path data = work.resolve("kafka-data");
        Files.writeString(props, String.join("\n",
                "process.roles=broker,controller",
                "node.id=1",
                "controller.quorum.bootstrap.servers=localhost:" + CONTROLLER_PORT,
                "listeners=PLAINTEXT://localhost:" + BROKER_PORT + ",CONTROLLER://localhost:" + CONTROLLER_PORT,
                "advertised.listeners=PLAINTEXT://localhost:" + BROKER_PORT,
                "controller.listener.names=CONTROLLER",
                "listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
                "log.dirs=" + data.toString().replace('\\', '/'),
                "num.partitions=1",
                "offsets.topic.replication.factor=1",
                "offsets.topic.num.partitions=4",
                "transaction.state.log.replication.factor=1",
                "transaction.state.log.min.isr=1",
                "transaction.state.log.num.partitions=4",
                "group.initial.rebalance.delay.ms=0",
                "log.retention.hours=-1",
                ""));
        String cp = kafkaHome.resolve("libs") + File.separator + "*";
        run(List.of(java, "-cp", cp, "kafka.tools.StorageTool", "format", "--standalone",
                "-t", org.apache.kafka.common.Uuid.randomUuid().toString(),
                "-c", props.toString()), "format.out");
        broker = new ProcessBuilder(java, "-Xms256m", "-Xmx512m", log4j("log4j2.yaml"),
                "-Dkafka.logs.dir=" + work.resolve("logs/broker"), "-cp", cp, "kafka.Kafka", props.toString())
                .redirectErrorStream(true).redirectOutput(work.resolve("broker.out").toFile()).start();
        long end = System.currentTimeMillis() + 60_000;
        while (true) {
            if (!broker.isAlive()) throw new IllegalStateException("broker exited; see " + work.resolve("broker.out"));
            try (Socket s = new Socket("localhost", BROKER_PORT)) { break; } catch (IOException e) {
                if (System.currentTimeMillis() > end) throw new IllegalStateException("broker did not start");
                Thread.sleep(250);
            }
        }
        try (Admin a = admin()) { a.describeCluster().clusterId().get(); }
    }

    private void run(List<String> cmd, String out) throws Exception {
        Process p = new ProcessBuilder(cmd).redirectErrorStream(true).redirectOutput(work.resolve(out).toFile()).start();
        if (p.waitFor() != 0) throw new IllegalStateException(cmd + " failed; see " + work.resolve(out));
    }

    public Admin admin() {
        return Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap()));
    }

    public static String bootstrap() { return "localhost:" + BROKER_PORT; }

    public void createTopic(String name, int partitions) throws Exception {
        try (Admin a = admin()) {
            a.createTopics(List.of(new NewTopic(name, partitions, (short) 1))).all().get();
        }
    }

    /**
     * @param eosSource       exactly.once.source.support=enabled (KIP-618 transactional source tasks)
     * @param readCommitted   isolation level of every sink task's consumer
     */
    public void startWorkers(int n, boolean eosSource, boolean readCommitted, Map<String, String> extra) throws Exception {
        for (int i = 0; i < n; i++) {
            Properties p = new Properties();
            p.put("bootstrap.servers", bootstrap());
            p.put("group.id", "kclab-connect");
            p.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
            p.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
            p.put("value.converter.schemas.enable", "true");
            p.put("config.storage.topic", "kc-configs");
            p.put("offset.storage.topic", "kc-offsets");
            p.put("status.storage.topic", "kc-status");
            p.put("config.storage.replication.factor", "1");
            p.put("offset.storage.replication.factor", "1");
            p.put("status.storage.replication.factor", "1");
            p.put("offset.flush.interval.ms", "10000");
            p.put("listeners", "http://localhost:" + (WORKER_PORT_BASE + i));
            p.put("rest.advertised.host.name", "localhost");
            p.put("rest.advertised.port", Integer.toString(WORKER_PORT_BASE + i));
            p.put("plugin.path", work.resolve("plugins").toString().replace('\\', '/'));
            p.put("plugin.discovery", "service_load");
            p.put("exactly.once.source.support", eosSource ? "enabled" : "disabled");
            p.put("consumer.isolation.level", readCommitted ? "read_committed" : "read_uncommitted");
            // Default is 5 minutes: a dead worker's tasks would sit unassigned that long.
            p.put("scheduled.rebalance.max.delay.ms", "5000");
            p.put("task.shutdown.graceful.timeout.ms", "5000");
            p.putAll(extra);
            Path file = work.resolve("worker-" + i + ".properties");
            try (var w = Files.newBufferedWriter(file, StandardCharsets.UTF_8)) { p.store(w, "kclab worker " + i); }
            workers.add(new Worker(i, file));
        }
        for (Worker w : workers) w.start();
        for (Worker w : workers) {
            if (!w.awaitRest(90_000)) throw new IllegalStateException("worker " + w.index + " REST did not come up");
        }
    }

    public List<Worker> workers() { return workers; }

    public Worker anyAlive() {
        for (Worker w : workers) if (w.alive()) return w;
        throw new IllegalStateException("no live worker");
    }

    @Override public void close() throws Exception {
        for (Worker w : workers) w.kill();
        if (broker != null) { broker.destroy(); if (!broker.waitFor(15, TimeUnit.SECONDS)) broker.destroyForcibly(); broker.waitFor(); }
    }

    static void deleteTree(Path p) throws IOException {
        if (!Files.exists(p)) return;
        for (int attempt = 0; ; attempt++) {
            try (Stream<Path> s = Files.walk(p)) {
                for (Path f : s.sorted(Comparator.reverseOrder()).toList()) {
                    // KRaft marks its snapshot .checkpoint files read-only; Windows refuses to delete those.
                    f.toFile().setWritable(true);
                    Files.deleteIfExists(f);
                }
                return;
            } catch (IOException e) {
                // Windows: a just-killed JVM can hold file handles for a moment.
                if (attempt >= 20) throw e;
                try { Thread.sleep(500); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); throw e; }
            }
        }
    }
}
