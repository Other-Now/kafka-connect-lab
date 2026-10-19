package kclab.common;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.DynamicMBean;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Per-task counters exposed as one JMX MBean, {@code kclab:type=<kind>,connector=<name>,task=<id>}.
 * The worker already publishes the framework's own metrics (batch sizes, offset-commit latency);
 * these are the connector-specific ones the framework cannot know about (gaps, backfills, DLQ).
 * The Prometheus JMX exporter on the worker turns both into /metrics.
 */
public final class TaskMetrics implements DynamicMBean, AutoCloseable {
    private final Map<String, AtomicLong> counters = new LinkedHashMap<>();
    private final ObjectName name;

    public TaskMetrics(String kind, String connector, String task, String... counterNames) {
        for (String c : counterNames) counters.put(c, new AtomicLong());
        try {
            name = new ObjectName("kclab:type=" + kind + ",connector=" + ObjectName.quote(connector) + ",task=" + task);
            MBeanServer s = ManagementFactory.getPlatformMBeanServer();
            if (s.isRegistered(name)) s.unregisterMBean(name);   // task restarted in the same worker
            s.registerMBean(this, name);
        } catch (Exception e) {
            throw new IllegalStateException("cannot register metrics " + kind, e);
        }
    }

    public AtomicLong get(String counter) {
        AtomicLong a = counters.get(counter);
        if (a == null) throw new IllegalArgumentException("unknown counter " + counter);
        return a;
    }

    public void add(String counter, long delta) { get(counter).addAndGet(delta); }
    public void set(String counter, long value) { get(counter).set(value); }

    @Override public void close() {
        try { ManagementFactory.getPlatformMBeanServer().unregisterMBean(name); } catch (Exception ignored) { }
    }

    @Override public Object getAttribute(String attribute) { return get(attribute).get(); }
    @Override public void setAttribute(Attribute attribute) { throw new UnsupportedOperationException(); }

    @Override public AttributeList getAttributes(String[] attributes) {
        AttributeList l = new AttributeList();
        for (String a : attributes) if (counters.containsKey(a)) l.add(new Attribute(a, counters.get(a).get()));
        return l;
    }

    @Override public AttributeList setAttributes(AttributeList attributes) { return new AttributeList(); }
    @Override public Object invoke(String actionName, Object[] params, String[] signature) { throw new UnsupportedOperationException(); }

    @Override public MBeanInfo getMBeanInfo() {
        MBeanAttributeInfo[] attrs = counters.keySet().stream()
                .map(k -> new MBeanAttributeInfo(k, "long", k, true, false, false))
                .toArray(MBeanAttributeInfo[]::new);
        return new MBeanInfo(getClass().getName(), "kclab task metrics", attrs, null, null, null);
    }
}
