package kclab.source;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class TradeSourceConnectorTest {

    @Test
    void symbolsAreSplitRoundRobinAndTasksCappedBySymbolCount() {
        TradeSourceConnector c = new TradeSourceConnector();
        c.start(Map.of("name", "src", "symbols", "A,B,C,D,E"));
        List<Map<String, String>> t = c.taskConfigs(2);
        assertEquals(2, t.size());
        assertEquals("A,C,E", t.get(0).get(TradeSourceConfig.TASK_SYMBOLS));
        assertEquals("B,D", t.get(1).get(TradeSourceConfig.TASK_SYMBOLS));
        assertEquals(5, c.taskConfigs(10).size(), "never more tasks than symbols");
    }

    @Test
    void rejectsBadConfig() {
        assertThrows(Exception.class, () -> new TradeSourceConnector().start(Map.of("symbols", "A", "start.from", "yesterday")));
    }
}
