package kclab.common;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class JsonTest {

    @Test
    void parsesBinanceCombinedStreamFrame() {
        String frame = "{\"stream\":\"btcusdt@aggTrade\",\"data\":{\"e\":\"aggTrade\",\"E\":1792399961700,\"s\":\"BTCUSDT\","
                + "\"a\":4093049186,\"p\":\"82827.35000000\",\"q\":\"0.03297000\",\"f\":6774163567,\"l\":6774163567,"
                + "\"T\":1792399961646,\"m\":false,\"M\":true}}";
        Map<String, Object> env = Json.parseObject(frame);
        @SuppressWarnings("unchecked") Map<String, Object> d = (Map<String, Object>) env.get("data");
        AggTrade t = AggTrade.fromBinance(null, d);
        assertEquals("BTCUSDT", t.symbol());
        assertEquals(4093049186L, t.aggId());
        assertEquals("0.03297000", t.qty(), "decimal strings must survive untouched");
        assertFalse(t.buyerMaker());
    }

    @Test
    void parsesRestArrayAndEscapes() {
        List<?> l = (List<?>) Json.parse("[ {\"a\":1,\"p\":\"1.0\",\"x\":\"a\\\"b\\u0041\"}, {\"a\":-2,\"d\":1.5e3,\"n\":null} ]");
        assertEquals(2, l.size());
        assertEquals("a\"bA", ((Map<?, ?>) l.get(0)).get("x"));
        assertEquals(-2L, ((Map<?, ?>) l.get(1)).get("a"));
        assertEquals(1500.0, ((Map<?, ?>) l.get(1)).get("d"));
        assertTrue(((Map<?, ?>) l.get(1)).containsKey("n"));
    }

    @Test
    void rejectsGarbage() {
        assertThrows(IllegalArgumentException.class, () -> Json.parse("{\"a\":"));
        assertThrows(IllegalArgumentException.class, () -> Json.parse("not json"));
        assertThrows(IllegalArgumentException.class, () -> Json.parse("{} x"));
    }
}
