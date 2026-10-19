package kclab.sink;

import kclab.common.AggTrade;
import kclab.source.TradeSchemas;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class TradeRowTest {

    static SinkRecord sink(AggTrade t, long offset) {
        SourceRecord s = TradeSchemas.record("trades", t);
        return new SinkRecord("trades", 0, s.keySchema(), s.key(), s.valueSchema(), s.value(), offset);
    }

    static AggTrade trade(long id, String price, String qty) {
        return new AggTrade("BTCUSDT", id, price, qty, id * 2, id * 2 + 1, 1_700_000_000_000L + id, id % 2 == 0);
    }

    @Test
    void validStructRoundTrips() {
        TradeRow r = TradeRow.from(sink(trade(7, "82827.35000000", "0.00009000"), 0));
        assertEquals(7, r.aggId());
        assertEquals(new BigDecimal("0.00009000"), r.qty());
        assertEquals(0, new BigDecimal("82827.35").compareTo(r.price()));
    }

    @Test
    void schemalessMapIsAccepted() {
        Map<String, Object> m = new HashMap<>();
        m.put("symbol", "ETHUSDT"); m.put("agg_id", 3L); m.put("price", "1.5"); m.put("qty", "2");
        m.put("first_trade_id", 1L); m.put("last_trade_id", 1L); m.put("trade_time", 5L); m.put("buyer_maker", true);
        TradeRow r = TradeRow.from(new SinkRecord("t", 0, null, null, null, m, 0));
        assertEquals("ETHUSDT", r.symbol());
    }

    @Test
    void poisonRecordsAreRejected() {
        assertThrows(TradeRow.InvalidRecord.class, () -> TradeRow.from(sink(trade(1, "100", "0"), 0)));
        assertThrows(TradeRow.InvalidRecord.class, () -> TradeRow.from(sink(trade(1, "-1", "1"), 0)));
        assertThrows(TradeRow.InvalidRecord.class, () -> TradeRow.from(sink(trade(1, "NaN", "1"), 0)));
        assertThrows(TradeRow.InvalidRecord.class, () -> TradeRow.from(new SinkRecord("t", 0, null, null, null, null, 0)));
        Struct bad = (Struct) sink(trade(1, "1", "1"), 0).value();
        bad.put("last_trade_id", 0L);
        assertThrows(TradeRow.InvalidRecord.class,
                () -> TradeRow.from(new SinkRecord("t", 0, null, null, bad.schema(), bad, 0)));
        assertThrows(TradeRow.InvalidRecord.class,
                () -> TradeRow.from(new SinkRecord("t", 0, null, null, null, Map.of("symbol", "X"), 0)));
    }
}
