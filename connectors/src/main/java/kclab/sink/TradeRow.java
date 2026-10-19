package kclab.sink;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

import java.math.BigDecimal;
import java.util.Map;

/** A validated row. Anything that fails {@link #from} goes to the dead-letter queue, not the table. */
record TradeRow(String symbol, long aggId, BigDecimal price, BigDecimal qty, long firstTradeId,
                long lastTradeId, long tradeTime, boolean buyerMaker) {

    static final class InvalidRecord extends RuntimeException {
        InvalidRecord(String msg) { super(msg); }
    }

    static TradeRow from(SinkRecord r) {
        Object v = r.value();
        if (v == null) throw new InvalidRecord("null value (tombstone)");
        if (v instanceof Struct s) {
            return build(s.getString("symbol"), s.getInt64("agg_id"), s.getString("price"), s.getString("qty"),
                    s.getInt64("first_trade_id"), s.getInt64("last_trade_id"), s.getInt64("trade_time"),
                    Boolean.TRUE.equals(s.getBoolean("buyer_maker")));
        }
        if (v instanceof Map<?, ?> m) {   // schemaless JSON
            try {
                return build((String) m.get("symbol"), ((Number) m.get("agg_id")).longValue(), (String) m.get("price"),
                        (String) m.get("qty"), ((Number) m.get("first_trade_id")).longValue(),
                        ((Number) m.get("last_trade_id")).longValue(), ((Number) m.get("trade_time")).longValue(),
                        Boolean.TRUE.equals(m.get("buyer_maker")));
            } catch (ClassCastException | NullPointerException e) {
                throw new InvalidRecord("schemaless value missing/mistyped field: " + e);
            }
        }
        throw new InvalidRecord("unexpected value type " + v.getClass().getName());
    }

    private static TradeRow build(String symbol, long aggId, String price, String qty, long first, long last,
                                  long time, boolean buyerMaker) {
        if (symbol == null || symbol.isEmpty()) throw new InvalidRecord("empty symbol");
        if (aggId < 0) throw new InvalidRecord("negative agg_id " + aggId);
        BigDecimal p = positive("price", price);
        BigDecimal q = positive("qty", qty);
        if (last < first) throw new InvalidRecord("last_trade_id < first_trade_id");
        return new TradeRow(symbol, aggId, p, q, first, last, time, buyerMaker);
    }

    private static BigDecimal positive(String field, String s) {
        BigDecimal d;
        try {
            d = new BigDecimal(s);
        } catch (NumberFormatException | NullPointerException e) {
            throw new InvalidRecord(field + " is not a decimal: " + s);
        }
        if (d.signum() <= 0) throw new InvalidRecord(field + " must be > 0, got " + s);
        return d;
    }
}
