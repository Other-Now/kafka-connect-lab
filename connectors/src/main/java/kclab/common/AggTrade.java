package kclab.common;

import java.util.Map;

/**
 * One Binance aggregate trade. {@code aggId} is dense and increasing per symbol, which is what
 * makes gap detection possible: the next id we expect is always last + 1.
 *
 * Price and quantity stay as the exchange's decimal strings all the way to Postgres NUMERIC;
 * a double would silently round "0.00009000".
 */
public record AggTrade(String symbol, long aggId, String price, String qty,
                       long firstTradeId, long lastTradeId, long tradeTime, boolean buyerMaker) {

    /** Same field names in the REST aggTrades array and the WebSocket aggTrade event. */
    public static AggTrade fromBinance(String symbol, Map<String, Object> m) {
        String s = m.containsKey("s") ? (String) m.get("s") : symbol;
        return new AggTrade(s, num(m, "a"), (String) m.get("p"), (String) m.get("q"),
                num(m, "f"), num(m, "l"), num(m, "T"), Boolean.TRUE.equals(m.get("m")));
    }

    private static long num(Map<String, Object> m, String k) {
        Object v = m.get(k);
        if (v == null) throw new IllegalArgumentException("missing field " + k);
        return ((Number) v).longValue();
    }
}
