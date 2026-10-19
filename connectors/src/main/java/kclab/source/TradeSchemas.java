package kclab.source;

import kclab.common.AggTrade;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.Map;

public final class TradeSchemas {
    public static final String PARTITION_SYMBOL = "symbol";
    public static final String OFFSET_AGG_ID = "aggId";

    public static final Schema VALUE = SchemaBuilder.struct().name("kclab.AggTrade").version(1)
            .field("symbol", Schema.STRING_SCHEMA)
            .field("agg_id", Schema.INT64_SCHEMA)
            .field("price", Schema.STRING_SCHEMA)
            .field("qty", Schema.STRING_SCHEMA)
            .field("first_trade_id", Schema.INT64_SCHEMA)
            .field("last_trade_id", Schema.INT64_SCHEMA)
            .field("trade_time", Schema.INT64_SCHEMA)
            .field("buyer_maker", Schema.BOOLEAN_SCHEMA)
            .build();

    private TradeSchemas() { }

    public static Map<String, String> sourcePartition(String symbol) {
        return Map.of(PARTITION_SYMBOL, symbol);
    }

    /** Key = symbol, so one symbol's trades land in one Kafka partition, in order. */
    public static SourceRecord record(String topic, AggTrade t) {
        Struct v = new Struct(VALUE)
                .put("symbol", t.symbol())
                .put("agg_id", t.aggId())
                .put("price", t.price())
                .put("qty", t.qty())
                .put("first_trade_id", t.firstTradeId())
                .put("last_trade_id", t.lastTradeId())
                .put("trade_time", t.tradeTime())
                .put("buyer_maker", t.buyerMaker());
        return new SourceRecord(sourcePartition(t.symbol()), Map.of(OFFSET_AGG_ID, t.aggId()),
                topic, null, Schema.STRING_SCHEMA, t.symbol(), VALUE, v, t.tradeTime());
    }
}
