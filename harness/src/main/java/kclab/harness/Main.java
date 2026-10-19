package kclab.harness;

import java.util.Arrays;
import java.util.List;

public final class Main {
    public static void main(String[] argv) throws Exception {
        String cmd = argv.length == 0 ? "help" : argv[0];
        Args a = new Args(argv, 1);
        switch (cmd) {
            case "chaos" -> { Chaos.main(a); System.exit(0); }
            case "bench" -> { Bench.main(a); System.exit(0); }
            case "live" -> { Live.main(a); System.exit(0); }
            case "mock-exchange" -> {
                List<String> symbols = Arrays.asList(a.str("symbols", "BTCUSDT,ETHUSDT").split(","));
                try (MockExchange m = new MockExchange(symbols, a.integer("rest-port", 18900), a.integer("ws-port", 18901),
                        a.dbl("rate", 100), a.integer("preload", 0), a.integer("poison-every", 1000), a.dbl("ws-drop", 0))) {
                    System.out.println("mock exchange: REST http://127.0.0.1:" + a.integer("rest-port", 18900)
                            + "  WS ws://127.0.0.1:" + a.integer("ws-port", 18901) + "/stream?streams=btcusdt@aggTrade");
                    while (true) { Thread.sleep(5000); System.out.println(m.stats()); }
                }
            }
            default -> System.out.println("""
                    usage: java -jar harness/target/harness.jar <command> [--flags]
                      chaos [scenario...]   alos-unsafe alos-offsets alos-upsert eos-offsets eos-offsets-uncommitted
                      bench                 throughput of source backfill and each sink mode
                      live                  real Binance feed, worker kills, contiguity check
                      mock-exchange         run the venue alone""");
        }
    }
}
