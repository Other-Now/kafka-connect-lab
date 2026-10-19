package kclab.harness;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** --key value flags plus positional arguments. */
final class Args {
    final Map<String, String> flags = new HashMap<>();
    final List<String> positional = new ArrayList<>();

    Args(String[] argv, int from) {
        for (int i = from; i < argv.length; i++) {
            if (argv[i].startsWith("--")) {
                String k = argv[i].substring(2);
                int eq = k.indexOf('=');
                if (eq >= 0) flags.put(k.substring(0, eq), k.substring(eq + 1));
                else flags.put(k, i + 1 < argv.length ? argv[++i] : "true");
            } else {
                positional.add(argv[i]);
            }
        }
    }

    String str(String k, String def) { return flags.getOrDefault(k, def); }
    int integer(String k, int def) { return flags.containsKey(k) ? Integer.parseInt(flags.get(k)) : def; }
    double dbl(String k, double def) { return flags.containsKey(k) ? Double.parseDouble(flags.get(k)) : def; }
}
