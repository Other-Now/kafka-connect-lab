package kclab.common;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Minimal JSON reader for exchange payloads. Objects become LinkedHashMap, arrays List,
 * numbers Long (or Double if they have a fraction/exponent), plus String/Boolean/null.
 *
 * The plugin deliberately carries no Jackson: the worker has its own copy, and a plugin that
 * relies on whatever the parent classloader happens to provide breaks on worker upgrades.
 */
public final class Json {
    private final String s;
    private int i;

    private Json(String s) { this.s = s; }

    public static Object parse(String text) {
        Json p = new Json(text);
        p.ws();
        Object v = p.value();
        p.ws();
        if (p.i != p.s.length()) throw p.err("trailing characters");
        return v;
    }

    @SuppressWarnings("unchecked")
    public static Map<String, Object> parseObject(String text) {
        Object v = parse(text);
        if (!(v instanceof Map)) throw new IllegalArgumentException("expected JSON object");
        return (Map<String, Object>) v;
    }

    private Object value() {
        if (i >= s.length()) throw err("unexpected end");
        char c = s.charAt(i);
        switch (c) {
            case '{': return object();
            case '[': return array();
            case '"': return string();
            case 't': lit("true"); return Boolean.TRUE;
            case 'f': lit("false"); return Boolean.FALSE;
            case 'n': lit("null"); return null;
            default: return number();
        }
    }

    private Map<String, Object> object() {
        Map<String, Object> m = new LinkedHashMap<>();
        i++; ws();
        if (peek() == '}') { i++; return m; }
        while (true) {
            ws();
            String k = string();
            ws(); expect(':'); ws();
            m.put(k, value());
            ws();
            char c = next();
            if (c == '}') return m;
            if (c != ',') throw err("expected , or }");
        }
    }

    private List<Object> array() {
        List<Object> l = new ArrayList<>();
        i++; ws();
        if (peek() == ']') { i++; return l; }
        while (true) {
            ws();
            l.add(value());
            ws();
            char c = next();
            if (c == ']') return l;
            if (c != ',') throw err("expected , or ]");
        }
    }

    private String string() {
        expect('"');
        StringBuilder b = null;
        int start = i;
        while (true) {
            if (i >= s.length()) throw err("unterminated string");
            char c = s.charAt(i++);
            if (c == '"') return b == null ? s.substring(start, i - 1) : b.toString();
            if (c == '\\') {
                if (b == null) b = new StringBuilder(s.substring(start, i - 1));
                char e = next();
                switch (e) {
                    case 'n' -> b.append('\n');
                    case 't' -> b.append('\t');
                    case 'r' -> b.append('\r');
                    case 'b' -> b.append('\b');
                    case 'f' -> b.append('\f');
                    case 'u' -> { b.append((char) Integer.parseInt(s.substring(i, i + 4), 16)); i += 4; }
                    default -> b.append(e);
                }
            } else if (b != null) {
                b.append(c);
            }
        }
    }

    private Object number() {
        int start = i;
        boolean frac = false;
        while (i < s.length()) {
            char c = s.charAt(i);
            if ((c >= '0' && c <= '9') || c == '-' || c == '+') { i++; }
            else if (c == '.' || c == 'e' || c == 'E') { frac = true; i++; }
            else break;
        }
        if (start == i) throw err("unexpected character");
        String n = s.substring(start, i);
        return frac ? (Object) Double.parseDouble(n) : (Object) Long.parseLong(n);
    }

    private void lit(String w) {
        if (!s.startsWith(w, i)) throw err("expected " + w);
        i += w.length();
    }

    private void ws() { while (i < s.length() && Character.isWhitespace(s.charAt(i))) i++; }
    private char peek() { return i < s.length() ? s.charAt(i) : '\0'; }
    private char next() { if (i >= s.length()) throw err("unexpected end"); return s.charAt(i++); }
    private void expect(char c) { if (next() != c) throw err("expected " + c); }
    private IllegalArgumentException err(String m) { return new IllegalArgumentException("json: " + m + " at " + i); }
}
