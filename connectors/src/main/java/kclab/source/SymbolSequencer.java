package kclab.source;

/**
 * Per-symbol sequencing state. The WebSocket is fast but lossy (disconnects, missed frames);
 * REST {@code aggTrades?fromId=} is slow but authoritative and replayable. This class decides,
 * for each live trade, whether to emit it, drop it as already seen, or stop and backfill.
 *
 * While {@code catchingUp}, live trades for the symbol are ignored: REST will return them anyway,
 * and catch-up ends only when a REST page comes back short (we reached the head). After that,
 * any live trade beyond {@code next} means we missed something and catch-up starts again.
 */
public final class SymbolSequencer {
    public enum Decision { EMIT, DUPLICATE, GAP, IGNORED_CATCHING_UP }

    private long next;
    private boolean catchingUp = true;   // always verify against REST on (re)start

    public SymbolSequencer(long next) { this.next = next; }

    public long next() { return next; }
    public boolean catchingUp() { return catchingUp; }

    public Decision onLive(long aggId) {
        if (catchingUp) return Decision.IGNORED_CATCHING_UP;
        if (aggId < next) return Decision.DUPLICATE;
        if (aggId == next) { next++; return Decision.EMIT; }
        catchingUp = true;
        return Decision.GAP;
    }

    /** A REST trade during catch-up; REST pages start at {@code next}, so only dups are skipped. */
    public boolean onBackfill(long aggId) {
        if (aggId < next) return false;
        if (aggId != next) {
            // REST itself skipped ids. Binance aggTrade ids are dense, so this means the venue is
            // inconsistent; accept and move on rather than spin (and count it via the caller).
            next = aggId + 1;
            return true;
        }
        next++;
        return true;
    }

    public void caughtUp() { catchingUp = false; }
    public void requireCatchUp() { catchingUp = true; }
}
