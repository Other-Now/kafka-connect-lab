package kclab.source;

import org.junit.jupiter.api.Test;

import static kclab.source.SymbolSequencer.Decision.*;
import static org.junit.jupiter.api.Assertions.*;

class SymbolSequencerTest {

    @Test
    void startsCatchingUpAndIgnoresLiveUntilRestReachesHead() {
        SymbolSequencer s = new SymbolSequencer(100);
        assertTrue(s.catchingUp());
        assertEquals(IGNORED_CATCHING_UP, s.onLive(105));
        assertTrue(s.onBackfill(100));
        assertTrue(s.onBackfill(101));
        s.caughtUp();
        assertEquals(102, s.next());
        assertEquals(EMIT, s.onLive(102));
    }

    @Test
    void duplicateLiveTradesAreDropped() {
        SymbolSequencer s = new SymbolSequencer(10);
        s.caughtUp();
        assertEquals(EMIT, s.onLive(10));
        assertEquals(DUPLICATE, s.onLive(10));
        assertEquals(DUPLICATE, s.onLive(3));
        assertEquals(EMIT, s.onLive(11));
    }

    @Test
    void gapSwitchesToCatchUpWithoutAdvancing() {
        SymbolSequencer s = new SymbolSequencer(10);
        s.caughtUp();
        assertEquals(EMIT, s.onLive(10));
        assertEquals(GAP, s.onLive(14));
        assertTrue(s.catchingUp());
        assertEquals(11, s.next(), "the missing ids 11..13 must still be fetched");
        assertEquals(IGNORED_CATCHING_UP, s.onLive(15));
    }

    @Test
    void backfillSkipsAlreadySeenIds() {
        SymbolSequencer s = new SymbolSequencer(10);
        assertFalse(s.onBackfill(9));
        assertTrue(s.onBackfill(10));
        assertFalse(s.onBackfill(10));
        assertEquals(11, s.next());
    }
}
