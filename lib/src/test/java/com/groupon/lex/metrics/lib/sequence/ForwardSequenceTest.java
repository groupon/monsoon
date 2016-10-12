package com.groupon.lex.metrics.lib.sequence;

import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.stream.StreamSupport;
import org.hamcrest.Matchers;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

public class ForwardSequenceTest {
    private final ForwardSequence seq = new ForwardSequence(5, 7);

    @Test
    public void constructor() {
        assertTrue(new ForwardSequence(5, 5).isEmpty());
        assertThat(new ForwardSequence(5, 5), Matchers.emptyIterable());

        assertEquals(5, seq.getBegin());
        assertEquals(7, seq.getEnd());
        assertEquals(2, seq.size());
        assertFalse(seq.isEmpty());
        assertThat(seq, Matchers.contains(5, 6));
        assertEquals(5, seq.get(0));
        assertEquals(6, seq.get(1));

        Spliterator.OfInt spliterator = seq.spliterator();
        assertFalse(spliterator.hasCharacteristics(Spliterator.CONCURRENT));
        assertTrue(spliterator.hasCharacteristics(Spliterator.DISTINCT));
        assertTrue(spliterator.hasCharacteristics(Spliterator.IMMUTABLE));
        assertTrue(spliterator.hasCharacteristics(Spliterator.NONNULL));
        assertTrue(spliterator.hasCharacteristics(Spliterator.ORDERED));
        assertTrue(spliterator.hasCharacteristics(Spliterator.SIZED));
        assertTrue(spliterator.hasCharacteristics(Spliterator.SORTED));
        assertTrue(spliterator.hasCharacteristics(Spliterator.SUBSIZED));
        assertNull(spliterator.getComparator());
        assertEquals(2, spliterator.estimateSize());

        Spliterator.OfInt splittedSpliterator = spliterator.trySplit();
        assertEquals(spliterator.characteristics(), splittedSpliterator.characteristics());
        assertTrue(splittedSpliterator.estimateSize() == 1);
        assertTrue(spliterator.estimateSize() == 1);
        assertNull(splittedSpliterator.trySplit());
        assertNull(spliterator.trySplit());

        assertArrayEquals(new int[]{5, 6}, seq.stream().toArray());
        assertArrayEquals(new int[]{5}, StreamSupport.intStream(splittedSpliterator, false).toArray());
        assertArrayEquals(new int[]{6}, StreamSupport.intStream(spliterator, false).toArray());
    }

    @Test
    public void limit() {
        assertSame(seq, seq.limit(2));

        ForwardSequence limited = seq.limit(1);
        assertEquals(5, limited.getBegin());
        assertEquals(6, limited.getEnd());
        assertEquals(1, limited.size());
        assertArrayEquals(new int[]{5}, limited.stream().toArray());
    }

    @Test
    public void skip() {
        assertSame(seq, seq.skip(0));

        ForwardSequence skipped = seq.skip(1);
        assertEquals(6, skipped.getBegin());
        assertEquals(7, skipped.getEnd());
        assertEquals(1, skipped.size());
        assertArrayEquals(new int[]{6}, skipped.stream().toArray());
    }

    @Test(expected = NoSuchElementException.class)
    public void badGet() {
        seq.get(10);
    }

    @Test
    public void reverse() {
        assertEquals(new ReverseSequence(5, 7), seq.reverse());
    }
}
