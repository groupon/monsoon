package com.groupon.lex.metrics.lib.sequence;

import java.util.Comparator;
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

public class ReverseSequenceTest {
    private final ReverseSequence seq = new ReverseSequence(5, 7);

    @Test
    public void constructor() {
        assertTrue(new ReverseSequence(5, 5).isEmpty());
        assertThat(new ReverseSequence(5, 5), Matchers.emptyIterable());

        assertEquals(5, seq.getBegin());
        assertEquals(7, seq.getEnd());
        assertEquals(2, seq.size());
        assertFalse(seq.isEmpty());
        assertThat(seq, Matchers.contains(6, 5));
        assertEquals(6, seq.get(0));
        assertEquals(5, seq.get(1));

        Spliterator.OfInt spliterator = seq.spliterator();
        assertFalse(spliterator.hasCharacteristics(Spliterator.CONCURRENT));
        assertTrue(spliterator.hasCharacteristics(Spliterator.DISTINCT));
        assertTrue(spliterator.hasCharacteristics(Spliterator.IMMUTABLE));
        assertTrue(spliterator.hasCharacteristics(Spliterator.NONNULL));
        assertTrue(spliterator.hasCharacteristics(Spliterator.ORDERED));
        assertTrue(spliterator.hasCharacteristics(Spliterator.SIZED));
        assertTrue(spliterator.hasCharacteristics(Spliterator.SORTED));
        assertTrue(spliterator.hasCharacteristics(Spliterator.SUBSIZED));
        assertSame(Comparator.reverseOrder(), spliterator.getComparator());
        assertEquals(2, spliterator.estimateSize());

        Spliterator.OfInt splittedSpliterator = spliterator.trySplit();
        assertEquals(spliterator.characteristics(), splittedSpliterator.characteristics());
        assertTrue(splittedSpliterator.estimateSize() == 1);
        assertTrue(spliterator.estimateSize() == 1);
        assertNull(splittedSpliterator.trySplit());
        assertNull(spliterator.trySplit());

        assertArrayEquals(new int[]{6, 5}, seq.stream().toArray());
        assertArrayEquals(new int[]{6}, StreamSupport.intStream(splittedSpliterator, false).toArray());
        assertArrayEquals(new int[]{5}, StreamSupport.intStream(spliterator, false).toArray());
    }

    @Test
    public void limit() {
        assertSame(seq, seq.limit(2));

        ReverseSequence limited = seq.limit(1);
        assertEquals(6, limited.getBegin());
        assertEquals(7, limited.getEnd());
        assertEquals(1, limited.size());
        assertArrayEquals(new int[]{6}, limited.stream().toArray());
    }

    @Test
    public void skip() {
        assertSame(seq, seq.skip(0));

        ReverseSequence skipped = seq.skip(1);
        assertEquals(5, skipped.getBegin());
        assertEquals(6, skipped.getEnd());
        assertEquals(1, skipped.size());
        assertArrayEquals(new int[]{5}, skipped.stream().toArray());
    }

    @Test(expected = NoSuchElementException.class)
    public void badGet() {
        seq.get(10);
    }

    @Test
    public void reverse() {
        assertEquals(new ForwardSequence(5, 7), seq.reverse());
    }
}
