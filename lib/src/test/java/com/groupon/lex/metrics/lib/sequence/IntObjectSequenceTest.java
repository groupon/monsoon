package com.groupon.lex.metrics.lib.sequence;

import org.hamcrest.Matchers;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

public class IntObjectSequenceTest {
    private final ObjectSequence<String> seq = new ForwardSequence(5, 7).map(Integer::toString, true, true, true);

    @Test
    public void constructor() {
        assertThat(seq, Matchers.instanceOf(IntObjectSequence.class));
        assertEquals(2, seq.size());
        assertEquals("5", seq.get(0));
        assertEquals("6", seq.get(1));
        assertEquals("5", seq.first());
        assertEquals("6", seq.last());
        assertFalse(seq.isEmpty());
        assertTrue(seq.isSorted());
        assertTrue(seq.isNonnull());
        assertTrue(seq.isDistinct());

        assertThat(seq, Matchers.contains("5", "6"));
        assertArrayEquals(new String[]{"5", "6"}, seq.toArray(new String[0]));
        assertArrayEquals(new String[]{"5", "6"}, seq.stream().toArray(String[]::new));
    }

    @Test
    public void map() {
        assertThat(seq.map(str -> "(" + str + ")", true, true, true), Matchers.contains("(5)", "(6)"));
    }
}
