package com.groupon.lex.metrics.lib.sequence;

import java.util.Comparator;
import java.util.stream.Collectors;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

public class SortedObjectSequenceTest {
    private final SortedObjectSequence<Integer> seq = new SortedObjectSequence(ObjectSequence.of(false, true, true, new Integer(9), 7, 1, 5, 3), Comparator.naturalOrder());

    @Test
    public void metadata() {
        assertTrue(seq.isSorted());
        assertTrue(seq.isNonnull());
        assertTrue(seq.isDistinct());
        assertSame(Comparator.naturalOrder(), seq.getComparator());
    }

    @Test
    public void walk() {
        assertThat(seq, contains(new Integer(1), 3, 5, 7, 9));
    }

    @Test
    public void index() {
        assertEquals(new Integer(3), seq.get(1));
        assertEquals(new Integer(9), seq.get(4));
    }

    @Test
    public void stream() {
        assertThat(seq.stream().collect(Collectors.toList()), contains(new Integer(1), 3, 5, 7, 9));
    }

    @Test
    public void reverse() {
        SortedObjectSequence<Integer> reversed = seq.reverse();

        assertTrue(reversed.isSorted());
        assertTrue(reversed.isNonnull());
        assertTrue(reversed.isDistinct());
        assertSame(Comparator.reverseOrder(), reversed.getComparator());
        assertThat(reversed, contains(new Integer(9), 7, 5, 3, 1));
        assertEquals(new Integer(7), reversed.get(1));
    }
}
