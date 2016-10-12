package com.groupon.lex.metrics.lib.sequence;

import static java.util.Collections.EMPTY_LIST;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;
import org.hamcrest.Matchers;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

public class EmptyObjectSequenceTest {
    private ObjectSequence<Integer> empty = ObjectSequence.empty();

    @Test
    public void constructor() {
        assertTrue(empty.isSorted());
        assertTrue(empty.isNonnull());
        assertTrue(empty.isDistinct());
        assertTrue(empty.isEmpty());
        assertEquals(0, empty.size());
        assertThat(empty, Matchers.emptyIterable());
        assertEquals(EMPTY_LIST, empty.stream().collect(Collectors.toList()));
        assertSame(empty, empty.reverse());
    }

    @Test(expected = NoSuchElementException.class)
    public void first() {
        empty.first();
    }

    @Test(expected = NoSuchElementException.class)
    public void last() {
        empty.last();
    }

    @Test(expected = NoSuchElementException.class)
    public void get() {
        empty.get(0);
    }
}
