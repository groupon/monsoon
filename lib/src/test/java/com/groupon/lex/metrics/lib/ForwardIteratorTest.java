package com.groupon.lex.metrics.lib;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Stream;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import org.junit.Test;

public class ForwardIteratorTest {
    @Test
    public void iteration() {
        Iterator<Integer> iter = new ForwardIterator<>(Stream.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10).iterator());
        List<Integer> seen = new ArrayList<>();
        iter.forEachRemaining(seen::add);

        assertEquals(Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10), seen);
    }

    @Test
    public void empty_iteration() {
        Iterator<Integer> iter = new ForwardIterator<>(Collections.emptyIterator());

        assertFalse(iter.hasNext());
    }

    @Test(expected = NoSuchElementException.class)
    public void empty_iteration_next_fails() {
        Iterator<Integer> iter = new ForwardIterator<>(Collections.emptyIterator());

        iter.next();
    }

    @Test
    public void repeatable() {
        ForwardIterator<Integer> iter = new ForwardIterator<>(Stream.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10).iterator());
        ForwardIterator<Integer> copy = iter.clone();

        List<Integer> iter_seen = new ArrayList<>();
        List<Integer> copy_seen = new ArrayList<>();
        iter.forEachRemaining(iter_seen::add);
        copy.forEachRemaining(copy_seen::add);

        assertEquals(iter_seen, copy_seen);
    }
}
