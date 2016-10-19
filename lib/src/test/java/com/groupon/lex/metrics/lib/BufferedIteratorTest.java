package com.groupon.lex.metrics.lib;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

public class BufferedIteratorTest {
    @Test(timeout = 8000)
    public void empty_iterator_test() {
        Iterator<?> iterator = BufferedIterator.iterator(ForkJoinPool.commonPool(), Stream.empty().iterator());

        assertFalse(iterator.hasNext());
    }

    @Test(expected = NoSuchElementException.class, timeout = 8000)
    public void empty_iterator_next_test() {
        BufferedIterator.iterator(ForkJoinPool.commonPool(), Stream.empty().iterator()).next();
    }

    @Test(timeout = 8000)
    public void nonempty_iterator_test() {
        Iterator<?> iterator = BufferedIterator.iterator(ForkJoinPool.commonPool(), Stream.of("foobar").iterator());

        assertTrue(iterator.hasNext());
        assertEquals("foobar", iterator.next());
        assertFalse(iterator.hasNext());
    }

    @Test(timeout = 8000)
    public void iteration() {
        List<Integer> visited = BufferedIterator.stream(ForkJoinPool.commonPool(), Stream.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20))
                .collect(Collectors.toList());

        assertEquals(Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20), visited);
    }

    @Test(timeout = 80000)
    public void blocking_iteration() {
        Stream<Integer> stream = BufferedIterator.stream(ForkJoinPool.commonPool(), Stream.generate(new Supplier<Integer>() {
            private int i = 0;

            @Override
            public Integer get() {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ex) {
                    Logger.getLogger(BufferedIteratorTest.class.getName()).log(Level.SEVERE, "interrupted", ex);
                }
                return i++;
            }
        }));

        assertEquals(Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9), stream.limit(10).collect(Collectors.toList()));
    }
}
