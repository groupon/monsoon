package com.groupon.lex.metrics.lib;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

public class BufferedIteratorTest {
    @Test(timeout = 8000)
    public void empty_iterator_test() {
        Iterator<?> iterator = blockingIterator(Stream.empty().iterator());

        assertFalse(iterator.hasNext());
    }

    @Test(expected = NoSuchElementException.class, timeout = 8000)
    public void empty_iterator_next_test() {
        blockingIterator(Stream.empty().iterator()).next();
    }

    @Test(timeout = 8000)
    public void nonempty_iterator_test() {
        Iterator<?> iterator = blockingIterator(Stream.of("foobar").iterator());

        assertTrue(iterator.hasNext());
        assertEquals("foobar", iterator.next());
        assertFalse(iterator.hasNext());
    }

    @Test(timeout = 8000)
    public void iteration() {
        List<Integer> visited = blockingStream(Stream.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20))
                .collect(Collectors.toList());

        assertEquals(Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20), visited);
    }

    @Test(timeout = 80000)
    public void blocking_iteration() {
        Stream<Integer> stream = blockingStream(Stream.generate(new Supplier<Integer>() {
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

    private static <T> Iterator<T> blockingIterator(Iterator<T> iter) {
        return new BlockingIterator<>(new BufferedIterator<>(iter));
    }

    private static <T> Stream<T> blockingStream(Stream<T> stream) {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(blockingIterator(stream.iterator()), Spliterator.ORDERED), false);
    }

    /**
     * It's a bad idea to do this, as buffering of already buffered computations
     * will lead to potential deadlock if all executor threads are committed to
     * iteration.
     */
    @RequiredArgsConstructor
    private static class BlockingIterator<T> implements Iterator<T> {
        @NonNull
        private final BufferedIterator<T> iter;

        @Override
        @SneakyThrows
        public boolean hasNext() {
            iter.waitAvail();
            assert (iter.nextAvail() || iter.atEnd());
            return !iter.atEnd();
        }

        @Override
        @SneakyThrows
        public T next() {
            iter.waitAvail();
            if (iter.atEnd())
                throw new NoSuchElementException();
            assert (iter.nextAvail());
            return iter.next();
        }
    }
}
