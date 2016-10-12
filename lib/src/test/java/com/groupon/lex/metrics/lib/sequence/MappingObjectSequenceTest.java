package com.groupon.lex.metrics.lib.sequence;

import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import org.junit.Test;

public class MappingObjectSequenceTest {
    private static final ObjectSequence<String> INPUT = ObjectSequence.of(true, true, true, "5", "6");
    private final MappingObjectSequence<String, Integer> seq = new MappingObjectSequence<>(INPUT, Integer::valueOf, true, true, true);

    @Test
    public void constructor() {
        assertFalse(seq.isEmpty());
        assertEquals(2, seq.size());
        assertThat(seq, contains(new Integer(5), new Integer(6)));
        assertEquals(Arrays.<Integer>asList(5, 6), seq.stream().collect(Collectors.toList()));
        assertEquals(new Integer(5), seq.get(0));
        assertEquals(new Integer(6), seq.get(1));
    }

    @Test
    public void reverse() {
        assertThat(seq.reverse(), contains(new Integer(6), new Integer(5)));
    }

    @Test(expected = NoSuchElementException.class)
    public void badGet() {
        seq.get(5);
    }

    @Test
    public void map() {
        assertThat(seq.map(x -> x + 1, true, true, true), contains(new Integer(6), new Integer(7)));
    }
}
