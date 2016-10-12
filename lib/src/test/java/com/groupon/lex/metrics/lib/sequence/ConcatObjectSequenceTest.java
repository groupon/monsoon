package com.groupon.lex.metrics.lib.sequence;

import java.util.Arrays;
import java.util.stream.Collectors;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import org.junit.Test;

public class ConcatObjectSequenceTest {
    private final ObjectSequence<String> seq = ObjectSequence.concat(new ObjectSequence[]{
        ObjectSequence.of(true, true, true, "zero"),
        ObjectSequence.of(true, true, true, "one")}, true, true);

    @Test
    public void constructor() {
        assertFalse(seq.isEmpty());
        assertEquals(2, seq.size());
        assertThat(seq, contains("zero", "one"));
        assertEquals(Arrays.asList("zero", "one"), seq.stream().collect(Collectors.toList()));

        assertEquals("zero", seq.get(0));
        assertEquals("one", seq.get(1));
    }
}
