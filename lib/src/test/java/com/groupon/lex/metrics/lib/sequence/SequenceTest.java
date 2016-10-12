package com.groupon.lex.metrics.lib.sequence;

import static org.junit.Assert.assertEquals;
import org.junit.Test;

public class SequenceTest {
    private Sequence seq = new ForwardSequence(5, 10);

    @Test
    public void equalRange() {
        assertEquals(new EqualRange(0, 1), seq.equalRange(i -> Integer.compare(i, 5)));  // First element.
        assertEquals(new EqualRange(4, 5), seq.equalRange(i -> Integer.compare(i, 9)));  // Last element.

        assertEquals(new EqualRange(0, 0), seq.equalRange(i -> Integer.compare(i, 3)));
        assertEquals(new EqualRange(0, 0), seq.equalRange(i -> Integer.compare(i, 4)));
        assertEquals(new EqualRange(1, 2), seq.equalRange(i -> Integer.compare(i, 6)));  // Inside range (second element).
        assertEquals(new EqualRange(5, 5), seq.equalRange(i -> Integer.compare(i, 10)));
        assertEquals(new EqualRange(5, 5), seq.equalRange(i -> Integer.compare(i, 11)));
        assertEquals(new EqualRange(5, 5), seq.equalRange(i -> Integer.compare(i, 12)));
    }
}
