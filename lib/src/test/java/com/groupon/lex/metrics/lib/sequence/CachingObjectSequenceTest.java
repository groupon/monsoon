package com.groupon.lex.metrics.lib.sequence;

import java.lang.ref.SoftReference;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class CachingObjectSequenceTest {
    private static final String ZERO = "zero";
    @Mock
    private ObjectSequence<String> underlying;

    private ObjectSequence<String> seq;

    @Before
    public void setup() {
        when(underlying.size()).thenReturn(1);
        when(underlying.get(0)).thenReturn(ZERO);  // Reference won't expire, because static constant exists.

        seq = new CachingObjectSequence<>(underlying, SoftReference::new);
        verify(underlying, times(1)).size();
    }

    @Test
    public void constructor() {
        assertFalse(seq.isEmpty());
        assertEquals(1, seq.size());

        verifyNoMoreInteractions(underlying);
    }

    @Test
    public void readTwice() {
        assertSame(ZERO, seq.get(0));
        verify(underlying, times(1)).get(0);

        assertSame(ZERO, seq.get(0));  // Second call uses cached value.
        verifyNoMoreInteractions(underlying);
    }

    @Test
    public void flags() {
        when(underlying.isSorted()).thenReturn(true);
        when(underlying.isNonnull()).thenReturn(true);
        when(underlying.isDistinct()).thenReturn(true);

        assertTrue(seq.isSorted());
        verify(underlying, times(1)).isSorted();
        assertTrue(seq.isNonnull());
        verify(underlying, times(1)).isNonnull();
        assertTrue(seq.isDistinct());
        verify(underlying, times(1)).isDistinct();

        verifyNoMoreInteractions(underlying);
    }
}
