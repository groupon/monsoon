package com.groupon.lex.metrics.lib;

import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import static org.junit.Assert.assertEquals;
import org.junit.Test;
import org.junit.runner.RunWith;
import static org.mockito.Matchers.any;
import org.mockito.Mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import org.mockito.runners.MockitoJUnitRunner;

/**
 *
 * @author ariane
 */
@RunWith(MockitoJUnitRunner.class)
public class LazyEvalTest {
    @Mock
    private Supplier<String> fn_0;
    @Mock
    private Function<String, String> fn_1;
    @Mock
    private BiFunction<String, String, String> fn_2;
    @Mock
    private TriFunction<String, String, String, String> fn_3;

    @Test
    public void fn0() {
        Supplier<String> eval = new LazyEval<>(fn_0);

        when(fn_0.get()).thenReturn("foobar");

        assertEquals("1st invocation", "foobar", eval.get());
        assertEquals("2nd invocation", "foobar", eval.get());

        verify(fn_0, times(1)).get();
        verifyNoMoreInteractions(fn_0);
        verifyZeroInteractions(fn_1, fn_2, fn_3);
    }

    @Test
    public void fn1() {
        Supplier<String> eval = new LazyEval<>(fn_1, "arg0");

        when(fn_1.apply("arg0")).thenReturn("foobar");

        assertEquals("1st invocation", "foobar", eval.get());
        assertEquals("2nd invocation", "foobar", eval.get());

        verify(fn_1, times(1)).apply(any());
        verifyNoMoreInteractions(fn_1);
        verifyZeroInteractions(fn_0, fn_2, fn_3);
    }

    @Test
    public void fn2() {
        Supplier<String> eval = new LazyEval<>(fn_2, "arg0", "arg1");

        when(fn_2.apply("arg0", "arg1")).thenReturn("foobar");

        assertEquals("1st invocation", "foobar", eval.get());
        assertEquals("2nd invocation", "foobar", eval.get());

        verify(fn_2, times(1)).apply(any(), any());
        verifyNoMoreInteractions(fn_2);
        verifyZeroInteractions(fn_0, fn_1, fn_3);
    }

    @Test
    public void fn3() {
        Supplier<String> eval = new LazyEval<>(fn_3, "arg0", "arg1", "arg2");

        when(fn_3.apply("arg0", "arg1", "arg2")).thenReturn("foobar");

        assertEquals("1st invocation", "foobar", eval.get());
        assertEquals("2nd invocation", "foobar", eval.get());

        verify(fn_3, times(1)).apply(any(), any(), any());
        verifyNoMoreInteractions(fn_3);
        verifyZeroInteractions(fn_0, fn_1, fn_2);
    }
}
