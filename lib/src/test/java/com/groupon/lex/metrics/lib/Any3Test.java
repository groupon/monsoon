package com.groupon.lex.metrics.lib;

import java.util.Optional;
import java.util.function.Function;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import static org.mockito.Mockito.verifyZeroInteractions;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class Any3Test {
    private static final String TEST = "TEST";
    @Mock
    private Function<String, String> fn1, fn2, fn3;

    @Test
    public void create_1() {
        Any3<String, String, String> v = Any3.create1(TEST);

        assertEquals(Optional.of(TEST), v.get1());
        assertEquals(Optional.empty(), v.get2());
        assertEquals(Optional.empty(), v.get3());

        assertThat(v.toString(), containsString(TEST));
        assertSame(TEST, v.mapCombine(Function.identity(), fn2, fn3));
        assertEquals(Any3.create1(TEST + TEST), v.map(s -> s + s, fn2, fn3));

        verifyZeroInteractions(fn1, fn2, fn3);
    }

    @Test
    public void create_2() {
        Any3<String, String, String> v = Any3.create2(TEST);

        assertEquals(Optional.empty(), v.get1());
        assertEquals(Optional.of(TEST), v.get2());
        assertEquals(Optional.empty(), v.get3());

        assertThat(v.toString(), containsString(TEST));
        assertSame(TEST, v.mapCombine(fn1, Function.identity(), fn3));
        assertEquals(Any3.create2(TEST + TEST), v.map(fn1, s -> s + s, fn3));

        verifyZeroInteractions(fn1, fn2, fn3);
    }

    @Test
    public void create_3() {
        Any3<String, String, String> v = Any3.create3(TEST);

        assertEquals(Optional.empty(), v.get1());
        assertEquals(Optional.empty(), v.get2());
        assertEquals(Optional.of(TEST), v.get3());

        assertThat(v.toString(), containsString(TEST));
        assertSame(TEST, v.mapCombine(fn1, fn2, Function.identity()));
        assertEquals(Any3.create3(TEST + TEST), v.map(fn1, fn2, s -> s + s));

        verifyZeroInteractions(fn1, fn2, fn3);
    }
}
