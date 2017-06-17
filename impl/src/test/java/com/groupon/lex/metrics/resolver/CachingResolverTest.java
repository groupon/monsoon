package com.groupon.lex.metrics.resolver;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.joda.time.Duration;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class CachingResolverTest {
    @Mock
    private AsyncResolver mockedAsyncResolver;

    @Before
    public void setup() {
        when(mockedAsyncResolver.getTupleWidth()).thenReturn(2);
    }

    @Test
    public void getTupleWidthTest() throws Exception {
        try (CachingResolver cr = new CachingResolver(mockedAsyncResolver, Duration.standardMinutes(10), Duration.standardMinutes(60))) {
            assertEquals(2, cr.getTupleWidth());
        }

        verify(mockedAsyncResolver, times(1)).getTupleWidth();
        verify(mockedAsyncResolver, atLeast(0)).getTuples();
        verifyNoMoreInteractions(mockedAsyncResolver);
    }

    @Test
    public void getTuplesTest() throws Exception {
        final CompletableFuture<Collection<ResolverTuple>> fut = new CompletableFuture<>();
        final List<ResolverTuple> RESULT = new ArrayList<>();

        when(mockedAsyncResolver.getTuples()).thenReturn((CompletableFuture) fut);

        final Collection<ResolverTuple> entries;
        try (CachingResolver cr = new CachingResolver(mockedAsyncResolver, Duration.standardMinutes(10), Duration.standardMinutes(60))) {
            fut.complete(RESULT);
            Thread.sleep(5000);  // Racy test, but this should work in most cases to allow callbacks to propagate.
            entries = cr.getTuples();
        }

        assertSame(RESULT, entries);
    }

    @Test(expected = CachingResolver.StaleResolverException.class)
    public void getTuplesExceptionalTest() throws Exception {
        final CompletableFuture<Collection<ResolverTuple>> fut = new CompletableFuture<>();
        final List<ResolverTuple> RESULT = new ArrayList<>();

        when(mockedAsyncResolver.getTuples()).thenReturn((CompletableFuture) fut);

        try (CachingResolver cr = new CachingResolver(mockedAsyncResolver, Duration.standardMinutes(10), Duration.standardMinutes(60))) {
            fut.completeExceptionally(new TestException());
            cr.getTuples();
        }
    }

    private static class TestException extends Exception {
    }
}
