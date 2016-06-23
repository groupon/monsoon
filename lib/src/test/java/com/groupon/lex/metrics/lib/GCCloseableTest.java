/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.groupon.lex.metrics.lib;

import static java.util.Objects.requireNonNull;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author ariane
 */
public class GCCloseableTest {
    private CompletableFuture<Object> closed_;
    private GCCloseable<CloseableImpl> ref;

    @Before
    public void setup() {
        closed_ = new CompletableFuture<>();
        ref = new GCCloseable<>(new CloseableImpl(closed_));
    }

    @Test
    public void closes_at_some_point() throws Exception {
        ref = null;

        for (int i = 0; i < 10; ++i) {
            System.gc();

            try {
                closed_.get(1, TimeUnit.SECONDS);
            } catch (TimeoutException ex) {
                /* Ignore. */
            }
        }
        assertTrue(closed_.isDone());
    }

    private static class CloseableImpl implements AutoCloseable {
        private final CompletableFuture<Object> fut_;

        public CloseableImpl(CompletableFuture<Object> fut) {
            fut_ = requireNonNull(fut);
        }

        @Override
        public void close() {
            fut_.complete(new Object());
        }
    }
}
