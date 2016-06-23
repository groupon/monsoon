/*
 * Copyright (c) 2016, Groupon, Inc.
 * All rights reserved. 
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met: 
 *
 * Redistributions of source code must retain the above copyright notice,
 * this list of conditions and the following disclaimer. 
 *
 * Redistributions in binary form must reproduce the above copyright
 * notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution. 
 *
 * Neither the name of GROUPON nor the names of its contributors may be
 * used to endorse or promote products derived from this software without
 * specific prior written permission. 
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
 * IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
 * TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
 * PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
 * TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 * NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package com.groupon.lex.metrics;

import java.util.concurrent.CompletableFuture;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import static org.mockito.Mockito.when;
import org.mockito.runners.MockitoJUnitRunner;

/**
 *
 * @author ariane
 */
@RunWith(MockitoJUnitRunner.class)
public class AbstractPushProcessorTest {
    @Mock
    private PushMetricRegistryInstance registry;

    @Before
    public void setup() {
        when(registry.getPackageName()).thenReturn("abracadabra");
        when(registry.getSupport()).thenReturn(new Support("abracadabra"));
    }

    @Test
    public void constructor() {
        class Impl extends AbstractPushProcessor {
            public Impl(PushMetricRegistryInstance registry, int interval_seconds) {
                super(registry, interval_seconds);
            }

            @Override
            protected void runImplementation() throws Exception {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }
        }

        try (Impl impl = new Impl(registry, 10)) {
            assertEquals(10, impl.getIntervalSeconds());
            assertSame(registry, impl.getMetricRegistry());
        }
    }

    @Test(timeout = 15000)
    public void run_one_cycle() throws Exception {
        CompletableFuture<Object> run_implementation_called = new CompletableFuture<Object>();
        class Impl extends AbstractPushProcessor {
            public Impl(PushMetricRegistryInstance registry, int interval_seconds) {
                super(registry, interval_seconds);
            }

            @Override
            protected void runImplementation() throws Exception {
                run_implementation_called.complete(this);
            }

            @Override
            public void close() {
                run_implementation_called.completeExceptionally(new Exception("Closed before I ran."));
                super.close();
            }
        }

        try (Impl impl = new Impl(registry, 10)) {
            impl.start();
            run_implementation_called.get();
        }
    }

    @Test
    public void get_support() {
        class Impl extends AbstractPushProcessor {
            public Impl(PushMetricRegistryInstance registry, int interval_seconds) {
                super(registry, interval_seconds);
            }

            @Override
            protected void runImplementation() throws Exception {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }
        }

        try (Impl impl = new Impl(registry, 10)) {
            assertEquals("abracadabra", impl.getSupport().getPackageName());
        }
    }

    @Test(timeout = 25000)
    public void keep_running_when_excepting() throws Exception {
        CompletableFuture<Object> run_implementation_called = new CompletableFuture<Object>();
        class Impl extends AbstractPushProcessor {
            int run_impl_called_counter = 0;

            public Impl(PushMetricRegistryInstance registry, int interval_seconds) {
                super(registry, interval_seconds);
            }

            @Override
            protected void runImplementation() throws Exception {
                if (run_impl_called_counter++ == 0)
                    throw new Exception("First call fails");
                else
                    run_implementation_called.complete(this);
            }

            @Override
            public void close() {
                run_implementation_called.completeExceptionally(new Exception("Closed before I ran."));
                super.close();
            }
        }

        try (Impl impl = new Impl(registry, 10)) {
            impl.start();
            run_implementation_called.get();
        }
    }
}
