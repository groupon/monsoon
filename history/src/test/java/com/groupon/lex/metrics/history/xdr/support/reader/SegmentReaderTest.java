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
package com.groupon.lex.metrics.history.xdr.support.reader;

import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import org.mockito.runners.MockitoJUnitRunner;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class SegmentReaderTest {
    private static final int EXPECTED = 23;

    private SegmentReader<Integer> readerImpl;
    @Mock
    private Consumer<Integer> consumer;
    @Mock
    private Supplier<Integer> supplier;

    @Before
    public void setup() throws Exception {
        readerImpl = SegmentReader.of(EXPECTED);
    }

    @Test
    public void of() throws Exception {
        assertEquals(new Integer(19), SegmentReader.of(19).decode());
    }

    @Test
    public void ofSupplier() throws Exception {
        when(supplier.get()).thenReturn(10);

        SegmentReader<Integer> reader = SegmentReader.ofSupplier(supplier);
        verifyZeroInteractions(supplier);

        assertEquals(new Integer(10), reader.decode());
        verify(supplier, times(1)).get();
        verifyNoMoreInteractions(supplier);
    }

    @Test
    public void map() throws Exception {
        assertEquals(new Integer(EXPECTED + 5), readerImpl.map(v -> v + 5).decode());
    }

    @Test
    public void flatMap() throws Exception {
        SegmentReader<Integer> reader = SegmentReader.of(0)
                .flatMap((v) -> readerImpl);

        assertEquals(new Integer(EXPECTED), reader.decode());
    }

    @Test
    public void combine() throws Exception {
        SegmentReader<Integer> reader = SegmentReader.of(5)
                .combine(readerImpl, (x, y) -> x + y);

        assertEquals(new Integer(EXPECTED + 5), reader.decode());
    }

    @Test
    public void peek() throws Exception {
        SegmentReader<Integer> reader = readerImpl.peek(consumer);
        verifyZeroInteractions(consumer);

        assertEquals(new Integer(EXPECTED), reader.decode());

        verify(consumer, times(1)).accept(eq(new Integer(EXPECTED)));
        verifyNoMoreInteractions(consumer);
    }

    @Test
    public void share() throws Exception {
        SegmentReader<Integer> reader = readerImpl.share();

        Integer value = reader.decode();
        assertEquals(new Integer(EXPECTED), value);
        assertSame(value, reader.decode());
    }

    @Test
    public void cache() throws Exception {
        SegmentReader<Integer> reader = readerImpl.cache();

        Integer value = reader.decode();
        assertEquals(new Integer(EXPECTED), value);
        assertSame(value, reader.decode());
    }

    @Test
    public void cacheWithInitial() throws Exception {
        SegmentReader<Integer> reader = readerImpl.cache(9);

        Integer value = reader.decode();
        assertEquals(new Integer(9), value);
        assertSame(value, reader.decode());
    }

    @Test
    public void filter() throws Exception {
        assertEquals(Optional.of(170), SegmentReader.of(170).filter(x -> true).decode());
        assertEquals(Optional.empty(), SegmentReader.of(170).filter(x -> false).decode());
    }
}
