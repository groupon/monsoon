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
package com.groupon.lex.metrics.collector.httpget;

import java.io.InputStream;
import static org.junit.Assert.assertEquals;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import org.mockito.runners.MockitoJUnitRunner;

/**
 *
 * @author ariane
 */
@RunWith(MockitoJUnitRunner.class)
public class ByteCountingInputStreamTest {
    @Mock
    private InputStream istream;

    @Test
    public void dont_close() throws Exception {
        ByteCountingInputStream s = new ByteCountingInputStream(istream, false);
        s.close();

        verify(istream, never()).close();
        verifyNoMoreInteractions(istream);
    }

    @Test
    public void do_close() throws Exception {
        ByteCountingInputStream s = new ByteCountingInputStream(istream);
        s.close();

        verify(istream, times(1)).close();
        verifyNoMoreInteractions(istream);
    }

    @Test
    public void read_byte() throws Exception {
        when(istream.read()).thenReturn(17);
        ByteCountingInputStream s = new ByteCountingInputStream(istream);

        assertEquals(17, s.read());
        assertEquals(1, s.getBytesRead());

        verify(istream, times(1)).read();
        verifyNoMoreInteractions(istream);
    }

    @Test
    public void read_bytearray() throws Exception {
        byte[] data = new byte[10];
        when(istream.read(data)).thenReturn(7);
        ByteCountingInputStream s = new ByteCountingInputStream(istream);

        assertEquals(7, s.read(data));
        assertEquals(7, s.getBytesRead());

        verify(istream, times(1)).read(data);
        verifyNoMoreInteractions(istream);
    }

    @Test
    public void read_bytearray_indexed() throws Exception {
        byte[] data = new byte[10];
        when(istream.read(data, 4, 9)).thenReturn(3);
        ByteCountingInputStream s = new ByteCountingInputStream(istream);

        assertEquals(3, s.read(data, 4, 9));
        assertEquals(3, s.getBytesRead());

        verify(istream, times(1)).read(data, 4, 9);
        verifyNoMoreInteractions(istream);
    }

    @Test
    public void skip() throws Exception {
        when(istream.skip(200L)).thenReturn(199L);
        ByteCountingInputStream s = new ByteCountingInputStream(istream);

        assertEquals(199L, s.skip(200L));
        assertEquals(199, s.getBytesRead());

        verify(istream, times(1)).skip(200L);
        verifyNoMoreInteractions(istream);
    }

    @Test
    public void available() throws Exception {
        when(istream.available()).thenReturn(255);

        ByteCountingInputStream s = new ByteCountingInputStream(istream);
        assertEquals(255, s.available());

        verify(istream, times(1)).available();
        verifyNoMoreInteractions(istream);
    }

    @Test
    public void mark() throws Exception {
        when(istream.skip(17)).thenReturn(17L);

        ByteCountingInputStream s = new ByteCountingInputStream(istream);
        s.skip(17);
        s.mark(2000);
        s.skip(17);
        assertEquals(2 * 17, s.getBytesRead());

        s.reset();
        assertEquals(17, s.getBytesRead());

        verify(istream, times(2)).skip(17);
        verify(istream, times(1)).mark(2000);
        verify(istream, times(1)).reset();
        verifyNoMoreInteractions(istream);
    }

    @Test
    public void mark_supported() throws Exception {
        when(istream.markSupported()).thenReturn(true);

        ByteCountingInputStream s = new ByteCountingInputStream(istream);
        assertEquals(true, s.markSupported());

        verify(istream, times(1)).markSupported();
        verifyNoMoreInteractions(istream);
    }

    @Test
    public void eof_read_byte() throws Exception {
        when(istream.read()).thenReturn(-1);
        ByteCountingInputStream s = new ByteCountingInputStream(istream);

        assertEquals(-1, s.read());
        assertEquals(0, s.getBytesRead());

        verify(istream, times(1)).read();
        verifyNoMoreInteractions(istream);
    }

    @Test
    public void eof_read_bytearray() throws Exception {
        byte[] data = new byte[10];
        when(istream.read(data)).thenReturn(-1);
        ByteCountingInputStream s = new ByteCountingInputStream(istream);

        assertEquals(-1, s.read(data));
        assertEquals(0, s.getBytesRead());

        verify(istream, times(1)).read(data);
        verifyNoMoreInteractions(istream);
    }

    @Test
    public void eof_read_bytearray_indexed() throws Exception {
        byte[] data = new byte[10];
        when(istream.read(data, 4, 9)).thenReturn(-1);
        ByteCountingInputStream s = new ByteCountingInputStream(istream);

        assertEquals(-1, s.read(data, 4, 9));
        assertEquals(0, s.getBytesRead());

        verify(istream, times(1)).read(data, 4, 9);
        verifyNoMoreInteractions(istream);
    }
}
