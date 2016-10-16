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
package com.groupon.lex.metrics.history;

import com.groupon.lex.metrics.history.xdr.Const;
import com.groupon.lex.metrics.history.xdr.support.SequenceTSData;
import com.groupon.lex.metrics.history.xdr.support.writer.FileChannelWriter;
import com.groupon.lex.metrics.history.xdr.support.writer.GzipWriter;
import com.groupon.lex.metrics.history.xdr.support.writer.XdrEncodingFileWriter;
import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import static org.junit.Assert.assertSame;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TSDataVersionDispatchTest {
    private static final Logger LOG = Logger.getLogger(TSDataVersionDispatchTest.class.getName());

    @Mock
    private TSDataVersionDispatch.Factory factory_v0, factory_v1;
    @Mock
    private SequenceTSData tsdata;

    private Path file;
    private List<TSDataVersionDispatch.Factory> FACTORY;

    @Before
    public void setup() throws Exception {
        File fileName = File.createTempFile("monsoon-", "-TSDataVersionDispatch");
        fileName.deleteOnExit();
        file = fileName.toPath();
        LOG.log(Level.INFO, "using test file: {0}", file);

        FACTORY = Arrays.asList(factory_v0, factory_v1);
        Mockito.when(factory_v0.open(Mockito.any(), Mockito.anyBoolean())).thenReturn(tsdata);
        Mockito.when(factory_v1.open(Mockito.any(), Mockito.anyBoolean())).thenReturn(tsdata);
    }

    private void applyVersion(int major, int minor) throws Exception {
        try (FileChannel fd = FileChannel.open(file, StandardOpenOption.WRITE)) {
            try (XdrEncodingFileWriter writer = new XdrEncodingFileWriter(new FileChannelWriter(fd, 0), 64)) {
                Const.writeMimeHeader(writer, (short) major, (short) minor);
            }
        }
    }

    private void applyVersionCompressed(int major, int minor) throws Exception {
        try (FileChannel fd = FileChannel.open(file, StandardOpenOption.WRITE)) {
            try (XdrEncodingFileWriter writer = new XdrEncodingFileWriter(new GzipWriter(new FileChannelWriter(fd, 0)), 64)) {
                Const.writeMimeHeader(writer, (short) major, (short) minor);
            }
        }
    }

    @Test
    public void open_v0() throws Exception {
        applyVersion(0, 1);

        TSData opened = TSDataVersionDispatch.open(file, FACTORY);

        assertSame(tsdata, opened);
        verify(factory_v0, times(1)).open(Mockito.any(), Mockito.anyBoolean());
        verifyNoMoreInteractions(factory_v0);
        verifyZeroInteractions(factory_v1, tsdata);
    }

    @Test
    public void open_v1() throws Exception {
        applyVersion(1, 0);

        TSData opened = TSDataVersionDispatch.open(file, FACTORY);

        assertSame(tsdata, opened);
        verify(factory_v1, times(1)).open(Mockito.any(), Mockito.anyBoolean());
        verifyNoMoreInteractions(factory_v1);
        verifyZeroInteractions(factory_v0, tsdata);
    }

    @Test
    public void open_v0_compressed() throws Exception {
        applyVersionCompressed(0, 1);

        TSData opened = TSDataVersionDispatch.open(file, FACTORY);

        assertSame(tsdata, opened);
        verify(factory_v0, times(1)).open(Mockito.any(), Mockito.anyBoolean());
        verifyNoMoreInteractions(factory_v0);
        verifyZeroInteractions(factory_v1, tsdata);
    }

    @Test
    public void open_v1_compressed() throws Exception {
        applyVersionCompressed(1, 0);

        TSData opened = TSDataVersionDispatch.open(file, FACTORY);

        assertSame(tsdata, opened);
        verify(factory_v1, times(1)).open(Mockito.any(), Mockito.anyBoolean());
        verifyNoMoreInteractions(factory_v1);
        verifyZeroInteractions(factory_v0, tsdata);
    }

    @Test(expected = IOException.class)
    public void badVersion() throws Exception {
        applyVersion(99, 0);

        TSDataVersionDispatch.open(file, FACTORY);
    }
}
