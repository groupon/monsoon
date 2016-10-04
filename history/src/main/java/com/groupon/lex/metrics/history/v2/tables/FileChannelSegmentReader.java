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
package com.groupon.lex.metrics.history.v2.tables;

import com.groupon.lex.metrics.history.xdr.BufferSupplier;
import com.groupon.lex.metrics.history.xdr.support.FilePos;
import com.groupon.lex.metrics.history.xdr.support.GzipDecodingBufferSupplier;
import com.groupon.lex.metrics.history.xdr.support.SegmentBufferSupplier;
import com.groupon.lex.metrics.history.xdr.support.XdrBufferDecodingStream;
import com.groupon.lex.metrics.lib.GCCloseable;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.function.Supplier;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.acplt.oncrpc.OncRpcException;
import org.acplt.oncrpc.XdrAble;
import org.acplt.oncrpc.XdrDecodingStream;

@AllArgsConstructor
public class FileChannelSegmentReader<T extends XdrAble> implements SegmentReader<T> {
    @NonNull
    private final Supplier<T> type;
    @NonNull
    private final GCCloseable<FileChannel> file;
    @NonNull
    private final FilePos pos;
    @Getter
    private final boolean compressed;

    @Override
    public T decode() throws IOException, OncRpcException {
        BufferSupplier supplier = new SegmentBufferSupplier(file, pos);
        if (isCompressed())
            supplier = new GzipDecodingBufferSupplier(supplier);

        T instance = type.get();
        final XdrDecodingStream xdr = new XdrBufferDecodingStream(supplier);
        xdr.beginDecoding();
        instance.xdrDecode(xdr);
        xdr.endDecoding();

        return instance;
    }

    @RequiredArgsConstructor
    public static class Factory implements SegmentReader.Factory<XdrAble> {
        private final GCCloseable<FileChannel> file;
        private final boolean compressed;

        @Override
        public <T extends XdrAble> SegmentReader<T> get(Supplier<T> type, FilePos pos) {
            return new FileChannelSegmentReader<>(type, file, pos, compressed);
        }
    }
}
