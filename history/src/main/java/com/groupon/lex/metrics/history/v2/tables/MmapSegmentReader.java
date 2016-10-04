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

import static com.groupon.lex.metrics.history.xdr.BufferSupplier.singleBuffer;
import com.groupon.lex.metrics.history.xdr.support.FilePos;
import com.groupon.lex.metrics.history.xdr.support.GzipDecodingBufferSupplier;
import com.groupon.lex.metrics.history.xdr.support.XdrBufferDecodingStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.function.Supplier;
import lombok.Getter;
import lombok.NonNull;
import org.acplt.oncrpc.OncRpcException;
import org.acplt.oncrpc.XdrAble;
import org.acplt.oncrpc.XdrDecodingStream;

public class MmapSegmentReader<T extends XdrAble> implements SegmentReader<T> {
    @Getter
    private final boolean compressed;
    private final Supplier<T> type;
    private final ByteBuffer view;

    public MmapSegmentReader(@NonNull Supplier<T> type, @NonNull ByteBuffer file, @NonNull FilePos pos, boolean compressed) {
        this.compressed = compressed;
        this.type = type;
        view = file.asReadOnlyBuffer();
        view.position((int)pos.getOffset());
        view.limit((int)pos.getEnd());
    }

    @Override
    public T decode() throws IOException, OncRpcException {
        T instance = type.get();
        XdrDecodingStream xdr;
        if (isCompressed())
            xdr = new XdrBufferDecodingStream(new GzipDecodingBufferSupplier(singleBuffer(view.duplicate())));
        else
            xdr = new XdrBufferDecodingStream(view.duplicate());
        xdr.beginDecoding();
        instance.xdrDecode(xdr);
        xdr.endDecoding();

        return instance;
    }
}
