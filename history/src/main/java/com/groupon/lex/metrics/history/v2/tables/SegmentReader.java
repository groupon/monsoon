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

import com.groupon.lex.metrics.history.xdr.support.FilePos;
import java.io.IOException;
import java.lang.ref.Reference;
import java.lang.ref.SoftReference;
import java.lang.ref.WeakReference;
import java.util.function.Function;
import java.util.function.Supplier;
import lombok.RequiredArgsConstructor;
import org.acplt.oncrpc.OncRpcException;

public interface SegmentReader<T> {
    public abstract T decode() throws IOException, OncRpcException;

    public default <U> SegmentReader<U> map(Function<? super T, ? extends U> fn) {
        return new MappedSegmentReader<T, U>(this, fn);
    }

    public default SegmentReader<T> share() {
        return new WeakSharedSegmentReader<>(this);
    }

    public default SegmentReader<T> cache() {
        return new SoftSharedSegmentReader<>(this);
    }

    public static interface Factory<BaseType> {
        public <T extends BaseType> SegmentReader<T> get(Supplier<T> type, FilePos pos);
    }
}

@RequiredArgsConstructor
class MappedSegmentReader<T, U> implements SegmentReader<U> {
    private final SegmentReader<T> in;
    private final Function<? super T, ? extends U> fn;

    @Override
    public U decode() throws IOException, OncRpcException {
        return fn.apply(in.decode());
    }
}

@RequiredArgsConstructor
class WeakSharedSegmentReader<T> implements SegmentReader<T> {
    private final SegmentReader<T> in;
    private Reference<T> ref = new WeakReference<>(null);

    @Override
    public synchronized T decode() throws IOException, OncRpcException {
        T v = ref.get();
        if (v != null) return v;

        v = in.decode();
        ref = new WeakReference<>(v);
        return v;
    }
}

@RequiredArgsConstructor
class SoftSharedSegmentReader<T> implements SegmentReader<T> {
    private final SegmentReader<T> in;
    private Reference<T> ref = new SoftReference<>(null);

    @Override
    public synchronized T decode() throws IOException, OncRpcException {
        T v = ref.get();
        if (v != null) return v;

        v = in.decode();
        ref = new SoftReference<>(v);
        return v;
    }
}
