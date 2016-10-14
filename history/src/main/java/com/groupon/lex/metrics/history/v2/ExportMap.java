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
package com.groupon.lex.metrics.history.v2;

import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import java.util.ArrayList;
import static java.util.Collections.EMPTY_MAP;
import java.util.List;
import java.util.Map;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode
public class ExportMap<T> implements Cloneable {
    private static final int NO_ENTRY = -1;
    private final TObjectIntMap<T> table = new TObjectIntHashMap<>(10, 0.5f, NO_ENTRY);
    private int initSeq;
    private int nextSeq;

    public ExportMap() {
        this(0, EMPTY_MAP);
    }

    public ExportMap(int nextSeq, Map<Integer, T> init) {
        this.initSeq = this.nextSeq = nextSeq;
        init.forEach((idx, val) -> {
            if (this.nextSeq <= idx)
                this.nextSeq = idx + 1;
            table.put(val, idx);
        });
    }

    private ExportMap(ExportMap<T> original) {
        table.putAll(original.table);
        initSeq = original.initSeq;
        nextSeq = original.nextSeq;
    }

    public synchronized int getOrCreate(T value) {
        int idx = table.putIfAbsent(value, nextSeq);
        if (idx == NO_ENTRY)
            idx = nextSeq++;
        return idx;
    }

    public synchronized int getOffset() {
        return initSeq;
    }

    public synchronized boolean isEmpty() {
        return !table.containsValue(initSeq);
    }

    public synchronized ArrayList<T> invert() {
        final ArrayList<T> data = new ArrayList<>(nextSeq);
        for (int i = 0; i < nextSeq; ++i)
            data.add(null);  // Make all elements accessable by data.set().

        table.forEachEntry((key, value) -> {
            data.set(value, key);
            return true;
        });
        return data;
    }

    /**
     * Reset for the next write cycle.
     *
     * The next write cycle will exclude any data present in the dictionary,
     * during serialization.
     */
    public synchronized void reset() {
        initSeq = nextSeq;
    }

    public synchronized List<T> createMap() {
        final List<T> data = new ArrayList<>(nextSeq - initSeq);
        for (int i = initSeq; i < nextSeq; ++i)
            data.add(null);  // Make all elements accessable by data.set().

        table.forEachEntry((key, value) -> {
            if (value >= initSeq)
                data.set(value - initSeq, key);
            return true;
        });
        return data;
    }

    @Override
    public synchronized ExportMap<T> clone() {
        return new ExportMap<>(this);
    }
}
