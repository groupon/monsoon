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

import gnu.trove.iterator.TObjectIntIterator;
import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import java.util.ArrayList;
import java.util.List;

public class ExportMap<T> {
    private static final int NO_ENTRY = -1;
    private final TObjectIntMap<T> table = new TObjectIntHashMap<>(10, 0.5f, NO_ENTRY);
    private final int initSeq;
    private int nextSeq;

    public ExportMap(int nextSeq) { this.initSeq = this.nextSeq = nextSeq; }
    public ExportMap() { this(0); }

    public int getOrCreate(T value) {
        int idx = table.putIfAbsent(value, nextSeq);
        if (idx == NO_ENTRY) idx = nextSeq++;
        return idx;
    }

    public int getOffset() { return initSeq; }

    public List<T> createMap() {
        final List<T> data = new ArrayList<>(nextSeq);
        for (int i = 0; i < nextSeq; ++i) data.add(null);  // Make all elements accessable by dada.set().

        final TObjectIntIterator<T> iter = table.iterator();
        while (iter.hasNext()) {
            iter.advance();
            data.set(iter.value(), iter.key());
        }
        return data;
    }
}
