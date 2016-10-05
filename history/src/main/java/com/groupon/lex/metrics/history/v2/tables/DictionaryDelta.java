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

import com.groupon.lex.metrics.MetricValue;
import com.groupon.lex.metrics.Tags;
import com.groupon.lex.metrics.history.v2.xdr.FromXdr;
import com.groupon.lex.metrics.history.v2.xdr.dictionary_delta;
import com.groupon.lex.metrics.history.v2.xdr.path_dictionary_delta;
import com.groupon.lex.metrics.history.v2.xdr.strval_dictionary_delta;
import com.groupon.lex.metrics.history.v2.xdr.tag_dictionary_delta;
import com.groupon.lex.metrics.history.v2.xdr.tags;
import com.groupon.lex.metrics.lib.SimpleMapEntry;
import static gnu.trove.TCollections.unmodifiableMap;
import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import java.util.ArrayList;
import java.util.Arrays;
import static java.util.Collections.unmodifiableList;
import java.util.List;
import java.util.Map;
import static java.util.Objects.requireNonNull;


public class DictionaryDelta {
    private final TIntObjectMap<String> stringTable;
    private final TIntObjectMap<List<String>> pathTable;
    private final TIntObjectMap<Tags> tagsTable;

    public DictionaryDelta(dictionary_delta input) {
        this(input, null);
    }

    public DictionaryDelta(dictionary_delta input, DictionaryDelta parent) {
        stringTable = unmodifiableMap(decode(input.sdd, (parent == null ? null : parent.stringTable)));
        pathTable = unmodifiableMap(decode(input.pdd, (parent == null ? null : parent.pathTable), stringTable));
        tagsTable = unmodifiableMap(decode(input.tdd, (parent == null ? null : parent.tagsTable), stringTable));
    }

    private static TIntObjectMap<String> decode(strval_dictionary_delta sdd, TIntObjectMap<String> parent) {
        final TIntObjectMap<String> stringTable = new TIntObjectHashMap<>(sdd.values.length, 1f);
        if (parent != null) stringTable.putAll(parent);
        for (int i = 0; i < sdd.values.length; ++i)
            stringTable.put(sdd.offset + i, sdd.values[i].value);
        return stringTable;
    }

    private static TIntObjectMap<List<String>> decode(path_dictionary_delta pdd, TIntObjectMap<List<String>> parent, TIntObjectMap<String> stringTable) {
        final TIntObjectMap<List<String>> pathTable = new TIntObjectHashMap<>(pdd.values.length, 1f);
        if (parent != null) pathTable.putAll(parent);
        for (int i = 0; i < pdd.values.length; ++i) {
            final List<String> value = unmodifiableList(Arrays.asList(Arrays.stream(pdd.values[i].value)
                    .mapToObj(str_ref -> requireNonNull(stringTable.get(str_ref), "Invalid string reference"))
                    .toArray(String[]::new)));
            pathTable.put(pdd.offset + i, value);
        }
        return pathTable;
    }

    private static TIntObjectMap<Tags> decode(tag_dictionary_delta tdd, TIntObjectMap<Tags> parent, TIntObjectMap<String> stringTable) {
        final TIntObjectMap<Tags> tagsTable = new TIntObjectHashMap<>(tdd.values.length, 1f);
        if (parent != null) tagsTable.putAll(parent);
        for (int i = 0; i < tdd.values.length; ++i) {
            final tags current = tdd.values[i];
            final List<Map.Entry<String, MetricValue>> tagEntries = new ArrayList<>(current.str_ref.length);

            for (int tagIdx = 0; tagIdx < current.str_ref.length; ++i) {
                final String key = requireNonNull(stringTable.get(current.str_ref[tagIdx]), "Invalid string reference");
                final MetricValue val = FromXdr.metricValue(current.value[tagIdx], (sref) -> requireNonNull(stringTable.get(sref), "Invalid string reference"));
                tagEntries.add(SimpleMapEntry.create(key, val));
            }
            tagsTable.put(tdd.offset + i, Tags.valueOf(tagEntries.stream()));
        }
        return tagsTable;
    }

    public String getString(int ref) {
        return requireNonNull(stringTable.get(ref), "Invalid string reference");
    }

    public Tags getTags(int ref) {
        return requireNonNull(tagsTable.get(ref), "Invalid tags reference");
    }

    public List<String> getPath(int ref) {
        return requireNonNull(pathTable.get(ref), "Invalid path reference");
    }
}
