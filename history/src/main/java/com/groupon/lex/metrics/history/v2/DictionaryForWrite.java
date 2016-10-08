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

import com.groupon.lex.metrics.Tags;
import com.groupon.lex.metrics.history.v2.tables.DictionaryDelta;
import com.groupon.lex.metrics.history.v2.xdr.ToXdr;
import com.groupon.lex.metrics.history.v2.xdr.dictionary_delta;
import com.groupon.lex.metrics.history.v2.xdr.metric_value;
import com.groupon.lex.metrics.history.v2.xdr.path;
import com.groupon.lex.metrics.history.v2.xdr.path_dictionary_delta;
import com.groupon.lex.metrics.history.v2.xdr.string_val;
import com.groupon.lex.metrics.history.v2.xdr.strval_dictionary_delta;
import com.groupon.lex.metrics.history.v2.xdr.tag_dictionary_delta;
import com.groupon.lex.metrics.history.v2.xdr.tags;
import com.groupon.lex.metrics.history.xdr.support.ForwardSequence;
import com.groupon.lex.metrics.lib.SimpleMapEntry;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@Getter
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@EqualsAndHashCode
public final class DictionaryForWrite implements Cloneable {
    private final ExportMap<String> stringTable;
    private final ExportMap<List<String>> pathTable;
    private final ExportMap<Tags> tagsTable;

    public DictionaryForWrite() {
        this(new ExportMap<>(), new ExportMap<>(), new ExportMap<>());
    }

    public DictionaryForWrite(DictionaryDelta input) {
        this(
                new ExportMap<>(
                        input.getStringRefEnd(),
                        new ForwardSequence(input.getStringRefOffset(), input.getStringRefEnd())
                                .map(Integer::valueOf, true, true, true)
                                .stream()
                                .collect(Collectors.toMap(idx -> idx, idx -> input.getString(idx)))),
                new ExportMap<>(
                        input.getPathRefEnd(),
                        new ForwardSequence(input.getPathRefOffset(), input.getPathRefEnd())
                                .map(Integer::valueOf, true, true, true)
                                .stream()
                                .collect(Collectors.toMap(idx -> idx, idx -> input.getPath(idx)))),
                new ExportMap<>(
                        input.getTagsRefEnd(),
                        new ForwardSequence(input.getTagsRefOffset(), input.getTagsRefEnd())
                                .map(Integer::valueOf, true, true, true)
                                .stream()
                                .collect(Collectors.toMap(idx -> idx, idx -> input.getTags(idx))))
        );
    }

    public boolean isEmpty() {
        return stringTable.isEmpty() && pathTable.isEmpty() && tagsTable.isEmpty();
    }

    public DictionaryDelta asDictionaryDelta() {
        return new DictionaryDelta(stringTable.invert(), pathTable.invert(), tagsTable.invert());
    }

    /**
     * Reset for the next write cycle.
     *
     * The next write cycle will exclude any data present in the dictionary, during serialization.
     */
    public void reset() {
        stringTable.reset();
        pathTable.reset();
        tagsTable.reset();
    }

    public dictionary_delta encode() {
        dictionary_delta dd = new dictionary_delta();
        dd.pdd = encodePaths(pathTable, stringTable);
        dd.tdd = encodeTags(tagsTable, stringTable);
        dd.sdd = encodeStrings(stringTable);  // Must be last!
        return dd;
    }

    private static tag_dictionary_delta encodeTags(ExportMap<Tags> tagsTable, ExportMap<String> stringTable) {
        tag_dictionary_delta tdd = new tag_dictionary_delta();
        tdd.offset = tagsTable.getOffset();
        tdd.values = tagsTable.createMap().stream()
                .map(tags -> {
                    List<Map.Entry<Integer, metric_value>> data = tags.stream()
                            .map(tagEntry -> {
                                return SimpleMapEntry.create(
                                        stringTable.getOrCreate(tagEntry.getKey()),
                                        ToXdr.metricValue(tagEntry.getValue(), stringTable::getOrCreate));
                            })
                            .collect(Collectors.toList());

                    tags t = new tags();
                    t.str_ref = data.stream()
                            .mapToInt(Map.Entry::getKey)
                            .toArray();
                    t.value = data.stream()
                            .map(Map.Entry::getValue)
                            .toArray(metric_value[]::new);
                    return t;
                })
                .toArray(tags[]::new);
        return tdd;
    }

    private static path_dictionary_delta encodePaths(ExportMap<List<String>> pathTable, ExportMap<String> stringTable) {
        path_dictionary_delta pdd = new path_dictionary_delta();
        pdd.offset = pathTable.getOffset();
        pdd.values = pathTable.createMap().stream()
                .map(p -> {
                    return new path(p.stream()
                            .mapToInt(stringTable::getOrCreate)
                            .toArray());
                })
                .toArray(path[]::new);
        return pdd;
    }

    private static strval_dictionary_delta encodeStrings(ExportMap<String> stringTable) {
        strval_dictionary_delta sdd = new strval_dictionary_delta();
        sdd.offset = stringTable.getOffset();
        sdd.values = stringTable.createMap().stream()
                .map(string_val::new)
                .toArray(string_val[]::new);
        return sdd;
    }

    @Override
    public DictionaryForWrite clone() {
        return new DictionaryForWrite(stringTable.clone(), pathTable.clone(), tagsTable.clone());
    }
}
