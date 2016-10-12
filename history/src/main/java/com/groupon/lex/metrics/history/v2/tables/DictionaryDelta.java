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

import com.groupon.lex.metrics.Tags;
import com.groupon.lex.metrics.history.v2.xdr.FromXdr;
import com.groupon.lex.metrics.history.v2.xdr.dictionary_delta;
import com.groupon.lex.metrics.history.v2.xdr.metric_value;
import com.groupon.lex.metrics.history.v2.xdr.path_dictionary_delta;
import com.groupon.lex.metrics.history.v2.xdr.strval_dictionary_delta;
import com.groupon.lex.metrics.history.v2.xdr.tag_dictionary_delta;
import com.groupon.lex.metrics.history.xdr.support.DecodingException;
import com.groupon.lex.metrics.lib.SimpleMapEntry;
import com.groupon.lex.metrics.lib.sequence.ForwardSequence;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import static java.util.Collections.EMPTY_LIST;
import static java.util.Collections.unmodifiableList;
import java.util.List;
import static java.util.Objects.requireNonNull;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;

@EqualsAndHashCode
public class DictionaryDelta {
    private final List<String> stringTable;
    private final List<List<String>> pathTable;
    private final List<Tags> tagsTable;
    @Getter
    private final int stringRefOffset;
    @Getter
    private final int pathRefOffset;
    @Getter
    private final int tagsRefOffset;

    public DictionaryDelta() {
        stringTable = EMPTY_LIST;
        pathTable = EMPTY_LIST;
        tagsTable = EMPTY_LIST;
        stringRefOffset = 0;
        pathRefOffset = 0;
        tagsRefOffset = 0;
    }

    public DictionaryDelta(dictionary_delta input) {
        this(input, null);
    }

    public DictionaryDelta(ArrayList<String> stringTable, ArrayList<List<String>> pathTable, ArrayList<Tags> tagsTable) {
        this.stringTable = stringTable;
        this.pathTable = pathTable;
        this.tagsTable = tagsTable;
        this.stringRefOffset = 0;
        this.pathRefOffset = 0;
        this.tagsRefOffset = 0;
    }

    public DictionaryDelta(@NonNull dictionary_delta input, DictionaryDelta parent) {
        if (parent == null) {
            stringRefOffset = input.sdd.offset;
            pathRefOffset = input.pdd.offset;
            tagsRefOffset = input.tdd.offset;
        } else {
            stringRefOffset = parent.stringRefOffset;
            pathRefOffset = parent.pathRefOffset;
            tagsRefOffset = parent.tagsRefOffset;
        }

        stringTable = unmodifiableList(decode(stringRefOffset, input.sdd, (parent == null ? null : parent.stringTable)));
        pathTable = unmodifiableList(decode(pathRefOffset, input.pdd, (parent == null ? null : parent.pathTable), this::getString));
        tagsTable = unmodifiableList(decode(tagsRefOffset, input.tdd, (parent == null ? null : parent.tagsTable), this::getString));
    }

    public int getStringRefEnd() {
        return stringRefOffset + stringTable.size();
    }

    public int getPathRefEnd() {
        return pathRefOffset + pathTable.size();
    }

    public int getTagsRefEnd() {
        return tagsRefOffset + tagsTable.size();
    }

    private static List<String> decode(int offset, strval_dictionary_delta sdd, List<String> parent) {
        final ArrayList<String> stringTable = new ArrayList<>();
        if (parent != null)
            stringTable.addAll(parent);
        if (offset + stringTable.size() != sdd.offset)
            throw new DecodingException("dictionary delta increments do not line up");

        Arrays.stream(sdd.values)
                .map(v -> v.value)
                .forEachOrdered(stringTable::add);
        stringTable.trimToSize();
        return stringTable;
    }

    private static List<List<String>> decode(int offset, path_dictionary_delta pdd, List<List<String>> parent, IntFunction<String> stringLookup) {
        final ArrayList<List<String>> pathTable = new ArrayList<>();
        if (parent != null)
            pathTable.addAll(parent);
        if (offset + pathTable.size() != pdd.offset)
            throw new DecodingException("dictionary delta increments do not line up");

        Arrays.stream(pdd.values)
                .map(v -> v.value)
                .map(vArr -> {
                    return Arrays.stream(vArr)
                            .mapToObj(str_ref -> stringLookup.apply(str_ref))
                            .collect(Collectors.toCollection(ArrayList::new));
                })
                .peek(ArrayList::trimToSize)
                .map(Collections::unmodifiableList)
                .forEachOrdered(pathTable::add);
        pathTable.trimToSize();
        return pathTable;
    }

    private static List<Tags> decode(int offset, tag_dictionary_delta tdd, List<Tags> parent, IntFunction<String> stringLookup) {
        final ArrayList<Tags> tagsTable = new ArrayList<>();
        if (parent != null)
            tagsTable.addAll(parent);
        if (offset + tagsTable.size() != tdd.offset)
            throw new DecodingException("dictionary delta increments do not line up");

        Arrays.stream(tdd.values)
                .map(current -> {
                    return Tags.valueOf(new ForwardSequence(0, current.str_ref.length).stream()
                            .mapToObj(idx -> {
                                final int str_ref = current.str_ref[idx];
                                final metric_value value = current.value[idx];
                                return SimpleMapEntry.create(stringLookup.apply(str_ref), FromXdr.metricValue(value, stringLookup));
                            }));
                })
                .forEachOrdered(tagsTable::add);
        tagsTable.trimToSize();
        return tagsTable;
    }

    public String getString(int ref) {
        if (ref < stringRefOffset || ref - stringRefOffset >= stringTable.size())
            throw new IllegalArgumentException("Invalid string reference (" + ref + " not between " + stringRefOffset + " and " + (stringRefOffset + stringTable.size()) + ")");
        return requireNonNull(stringTable.get(ref - stringRefOffset), "Programmer error: elements in dictionary should not be null");
    }

    public Tags getTags(int ref) {
        if (ref < tagsRefOffset || ref - tagsRefOffset >= tagsTable.size())
            throw new IllegalArgumentException("Invalid tags reference (" + ref + " not between " + tagsRefOffset + " and " + (tagsRefOffset + tagsTable.size()) + ")");
        return requireNonNull(tagsTable.get(ref - tagsRefOffset), "Programmer error: elements in dictionary should not be null");
    }

    public List<String> getPath(int ref) {
        if (ref < pathRefOffset || ref - pathRefOffset >= pathTable.size())
            throw new IllegalArgumentException("Invalid path reference (" + ref + " not between " + pathRefOffset + " and " + (pathRefOffset + pathTable.size()) + ")");
        return requireNonNull(pathTable.get(ref - pathRefOffset), "Programmer error: elements in dictionary should not be null");
    }
}
