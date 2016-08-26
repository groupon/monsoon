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
package com.groupon.lex.metrics;

import com.groupon.lex.metrics.lib.Any2;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A set of tuples, with multiple values associated with them.
 *
 * Example:
 *   (x, y) = [ ("a", "b"), ("c", "d") ]
 */
@Deprecated
public class TupledElements {
    private final List<Any2<String, Integer>> key_;
    private final Set<List<String>> values_ = new HashSet<>();

    public TupledElements(List<Any2<String, Integer>> key) {
        key_ = Collections.unmodifiableList(new ArrayList<>(key));
    }

    public int size() {
        return key_.size();
    }

    public List<Any2<String, Integer>> getKeys() {
        return key_;
    }

    public Stream<List<String>> getValues() {
        return values_.stream().map(Collections::unmodifiableList);
    }

    public void addValues(List<String> values) {
        if (values.size() != size()) {
            throw new IllegalArgumentException("incorrect number of values");
        }
        values_.add(new ArrayList<>(values));
    }

    public Stream<Map<Any2<String, Integer>, String>> stream() {
        return values_.stream().map((List<String> values) -> {
            final Map<Any2<String, Integer>, String> rv = new HashMap<>();
            final Iterator<Any2<String, Integer>> k = key_.iterator();
            final Iterator<String> v = values.iterator();
            for (int i = 0; i < size(); ++i) {
                rv.put(k.next(), v.next());
            }
            assert (!k.hasNext());
            assert (!v.hasNext());
            return rv;
        });
    }

    private static Map<Any2<String, Integer>, String> flatten_(Stream<Map<Any2<String, Integer>, String>> elem) {
        return elem.map(Map::entrySet).flatMap(Collection::stream).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public static Stream<Map<Any2<String, Integer>, String>> cartesianProduct(List<TupledElements> elems) {
        if (elems.isEmpty()) {
            return Stream.of(Collections.emptyMap());
        }
        return elems.get(0).stream().flatMap((Map<Any2<String, Integer>, String> m0) -> {
            return cartesianProduct(elems.subList(1, elems.size())).map((Map<Any2<String, Integer>, String> m1) -> flatten_(Stream.of(m0, m1)));
        });
    }

    public static Stream<Map<Any2<String, Integer>, String>> cartesianProduct(Stream<TupledElements> elems) {
        return cartesianProduct(elems.collect(Collectors.toList()));
    }

    private static StringBuffer arg_config_string_(TupledElements arg_set) {
        if (arg_set.size() == 1) {
            return new StringBuffer()
                    .append(arg_set.getKeys().get(0)
                            .<CharSequence>mapCombine(ConfigSupport::maybeQuoteIdentifier, (Integer x) -> x.toString()))
                    .append(" = [")
                    .append(String.join(", ",
                            arg_set.getValues()
                                    .map(list -> list.get(0))
                                    .map(ConfigSupport::quotedString)
                                    .collect(Collectors.toList())))
                    .append("]");
        }

        return new StringBuffer()
                .append("(")
                .append(String.join(", ",
                        arg_set.getKeys().stream()
                                .map(e -> e.<CharSequence>mapCombine(ConfigSupport::maybeQuoteIdentifier, (Integer x) -> x.toString()))
                                .collect(Collectors.toList())
                        ))
                .append(")")
                .append(" = ")
                .append("[")
                .append(String.join(", ",  // Concatenate all tuples.
                        arg_set.getValues()
                                .map(list -> {
                                    // Render a tuple.
                                    return new StringBuilder("(")
                                            .append(String.join(", ", list.stream().map(ConfigSupport::quotedString).collect(Collectors.toList())))
                                            .append(')');
                                })
                                .collect(Collectors.toList())
                        ))
                .append("]");
    }

    public static StringBuffer config_string_for_args(String prefix, Collection<TupledElements> arg_sets) {
        StringBuffer buf = new StringBuffer();
        for (Iterator<TupledElements> i = arg_sets.iterator(); i.hasNext(); ) {
            buf
                    .append(prefix)
                    .append(arg_config_string_(i.next()));
            if (i.hasNext()) buf.append(',');
            buf.append("\n");
        }
        return buf;
    }
}
