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
package com.groupon.lex.metrics.timeseries;

import static com.groupon.lex.metrics.ConfigSupport.maybeQuoteIdentifier;
import com.groupon.lex.metrics.Tags;
import com.groupon.lex.metrics.lib.SimpleMapEntry;
import static com.groupon.lex.metrics.timeseries.TagClause.mismatchingKeys;
import java.util.ArrayList;
import java.util.Collection;
import static java.util.Collections.unmodifiableSet;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Getter;

@Getter
public class WithoutTagAggregationClause implements TagAggregationClause {
    private final Set<String> tags;

    public WithoutTagAggregationClause(Collection<String> tags) {
        this.tags = unmodifiableSet(new HashSet<>(tags));
    }

    @Override
    public boolean isScalar() { return false; }

    private Tags key_(Tags in) {
        return mismatchingKeys(in, tags);
    }

    @Override
    public <X, R> Map<Tags, Collection<R>> apply(Stream<X> x_stream, Function<? super X, Tags> x_tag_fn, Function<? super X, R> x_map_fn) {
        return x_stream
                .map(x -> SimpleMapEntry.create(x_tag_fn.apply(x), x_map_fn.apply(x)))
                .filter(entry -> tags.stream().allMatch(entry.getKey()::contains))
                .collect(Collectors.groupingBy(
                        entry -> key_(entry.getKey()),
                        Collectors.mapping(Map.Entry::getValue, Collectors.toCollection(() -> (Collection<R>)new ArrayList()))));
    }

    @Override
    public <X, Y, R> Map<Tags, Collection<R>> apply(Stream<X> x_stream, Stream<Y> y_stream,
            Function<? super X, Tags> x_tag_fn, Function<? super Y, Tags> y_tag_fn,
            Function<? super X, R> x_map_fn, Function<? super Y, R> y_map_fn) {
        return Stream.concat(
                x_stream.map(x -> SimpleMapEntry.create(x_tag_fn.apply(x), x_map_fn.apply(x))),
                y_stream.map(y -> SimpleMapEntry.create(y_tag_fn.apply(y), y_map_fn.apply(y))))
                .filter(entry -> tags.stream().allMatch(entry.getKey()::contains))
                .collect(Collectors.groupingBy(
                        entry -> key_(entry.getKey()),
                        Collectors.mapping(entry -> entry.getValue(), Collectors.toCollection(() -> (Collection<R>)new ArrayList()))));
    }

    @Override
    public StringBuilder configString() {
        StringBuilder result = new StringBuilder("without (");
        tags.stream().sorted().forEachOrdered(tag -> result.append(maybeQuoteIdentifier(tag)).append(", "));
        result.replace(result.length() - 2, result.length(), ")");
        return result;
    }
}
