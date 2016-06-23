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
import java.util.Collection;
import static java.util.Collections.unmodifiableSet;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 *
 * @author ariane
 */
public class ByTagMatchingClause implements TagMatchingClause {
    private final Set<String> tags_;
    private final boolean keep_common_;

    public ByTagMatchingClause(Collection<String> tags, boolean keep_common) {
        tags_ = unmodifiableSet(new HashSet<>(tags));
        keep_common_ = keep_common;
    }

    private <T> Map<Tags, Map.Entry<Tags, T>> collect_(Stream<T> stream, Function<? super T, Tags> tag_fn) {
        return stream
                .map(v -> SimpleMapEntry.create(tag_fn.apply(v), v))
                .collect(Collectors.groupingBy(tagged_v -> TagClause.matchingKeys(tagged_v.getKey(), tags_))).entrySet().stream()
                .filter(keyed_val -> keyed_val.getValue().size() == 1)
                .map(keyed_val -> SimpleMapEntry.create(keyed_val.getKey(), keyed_val.getValue().get(0)))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    @Override
    public <X, Y, R> Map<Tags, R> apply(Stream<X> x_stream, Stream<Y> y_stream, Function<? super X, Tags> x_tag_fn, Function<? super Y, Tags> y_tag_fn, BiFunction<? super X, ? super Y, ? extends R> map_fn) {
        final Map<Tags, Map.Entry<Tags, X>> x_map = collect_(x_stream, x_tag_fn);
        final Map<Tags, Map.Entry<Tags, Y>> y_map = collect_(y_stream, y_tag_fn);

        return x_map.entrySet().stream()
                .map(keyed_x -> {
                    final Tags key = keyed_x.getKey();
                    final Map.Entry<Tags, X> x = keyed_x.getValue();
                    return Optional.ofNullable(y_map.get(key))
                            .map(y -> {
                                final R rv = map_fn.apply(x.getValue(), y.getValue());
                                final Tags combined_key;
                                if (keep_common_)
                                    combined_key = TagClause.keepCommonTags(x.getKey(), y.getKey());
                                else
                                    combined_key = key;
                                return SimpleMapEntry.create(combined_key, rv);
                            });
                })
                .flatMap(opt_rv -> opt_rv.map(Stream::of).orElseGet(Stream::empty))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    @Override
    public StringBuilder configString() {
        final StringBuilder result = new StringBuilder();

        if (!tags_.isEmpty()) {
            result.append("by (");
            tags_.stream().sorted().forEachOrdered(tag -> result.append(maybeQuoteIdentifier(tag)).append(", "));
            result.replace(result.length() - 2, result.length(), ")");
        }

        if (keep_common_) {
            if (result.length() != 0) result.append(' ');  // Separator
            result.append("keep_common");
        }

        return result;
    }
}
