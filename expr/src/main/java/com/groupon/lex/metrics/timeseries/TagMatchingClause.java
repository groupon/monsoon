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

import com.groupon.lex.metrics.MetricValue;
import com.groupon.lex.metrics.Tags;
import com.groupon.lex.metrics.lib.SimpleMapEntry;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 *
 * @author ariane
 */
public interface TagMatchingClause extends TagClause {
    public static final TagMatchingClause DEFAULT = new TagMatchingClause() {
        @Override
        public <X, Y, R> Map<Tags, R> apply(Stream<X> x_stream, Stream<Y> y_stream, Function<? super X, Tags> x_tag_fn, Function<? super Y, Tags> y_tag_fn, BiFunction<? super X, ? super Y, ? extends R> map_fn) {
            final Map<Tags, Y> y_map = y_stream.collect(Collectors.toMap(y_tag_fn, Function.identity()));
            return x_stream
                    .flatMap((x) -> {
                        final Tags key = x_tag_fn.apply(x);
                        return Optional.ofNullable(y_map.get(key))
                                .map(y -> map_fn.apply(x, y))
                                .map(rv -> SimpleMapEntry.create(key, rv))
                                .map(Stream::of)
                                .orElseGet(Stream::empty);
                    })
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        }

        @Override
        public StringBuilder configString() {
            return new StringBuilder();
        }
    };

    public static ByTagMatchingClause by(Collection<String> tags, boolean keep_common) {
        return new ByTagMatchingClause(tags, keep_common);
    }

    public <X, Y, R> Map<Tags, R> apply(Stream<X> x_stream, Stream<Y> y_stream,
                                        Function<? super X, Tags> x_tag_fn,
                                        Function<? super Y, Tags> y_tag_fn,
                                        BiFunction<? super X, ? super Y, ? extends R> map_fn);

    public default TimeSeriesMetricDeltaSet applyOptional(TimeSeriesMetricDeltaSet x_values, TimeSeriesMetricDeltaSet y_values, BiFunction<? super MetricValue, ? super MetricValue, Optional<? extends MetricValue>> fn) {
        if (x_values.isScalar() && y_values.isScalar()) {
            return fn.apply(x_values.asScalar().get(), y_values.asScalar().get())
                    .map(TimeSeriesMetricDeltaSet::new)
                    .orElseGet(TimeSeriesMetricDeltaSet::new);
        }

        final Stream<Map.Entry<Tags, MetricValue>> x_stream = x_values.streamAsMap(y_values.getTags());
        final Stream<Map.Entry<Tags, MetricValue>> y_stream = y_values.streamAsMap(x_values.getTags());
        return new TimeSeriesMetricDeltaSet(apply(x_stream, y_stream, Map.Entry::getKey, Map.Entry::getKey, (x, y) -> fn.apply(x.getValue(), y.getValue()))
                .entrySet().stream()
                .map(tag_val -> tag_val.getValue().map(val -> SimpleMapEntry.create(tag_val.getKey(), val)))
                .flatMap(opt -> opt.map(Stream::of).orElseGet(Stream::empty)));
    }
}
