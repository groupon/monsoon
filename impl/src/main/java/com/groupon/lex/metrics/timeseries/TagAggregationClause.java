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

import com.groupon.lex.metrics.Tags;
import com.groupon.lex.metrics.config.ConfigStatement;
import java.util.Collection;
import static java.util.Collections.EMPTY_LIST;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 *
 * @author ariane
 */
public interface TagAggregationClause extends ConfigStatement, TagClause {
    public static TagAggregationClause DEFAULT = new ByTagAggregationClause(EMPTY_LIST, false);
    public static TagAggregationClause KEEP_COMMON = new ByTagAggregationClause(EMPTY_LIST, true);

    public static TagAggregationClause by(Collection<String> tags, boolean keep_common) {
        return new ByTagAggregationClause(tags, keep_common);
    }

    public static TagAggregationClause without(Collection<String> tags) {
        return new WithoutTagAggregationClause(tags);
    }

    public boolean isScalar();

    public <X, R> Map<Tags, Collection<R>> apply(Stream<X> x_stream,
            Function<? super X, Tags> x_tag_fn,
            Function<? super X, R> x_map_fn);
    public <X, Y, R> Map<Tags, Collection<R>> apply(Stream<X> x_stream, Stream<Y> y_stream,
            Function<? super X, Tags> x_tag_fn, Function<? super Y, Tags> y_tag_fn,
            Function<? super X, R> x_map_fn, Function<? super Y, R> y_map_fn);
}
