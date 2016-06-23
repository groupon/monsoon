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
package com.groupon.lex.metrics.collector.httpget;

import com.groupon.lex.metrics.TupledElements;
import com.groupon.lex.metrics.lib.StringTemplate;
import com.groupon.lex.metrics.GroupName;
import com.groupon.lex.metrics.MetricValue;
import com.groupon.lex.metrics.NameCache;
import com.groupon.lex.metrics.SimpleGroupPath;
import com.groupon.lex.metrics.lib.Any2;
import com.groupon.lex.metrics.lib.SimpleMapEntry;
import java.util.ArrayList;
import java.util.Collection;
import static java.util.Collections.unmodifiableCollection;
import java.util.Comparator;
import java.util.Map;
import java.util.Map.Entry;
import static java.util.Objects.requireNonNull;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 *
 * @author ariane
 */
public class UrlPattern {
    private final StringTemplate url_template_;
    private final Collection<TupledElements> template_args_ = new ArrayList<>();

    public UrlPattern(StringTemplate url_template, Collection<TupledElements> template_args) {
        url_template_ = requireNonNull(url_template);
        if (template_args.size() < url_template_.indexSize())
            throw new IllegalArgumentException("Insufficient arguments, need at least " + url_template_.indexSize() + " arguments");

        /* Only store elements that are actually used. */
        template_args.forEach(template_args_::add);
    }

    public UrlPattern(String url_template, Collection<TupledElements> template_args) {
        this(StringTemplate.fromString(url_template), template_args);
    }

    public StringTemplate getUrlTemplate() { return url_template_; }
    public Collection<TupledElements> getTemplateArgs() { return unmodifiableCollection(template_args_); }

    private Entry<GroupName, String> apply_args_(Map<Any2<String, Integer>, String> args) {
        final SimpleGroupPath path = NameCache.singleton.newSimpleGroupPath(args.entrySet().stream()
                .map(e -> e.getKey().getRight().map(k -> SimpleMapEntry.create(k, e.getValue())))
                .flatMap(o -> o.map(Stream::of).orElseGet(Stream::empty))
                .sorted(Comparator.comparing(Entry::getKey))
                .map(Entry::getValue)
                .collect(Collectors.toList()));
        final Stream<Entry<String, MetricValue>> tagMap = args.entrySet().stream()
                .map(e -> e.getKey().getLeft().map(k -> SimpleMapEntry.create(k, MetricValue.fromStrValue(e.getValue()))))
                .flatMap(o -> o.map(Stream::of).orElseGet(Stream::empty));
        return SimpleMapEntry.create(NameCache.singleton.newGroupName(path, tagMap), url_template_.apply(args));
    }

    public Stream<Map.Entry<GroupName, String>> getUrls() {
        return TupledElements.cartesianProduct(template_args_.stream())
                .map(this::apply_args_);
    }

    @Override
    public String toString() { return url_template_.toString(); }
}
