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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.util.concurrent.UncheckedExecutionException;
import static com.groupon.lex.metrics.ConfigSupport.maybeQuoteIdentifier;
import static java.util.Collections.EMPTY_MAP;
import static java.util.Collections.unmodifiableSortedMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 *
 * @author ariane
 */
public final class Tags implements Comparable<Tags> {
    private static final Function<Map<String, MetricValue>, Tags> CACHE = CacheBuilder.newBuilder()
            .softValues()
            .build(CacheLoader.from((Map<String, MetricValue> in) -> new Tags(in)))::getUnchecked;
    public static final Tags EMPTY = new Tags(EMPTY_MAP);
    private final SortedMap<String, MetricValue> tags_;

    /** Use valueOf() instead. */
    private Tags(Map<String, MetricValue> tags) {
        tags_ = unmodifiableSortedMap(new TreeMap<String, MetricValue>(tags));

        // Validation
        tags_.keySet().forEach(Objects::requireNonNull);
        tags_.values().forEach(Objects::requireNonNull);
        if (!tags_.values().stream().allMatch(MetricValue::isPresent))
            throw new IllegalArgumentException("tag with absent metric value");
        if (tags_.values().stream().map(MetricValue::histogram).anyMatch(Optional::isPresent))
            throw new IllegalArgumentException("tag with histogram makes no sense");
    }

    public static Tags valueOf(Stream<Map.Entry<String, MetricValue>> tags) {
        return valueOf(tags.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
    }

    public static Tags valueOf(Map<String, MetricValue> tags) {
        if (tags.isEmpty()) return EMPTY;
        try {
            return CACHE.apply(tags);
        } catch (UncheckedExecutionException e) {
            if (e.getCause() instanceof RuntimeException)
                throw (RuntimeException)e.getCause();
            throw e;
        }
    }

    public SortedMap<String, MetricValue> asMap() { return tags_; }
    public boolean isEmpty() { return asMap().isEmpty(); }
    public boolean contains(String tag) { return tags_.containsKey(tag); }
    public Iterator<Map.Entry<String, MetricValue>> iterator() { return asMap().entrySet().iterator(); }

    public Optional<MetricValue> getTag(String key) {
        return Optional.ofNullable(asMap().get(key));
    }

    public Stream<Entry<String, MetricValue>> stream() {
        return asMap().entrySet().stream();
    }

    public String getTagString() {
        return String.join(", ", stream()
                        .map((entry) -> tag_as_string_(entry.getKey(), entry.getValue()))
                        .collect(Collectors.toList()));
    }

    private static StringBuilder tag_as_string_(String key, MetricValue value) {
        return maybeQuoteIdentifier(key)
                .append('=')
                .append(value.configString());
    }

    /**
     * Reduce the list of tags, to only those specified in the argument set.
     * @param tag_names The tag names to retain.
     * @return A new instance of tags, which contains only tags mentioned in the argument set.
     */
    public Tags filter(Set<String> tag_names) {
        return Tags.valueOf(tags_.entrySet().stream()
                .filter(entry -> tag_names.contains(entry.getKey())));
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 11 * hash + Objects.hashCode(this.tags_);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final Tags other = (Tags) obj;
        if (!Objects.equals(this.tags_, other.tags_)) {
            return false;
        }
        return true;
    }

    @Override
    public int compareTo(Tags other) {
        int cmp = 0;

        Iterator<Entry<String, MetricValue>> self_iter = tags_.entrySet().iterator();
        Iterator<Entry<String, MetricValue>> othr_iter = other.tags_.entrySet().iterator();
        while (cmp == 0 && self_iter.hasNext() && othr_iter.hasNext()) {
            Entry<String, MetricValue> self_entry = self_iter.next();
            Entry<String, MetricValue> othr_entry = othr_iter.next();
            cmp = self_entry.getKey().compareTo(othr_entry.getKey());
            if (cmp == 0) cmp = self_entry.getValue().compareTo(othr_entry.getValue());
        }
        if (cmp == 0)
            cmp = (self_iter.hasNext() ? 1 : (othr_iter.hasNext() ? -1 : 0));

        return cmp;
    }

    @Override
    public String toString() { return '{' + getTagString() + '}'; }
}
