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
import com.google.common.util.concurrent.ExecutionError;
import com.google.common.util.concurrent.UncheckedExecutionException;
import static com.groupon.lex.metrics.ConfigSupport.maybeQuoteIdentifier;
import com.groupon.lex.metrics.lib.SimpleMapEntry;
import gnu.trove.map.hash.THashMap;
import java.util.Arrays;
import static java.util.Collections.unmodifiableMap;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;

/**
 *
 * @author ariane
 */
public final class Tags implements Comparable<Tags> {
    private static final Function<Key, Tags> CACHE = CacheBuilder.newBuilder()
            .softValues()
            .build(CacheLoader.from((Key in) -> new Tags(in)))::getUnchecked;
    public static final Tags EMPTY = new Tags(new Key(new Entry[]{}));
    private static final float LOAD_FACTOR = 4;
    private final Map<String, MetricValue> tags_;
    private final int hashCode_;

    /** Use valueOf() instead. */
    private Tags(Key tags) {
        tags_ = unmodifiableMap(tags.stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue,
                        (x, y) -> { throw new IllegalStateException("duplicate metric name"); },
                        () -> new THashMap<String, MetricValue>(tags.size(), LOAD_FACTOR))));
        hashCode_ = Objects.hashCode(this.tags_);
    }

    public static Tags valueOf(Stream<Map.Entry<String, MetricValue>> tags) {
        final Key key = new Key(tags
                .map(entry -> new Entry(entry.getKey(), entry.getValue()))
                .toArray(Entry[]::new));
        if (key.isEmpty()) return EMPTY;

        try {
            return CACHE.apply(key);
        } catch (UncheckedExecutionException e) {
            if (e.getCause() instanceof RuntimeException)
                throw (RuntimeException)e.getCause();
            throw e;
        } catch (ExecutionError e) {
            if (e.getCause() instanceof Error)
                throw (Error)e.getCause();
            throw e;
        }
    }

    public static Tags valueOf(Map<String, MetricValue> tags) {
        return valueOf(tags.entrySet().stream());
    }

    public Map<String, MetricValue> asMap() { return tags_; }
    public boolean isEmpty() { return asMap().isEmpty(); }
    public boolean contains(String tag) { return tags_.containsKey(tag); }
    public Iterator<Map.Entry<String, MetricValue>> iterator() { return asMap().entrySet().iterator(); }

    public Optional<MetricValue> getTag(String key) {
        return Optional.ofNullable(asMap().get(key));
    }

    public Stream<Map.Entry<String, MetricValue>> stream() {
        return asMap().entrySet().stream();
    }

    public String getTagString() {
        return stream()
                        .sorted(Comparator.comparing(Map.Entry::getKey))
                        .map((entry) -> tag_as_string_(entry.getKey(), entry.getValue()))
                        .collect(Collectors.joining(", "));
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
        return hashCode_;
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
        if (this.hashCode_ != other.hashCode_) {
            return false;
        }
        if (!Objects.equals(this.tags_, other.tags_)) {
            return false;
        }
        return true;
    }

    @Override
    public int compareTo(Tags other) {
        int cmp = 0;

        Iterator<Map.Entry<String, MetricValue>> self_iter = tags_.entrySet().stream().sorted(Comparator.comparing(Map.Entry::getKey)).iterator();
        Iterator<Map.Entry<String, MetricValue>> othr_iter = other.tags_.entrySet().stream().sorted(Comparator.comparing(Map.Entry::getKey)).iterator();
        while (cmp == 0 && self_iter.hasNext() && othr_iter.hasNext()) {
            Map.Entry<String, MetricValue> self_entry = self_iter.next();
            Map.Entry<String, MetricValue> othr_entry = othr_iter.next();
            cmp = self_entry.getKey().compareTo(othr_entry.getKey());
            if (cmp == 0) cmp = self_entry.getValue().compareTo(othr_entry.getValue());
        }
        if (cmp == 0)
            cmp = (self_iter.hasNext() ? 1 : (othr_iter.hasNext() ? -1 : 0));

        return cmp;
    }

    @Override
    public String toString() { return '{' + getTagString() + '}'; }

    @EqualsAndHashCode
    @Getter
    @ToString
    private final static class Entry {
        @NonNull
        private final String tagName;
        @NonNull
        private final MetricValue tagValue;

        public Entry(@NonNull String tagName, @NonNull MetricValue tagValue) {
            if (tagName.isEmpty())
                throw new IllegalArgumentException("tag with empty name");
            if (!tagValue.isPresent())
                throw new IllegalArgumentException("tag with absent metric value");
            if (tagValue.histogram().isPresent())
                throw new IllegalArgumentException("tag with histogram makes no sense");

            this.tagName = tagName;
            this.tagValue = tagValue;
        }
    }

    @EqualsAndHashCode
    @Getter
    @ToString
    private final static class Key {
        private final String tagName[];
        private final MetricValue tagValue[];

        public Key(@NonNull Entry data[]) {
            final Entry cleanedData[] = Arrays.stream(data)
                    .peek(Objects::requireNonNull)
                    .sorted(Comparator.comparing(Entry::getTagName))
                    .toArray(Entry[]::new);

            tagName = Arrays.stream(cleanedData)
                    .map(Entry::getTagName)
                    .toArray(String[]::new);
            tagValue = Arrays.stream(cleanedData)
                    .map(Entry::getTagValue)
                    .toArray(MetricValue[]::new);
        }

        public Key(Stream<Entry> entries) {
            this(entries.toArray(Entry[]::new));
        }

        public boolean isEmpty() {
            return this.tagName.length == 0;
        }

        public int size() {
            return this.tagName.length;
        }

        public Stream<Map.Entry<String, MetricValue>> stream() {
            return IntStream.range(0, tagName.length)
                    .mapToObj(idx -> SimpleMapEntry.create(tagName[idx], tagValue[idx]));
        }
    }
}
