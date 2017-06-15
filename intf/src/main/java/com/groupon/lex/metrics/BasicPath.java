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
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.ExecutionError;
import com.google.common.util.concurrent.UncheckedExecutionException;
import java.util.Arrays;
import static java.util.Collections.unmodifiableList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;

/**
 * Contains the code shared between SimpleGroupPath and MetricName.
 * @author ariane
 */
class BasicPath<Self extends BasicPath> implements Path, Comparable<Self> {
    private final String path[];
    private final int hashCode_;

    protected BasicPath(PathArray path) {
        this.path = path.getPath();
        this.hashCode_ = Arrays.deepHashCode(this.path);
    }

    @Override
    public final List<String> getPath() {
        return unmodifiableList(Arrays.asList(path));
    }

    @Override
    public String toString() {
        return configString().toString();
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
        final BasicPath other = (BasicPath) obj;
        if (this.hashCode_ != other.hashCode_) {
            return false;
        }
        if (!Arrays.deepEquals(this.path, other.path)) {
            return false;
        }
        return true;
    }

    @Override
    public int compareTo(Self o) {
        if (o == null) return 1;
        int cmp = 0;

        final Iterator<String> self_iter = Iterators.forArray(path);
        final Iterator<String> othr_iter = Iterators.forArray(((BasicPath)o).path);
        while (cmp == 0 && self_iter.hasNext() && othr_iter.hasNext())
            cmp = self_iter.next().compareTo(othr_iter.next());
        if (cmp == 0)
            cmp = (self_iter.hasNext() ? 1 : othr_iter.hasNext() ? -1 : 0);

        return cmp;
    }

    protected static <T extends BasicPath> Function<PathArray, T> makeCacheFunction(Function<PathArray, T> constructor) {
        final LoadingCache<PathArray, T> cache = CacheBuilder.newBuilder()
                .softValues()
                .build(CacheLoader.from(constructor::apply));

        return (PathArray path) -> {
            try {
                return cache.getUnchecked(path);
            } catch (UncheckedExecutionException e) {
                if (e.getCause() instanceof RuntimeException)
                    throw (RuntimeException)e.getCause();
                throw e;
            } catch (ExecutionError e) {
                if (e.getCause() instanceof Error)
                    throw (Error)e.getCause();
                throw e;
            }
        };
    }

    @EqualsAndHashCode
    @ToString
    @Getter
    protected static final class PathArray {
        @NonNull
        private final String[] path;

        public PathArray(@NonNull String[] path) {
            Arrays.stream(path).forEach(Objects::nonNull);
            this.path = Arrays.copyOf(path, path.length);
        }

        public PathArray(@NonNull List<String> path) {
            this(path.toArray(new String[path.size()]));
        }
    }
}
