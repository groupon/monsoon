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
import java.util.ArrayList;
import java.util.Arrays;
import static java.util.Collections.unmodifiableList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/**
 *
 * @author ariane
 */
public final class SimpleGroupPath implements Comparable<SimpleGroupPath>, Path {
    private static final Function<List<String>, SimpleGroupPath> CACHE = CacheBuilder.newBuilder()
            .softValues()
            .build(CacheLoader.from((List<String> pathelems) -> new SimpleGroupPath(pathelems)))::getUnchecked;
    private final List<String> path_;

    /** Use valueOf() instead. */
    private SimpleGroupPath(List<String> path) {
        path_ = unmodifiableList(new ArrayList<>(path));
    }

    public static SimpleGroupPath valueOf(String... path) {
        return valueOf(Arrays.asList(path));
    }

    public static SimpleGroupPath valueOf(List<String> path) {
        try {
            return CACHE.apply(path);
        } catch (UncheckedExecutionException e) {
            if (e.getCause() instanceof RuntimeException)
                throw (RuntimeException)e.getCause();
            throw e;
        }
    }

    @Override
    public List<String> getPath() { return path_; }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 97 * hash + Objects.hashCode(this.path_);
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
        final SimpleGroupPath other = (SimpleGroupPath) obj;
        if (!Objects.equals(this.path_, other.path_)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return getPathString();
    }

    @Override
    public int compareTo(SimpleGroupPath o) {
        if (o == null) return 1;

        int cmp = 0;

        Iterator<String> self_iter = path_.iterator();
        Iterator<String> othr_iter = o.path_.iterator();
        while (cmp == 0 && self_iter.hasNext() && othr_iter.hasNext())
            cmp = self_iter.next().compareTo(othr_iter.next());
        if (cmp == 0)
            cmp = (self_iter.hasNext() ? 1 : (othr_iter.hasNext() ? -1 : 0));

        return cmp;
    }
}
