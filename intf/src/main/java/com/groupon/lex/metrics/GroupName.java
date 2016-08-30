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
import java.io.Serializable;
import static java.util.Collections.emptyMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Stream;
import lombok.Value;

/**
 *
 * @author ariane
 */
public final class GroupName implements Serializable, Comparable<GroupName>, Tagged {
    private static final Function<GroupNameArgs, GroupName> CACHE = CacheBuilder.newBuilder()
            .softValues()
            .build(CacheLoader.from((GroupNameArgs in) -> new GroupName(in.getPath(), in.getTags())))::getUnchecked;
    private final SimpleGroupPath path_;
    private final Tags tags_;

    /** Use valueOf() instead. */
    private GroupName(SimpleGroupPath path, Tags tags) {
        path_ = Objects.requireNonNull(path);
        tags_ = Objects.requireNonNull(tags);
    }

    public static GroupName valueOf(String... path) {
        return valueOf(SimpleGroupPath.valueOf(path));
    }

    public static GroupName valueOf(SimpleGroupPath path) {
        return valueOf(path, emptyMap());
    }

    public static GroupName valueOf(SimpleGroupPath path, Tags tags) {
        try {
            return CACHE.apply(new GroupNameArgs(path, tags));
        } catch (UncheckedExecutionException e) {
            if (e.getCause() instanceof RuntimeException)
                throw (RuntimeException)e.getCause();
            throw e;
        }
    }

    public static GroupName valueOf(SimpleGroupPath path, Map<String, MetricValue> tags) {
        return valueOf(path, Tags.valueOf(tags));
    }

    public static GroupName valueOf(SimpleGroupPath path, Stream<Map.Entry<String, MetricValue>> tags) {
        return valueOf(path, Tags.valueOf(tags));
    }

    @Override
    public Tags getTags() { return tags_; }
    public SimpleGroupPath getPath() { return path_; }

    public String getPathString() {
        return path_.getPathString();
    }

    public StringBuilder configString() {
        final StringBuilder result = new StringBuilder(getPathString());

        String tag_string = tags_.getTagString();
        if (!tag_string.isEmpty())
            result.append('{').append(tag_string).append('}');
        return result;
    }

    @Override
    public String toString() {
        return configString().toString();
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 53 * hash + Objects.hashCode(this.path_);
        hash = 53 * hash + Objects.hashCode(this.tags_);
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
        final GroupName other = (GroupName) obj;
        if (!Objects.equals(this.path_, other.path_)) {
            return false;
        }
        if (!Objects.equals(this.tags_, other.tags_)) {
            return false;
        }
        return true;
    }

    @Override
    public int compareTo(GroupName o) {
        if (o == null) return 1;

        int cmp = 0;

        if (cmp == 0)
            cmp = getPath().compareTo(o.getPath());
        if (cmp == 0)
            cmp = getTags().compareTo(o.getTags());

        return cmp;
    }

    @Value
    private static class GroupNameArgs {
        SimpleGroupPath path;
        Tags tags;
    }
}
