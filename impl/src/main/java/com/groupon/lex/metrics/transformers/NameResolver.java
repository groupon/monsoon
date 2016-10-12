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
package com.groupon.lex.metrics.transformers;

import com.groupon.lex.metrics.Path;
import com.groupon.lex.metrics.config.ConfigStatement;
import com.groupon.lex.metrics.timeseries.expression.Context;
import static com.groupon.lex.metrics.timeseries.expression.Util.pairwiseMap;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import static java.util.Objects.requireNonNull;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 *
 * @author ariane
 */
public interface NameResolver extends Function<Context<?>, Optional<Path>>, ConfigStatement {
    @Override
    public Optional<Path> apply(Context<?> ctx);

    /**
     * Combine two group name resolvers into a single group name resolver.
     * @param a The prefix of the resulting group name.
     * @param b The suffix of the resulting group name.
     * @return A groupname resolver concatenating the result of both resolvers.
     */
    public static NameResolver combine(NameResolver a, NameResolver b) {
        return new CombinedNameResolver(a, b);
    }

    /**
     * A groupname resolver that concatenates the result of its two component group name resolvers.
     */
    public static class CombinedNameResolver implements NameResolver {
        private final NameResolver a_, b_;

        public CombinedNameResolver(NameResolver a, NameResolver b) {
            a_ = requireNonNull(a);
            b_ = requireNonNull(b);
        }

        @Override
        public Optional<Path> apply(Context ctx) {
            return pairwiseMap(a_.apply(ctx), b_.apply(ctx), (a_name, b_name) -> {
                final List<String> path = Stream.of(a_name, b_name)
                        .map(Path::getPath)
                        .flatMap(Collection::stream)
                        .collect(Collectors.toList());
                final Path result = () -> path;
                return result;
            });
        }

        @Override
        public StringBuilder configString() {
            return a_.configString()
                    .append('.')
                    .append(b_.configString());
        }

        @Override
        public int hashCode() {
            int hash = 7;
            hash = 47 * hash + Objects.hashCode(this.a_);
            hash = 47 * hash + Objects.hashCode(this.b_);
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
            final CombinedNameResolver other = (CombinedNameResolver) obj;
            if (!Objects.equals(this.a_, other.a_)) {
                return false;
            }
            if (!Objects.equals(this.b_, other.b_)) {
                return false;
            }
            return true;
        }

        @Override
        public String toString() {
            return configString().toString();
        }
    }
}
