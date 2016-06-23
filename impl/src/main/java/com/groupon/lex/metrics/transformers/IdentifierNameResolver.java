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

import com.groupon.lex.metrics.NameCache;
import com.groupon.lex.metrics.Path;
import com.groupon.lex.metrics.config.ConfigStatement;
import com.groupon.lex.metrics.timeseries.expression.Context;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import static java.util.Objects.requireNonNull;
import java.util.Optional;
import java.util.function.Function;

/**
 *
 * @author ariane
 */
public class IdentifierNameResolver implements NameResolver {
    public static interface SubSelect extends Function<Path, Path>, ConfigStatement {}

    public static class SubSelectIndex implements SubSelect {
        private final int idx_;

        public SubSelectIndex(int idx) { idx_ = idx; }

        @Override
        public Path apply(Path path) {
            final List<String> elems = path.getPath();
            try {
                final List<String> result = Arrays.asList(elems.get(idx_to_abs_(idx_, elems)));
                return () -> result;
            } catch (IndexOutOfBoundsException ex) {
                return () -> Collections.EMPTY_LIST;
            }
        }

        @Override
        public StringBuilder configString() {
            return new StringBuilder()
                    .append('[')
                    .append(idx_)
                    .append(']');
        }
    }

    public static class SubSelectRange implements SubSelect {
        private final Optional<Integer> b_, e_;

        public SubSelectRange(Optional<Integer> b, Optional<Integer> e) {
            b_ = requireNonNull(b);
            e_ = requireNonNull(e);
        }

        @Override
        public Path apply(Path path) {
            final List<String> elems = path.getPath();
            try {
                final List<String> result = elems.subList(idx_to_abs_(b_, elems).orElse(0), idx_to_abs_(e_, elems).orElseGet(elems::size));
                return () -> result;
            } catch (IndexOutOfBoundsException ex) {
                return () -> Collections.EMPTY_LIST;
            }
        }

        @Override
        public StringBuilder configString() {
            if (!b_.isPresent() && !e_.isPresent()) return new StringBuilder();
            return new StringBuilder()
                    .append('[')
                    .append(b_.map(Object::toString).orElse(""))
                    .append(':')
                    .append(e_.map(Object::toString).orElse(""))
                    .append(']');
        }
    }

    private final String identifier_;
    private final Optional<SubSelect> sub_select_;

    public IdentifierNameResolver(String identifier, Optional<SubSelect> sub_select) {
        identifier_ = requireNonNull(identifier);
        sub_select_ = requireNonNull(sub_select);
    }

    public IdentifierNameResolver(String identifier) {
        this(identifier, Optional.empty());
    }

    @Override
    public Optional<Path> apply(Context ctx) {
        return ctx.getAliasFromIdentifier(Path.class, identifier_)
                .map(path -> sub_select_.map(s -> s.apply(path)).orElseGet(() -> NameCache.singleton.newSimpleGroupPath(path.getPath())));
    }

    @Override
    public StringBuilder configString() {
        return new StringBuilder()
                .append("${")
                .append(identifier_)
                .append(sub_select_.map(SubSelect::configString).orElseGet(StringBuilder::new))
                .append("}");
    }

    private static int idx_to_abs_(int idx, List<?> list) {
        return (idx < 0 ? list.size() + idx : idx);
    }

    private static Optional<Integer> idx_to_abs_(Optional<Integer> idx, List<?> list) {
        return idx.map(i -> idx_to_abs_(i, list));
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 47 * hash + Objects.hashCode(this.identifier_);
        hash = 47 * hash + Objects.hashCode(this.sub_select_);
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
        final IdentifierNameResolver other = (IdentifierNameResolver) obj;
        if (!Objects.equals(this.identifier_, other.identifier_)) {
            return false;
        }
        if (!Objects.equals(this.sub_select_, other.sub_select_)) {
            return false;
        }
        return true;
    }
}
