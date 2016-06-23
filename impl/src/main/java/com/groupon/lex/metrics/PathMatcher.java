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
import com.groupon.lex.metrics.config.ConfigStatement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import static java.util.Objects.requireNonNull;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import lombok.Value;

/**
 *
 * @author ariane
 */
public class PathMatcher implements ConfigStatement {
    private static final Logger LOG = Logger.getLogger(PathMatcher.class.getName());
    private final IdentifierMatch matcher_;
    private final LoadingCache<List<String>, CachedOutcome> match_cache_;

    /** Wrapped boolean inside a container class, so the SoftReferences work properly. */
    @Value
    private static class CachedOutcome {
        private final boolean matched;
    }

    public static interface IdentifierMatch {
        public static class SkipBacktrack {
            public boolean skip = false;
        }

        public boolean match(List<String> path, SkipBacktrack skipper);
        public IdentifierMatch rebindWithSuccessor(Optional<IdentifierMatch> successor);
        public default IdentifierMatch rebindWithSuccessor(IdentifierMatch successor) { return rebindWithSuccessor(Optional.of(successor)); }
        public StringBuilder populateExpression(StringBuilder buf);
    }

    public static class LiteralNameMatch implements IdentifierMatch {
        private final String literal_;
        private final Optional<IdentifierMatch> successor_;

        public LiteralNameMatch(String literal) { this(literal, Optional.empty()); }

        public LiteralNameMatch(String literal, Optional<IdentifierMatch> successor) {
            literal_ = Objects.requireNonNull(literal);
            successor_ = Objects.requireNonNull(successor);
        }

        @Override
        public boolean match(List<String> path, SkipBacktrack skipper) {
            if (path.isEmpty()) return false;
            if (!literal_.equals(path.get(0))) return false;
            return successor_
                    .map((succ) -> succ.match(path.subList(1, path.size()), skipper))
                    .orElse(path.size() == 1);
        }

        @Override
        public LiteralNameMatch rebindWithSuccessor(Optional<IdentifierMatch> successor) {
            return new LiteralNameMatch(literal_, successor);
        }

        @Override
        public StringBuilder populateExpression(StringBuilder buf) {
            buf.append(ConfigSupport.maybeQuoteIdentifier(literal_));
            return successor_
                    .map((succ) -> succ.populateExpression(buf.append('.')))
                    .orElse(buf);
        }

        @Override
        public String toString() {
            return populateExpression(new StringBuilder()).toString();
        }

        @Override
        public int hashCode() {
            int hash = 5;
            hash = 97 * hash + Objects.hashCode(this.literal_);
            hash = 97 * hash + Objects.hashCode(this.successor_);
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
            final LiteralNameMatch other = (LiteralNameMatch) obj;
            if (!Objects.equals(this.literal_, other.literal_)) {
                return false;
            }
            if (!Objects.equals(this.successor_, other.successor_)) {
                return false;
            }
            return true;
        }
    }

    public static class WildcardMatch implements IdentifierMatch {
        private final Optional<IdentifierMatch> successor_;

        public WildcardMatch() { this(Optional.empty()); }

        public WildcardMatch(Optional<IdentifierMatch> successor) {
            successor_ = Objects.requireNonNull(successor);
        }

        @Override
        public boolean match(List<String> path, SkipBacktrack skipper) {
            if (path.isEmpty()) return false;
            return successor_
                    .map((succ) -> succ.match(path.subList(1, path.size()), skipper))
                    .orElse(path.size() == 1);
        }

        @Override
        public WildcardMatch rebindWithSuccessor(Optional<IdentifierMatch> successor) {
            return new WildcardMatch(successor);
        }

        @Override
        public StringBuilder populateExpression(StringBuilder buf) {
            buf.append('*');
            return successor_
                    .map((succ) -> succ.populateExpression(buf.append('.')))
                    .orElse(buf);
        }

        @Override
        public String toString() {
            return populateExpression(new StringBuilder()).toString();
        }

        @Override
        public int hashCode() {
            int hash = 7;
            hash = 83 * hash + Objects.hashCode(this.successor_);
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
            final WildcardMatch other = (WildcardMatch) obj;
            if (!Objects.equals(this.successor_, other.successor_)) {
                return false;
            }
            return true;
        }
    }

    public static class DoubleWildcardMatch implements IdentifierMatch {
        private final Optional<IdentifierMatch> successor_;

        public DoubleWildcardMatch() { this(Optional.empty()); }

        public DoubleWildcardMatch(Optional<IdentifierMatch> successor) {
            successor_ = Objects.requireNonNull(successor);
        }

        @Override
        public boolean match(List<String> path, SkipBacktrack skipper) {
            skipper.skip = true;

            SkipBacktrack my_skipper = new SkipBacktrack();
            if (!successor_.isPresent()) return true;
            for (int i = 0; i < path.size(); ++i) {
                if (successor_.get().match(path.subList(i, path.size()), my_skipper))
                    return true;
                if (my_skipper.skip) return false;  // Nested expression has done all the backtracking it needs.
            }
            return false;  // Successor couldn't match.
        }

        @Override
        public DoubleWildcardMatch rebindWithSuccessor(Optional<IdentifierMatch> successor) {
            return new DoubleWildcardMatch(successor);
        }

        @Override
        public StringBuilder populateExpression(StringBuilder buf) {
            buf.append("**");
            return successor_
                    .map((succ) -> succ.populateExpression(buf.append('.')))
                    .orElse(buf);
        }

        @Override
        public String toString() {
            return populateExpression(new StringBuilder()).toString();
        }

        @Override
        public int hashCode() {
            int hash = 7;
            hash = 97 * hash + Objects.hashCode(this.successor_);
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
            final DoubleWildcardMatch other = (DoubleWildcardMatch) obj;
            if (!Objects.equals(this.successor_, other.successor_)) {
                return false;
            }
            return true;
        }
    }

    public static class RegexMatch implements IdentifierMatch {
        private final String regex_;
        private final Predicate<String> match_;
        private final Optional<IdentifierMatch> successor_;

        public RegexMatch(String regex) { this(regex, Optional.empty()); }

        public RegexMatch(String regex, Optional<IdentifierMatch> successor) {
            regex_ = requireNonNull(regex);
            match_ = Pattern.compile(regex).asPredicate();
            successor_ = Objects.requireNonNull(successor);
        }

        @Override
        public boolean match(List<String> path, SkipBacktrack skipper) {
            if (path.isEmpty()) return false;
            if (!match_.test(path.get(0))) return false;
            return successor_
                    .map((succ) -> succ.match(path.subList(1, path.size()), skipper))
                    .orElse(path.size() == 1);
        }

        @Override
        public RegexMatch rebindWithSuccessor(Optional<IdentifierMatch> successor) {
            return new RegexMatch(regex_, successor);
        }

        @Override
        public StringBuilder populateExpression(StringBuilder buf) {
            buf.append(ConfigSupport.regex(regex_));
            return successor_
                    .map((succ) -> succ.populateExpression(buf.append('.')))
                    .orElse(buf);
        }

        @Override
        public String toString() {
            return populateExpression(new StringBuilder()).toString();
        }

        @Override
        public int hashCode() {
            int hash = 5;
            hash = 97 * hash + Objects.hashCode(this.regex_);
            hash = 97 * hash + Objects.hashCode(this.successor_);
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
            final RegexMatch other = (RegexMatch) obj;
            if (!Objects.equals(this.regex_, other.regex_)) {
                return false;
            }
            if (!Objects.equals(this.successor_, other.successor_)) {
                return false;
            }
            return true;
        }
    }

    public PathMatcher(List<IdentifierMatch> matchers) {
        List<IdentifierMatch> reversed_matchers = new ArrayList<IdentifierMatch>(matchers);  // Create a copy, since we don't want to mangle the input argument.
        Collections.reverse(reversed_matchers);

        IdentifierMatch match_head = reversed_matchers.remove(0);
        while (!reversed_matchers.isEmpty()) {
            match_head = reversed_matchers.remove(0).rebindWithSuccessor(match_head);
        }

        matcher_ = match_head;
        match_cache_ = CacheBuilder.newBuilder()
                .softValues()
                .build(CacheLoader.from(grp_path -> new CachedOutcome(match_(grp_path))));
    }

    public PathMatcher(IdentifierMatch... matchers) {
        this(Arrays.asList(matchers));
    }

    public boolean match_(List<String> grp_path) {
        boolean result = matcher_.match(grp_path, new IdentifierMatch.SkipBacktrack());
        LOG.log(Level.FINER, "Attempting to match {0} with {1} -> {2}", new Object[]{grp_path, matcher_, result});
        return result;
    }

    public boolean match(List<String> grp_path) {
        return match_cache_.getUnchecked(grp_path).isMatched();
    }

    @Deprecated
    public boolean match(SimpleGroupPath grp) {
        return match(grp.getPath());
    }

    @Deprecated
    public boolean match(MetricName metric) {
        return match(metric.getPath());
    }

    @Override
    public StringBuilder configString() {
        return matcher_.populateExpression(new StringBuilder());
    }

    @Override
    public String toString() {
        return "PathMatcher:" + configString().toString();
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 19 * hash + Objects.hashCode(this.matcher_);
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
        final PathMatcher other = (PathMatcher) obj;
        if (!Objects.equals(this.matcher_, other.matcher_)) {
            return false;
        }
        return true;
    }
}
