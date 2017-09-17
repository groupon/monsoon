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
import com.groupon.lex.metrics.timeseries.parser.PathMatcherGrammar;
import com.groupon.lex.metrics.timeseries.parser.PathMatcherLexer;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import static java.util.Collections.EMPTY_LIST;
import static java.util.Collections.unmodifiableList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.BufferedTokenStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.Recognizer;

/**
 *
 * @author ariane
 */
public class PathMatcher {
    private static final Logger LOG = Logger.getLogger(PathMatcher.class.getName());
    private final IdentifierMatch matcher_;
    private final LoadingCache<List<String>, CachedOutcome> match_cache_;

    /**
     * Wrapped boolean inside a container class, so the SoftReferences work
     * properly.
     */
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

        public default IdentifierMatch rebindWithSuccessor(IdentifierMatch successor) {
            return rebindWithSuccessor(Optional.of(successor));
        }

        public StringBuilder populateExpression(StringBuilder buf);

        public default boolean isLiteral() {
            return false;
        }

        public default Optional<Stream<String>> asLiteralComponents() {
            return Optional.empty();
        }

        public Optional<IdentifierMatch> getSuccessor();

        public void doVisit(Visitor v);
    }

    @RequiredArgsConstructor
    @EqualsAndHashCode
    @Getter
    public static class LiteralNameMatch implements IdentifierMatch {
        @NonNull
        private final String literal;
        @NonNull
        private final Optional<IdentifierMatch> successor;

        public LiteralNameMatch(String literal) {
            this(literal, Optional.empty());
        }

        @Override
        public boolean match(List<String> path, SkipBacktrack skipper) {
            if (path.isEmpty())
                return false;
            if (!literal.equals(path.get(0)))
                return false;
            return successor
                    .map((succ) -> succ.match(path.subList(1, path.size()), skipper))
                    .orElse(path.size() == 1);
        }

        @Override
        public LiteralNameMatch rebindWithSuccessor(Optional<IdentifierMatch> successor) {
            return new LiteralNameMatch(literal, successor);
        }

        @Override
        public StringBuilder populateExpression(StringBuilder buf) {
            buf.append(ConfigSupport.maybeQuoteIdentifier(literal));
            return successor
                    .map((succ) -> succ.populateExpression(buf.append('.')))
                    .orElse(buf);
        }

        @Override
        public boolean isLiteral() {
            return !successor.isPresent() || successor.get().isLiteral();
        }

        @Override
        public Optional<Stream<String>> asLiteralComponents() {
            if (!successor.isPresent()) return Optional.of(Stream.of(literal));

            return successor
                    .flatMap(IdentifierMatch::asLiteralComponents)
                    .map(tailStream -> Stream.concat(Stream.of(literal), tailStream));
        }

        @Override
        public String toString() {
            return populateExpression(new StringBuilder()).toString();
        }

        @Override
        public void doVisit(Visitor v) {
            v.accept(this);
        }
    }

    @RequiredArgsConstructor
    @EqualsAndHashCode
    @Getter
    public static class WildcardMatch implements IdentifierMatch {
        @NonNull
        private final Optional<IdentifierMatch> successor;

        public WildcardMatch() {
            this(Optional.empty());
        }

        @Override
        public boolean match(List<String> path, SkipBacktrack skipper) {
            if (path.isEmpty())
                return false;
            return successor
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
            return successor
                    .map((succ) -> succ.populateExpression(buf.append('.')))
                    .orElse(buf);
        }

        @Override
        public String toString() {
            return populateExpression(new StringBuilder()).toString();
        }

        @Override
        public void doVisit(Visitor v) {
            v.accept(this);
        }
    }

    @RequiredArgsConstructor
    @EqualsAndHashCode
    @Getter
    public static class DoubleWildcardMatch implements IdentifierMatch {
        @NonNull
        private final Optional<IdentifierMatch> successor;

        public DoubleWildcardMatch() {
            this(Optional.empty());
        }

        @Override
        public boolean match(List<String> path, SkipBacktrack skipper) {
            skipper.skip = true;

            SkipBacktrack my_skipper = new SkipBacktrack();
            if (!successor.isPresent())
                return true;
            for (int i = 0; i < path.size(); ++i) {
                if (successor.get().match(path.subList(i, path.size()), my_skipper))
                    return true;
                if (my_skipper.skip)
                    return false;  // Nested expression has done all the backtracking it needs.
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
            return successor
                    .map((succ) -> succ.populateExpression(buf.append('.')))
                    .orElse(buf);
        }

        @Override
        public String toString() {
            return populateExpression(new StringBuilder()).toString();
        }

        @Override
        public void doVisit(Visitor v) {
            v.accept(this);
        }
    }

    @EqualsAndHashCode(exclude = {"match"})
    public static class RegexMatch implements IdentifierMatch {
        @Getter
        private final String regex;
        private final Predicate<String> match;
        @Getter
        private final Optional<IdentifierMatch> successor;

        public RegexMatch(String regex) {
            this(regex, Optional.empty());
        }

        public RegexMatch(@NonNull String regex, @NonNull Optional<IdentifierMatch> successor) {
            this.regex = regex;
            this.match = Pattern.compile(regex).asPredicate();
            this.successor = successor;
        }

        @Override
        public boolean match(List<String> path, SkipBacktrack skipper) {
            if (path.isEmpty())
                return false;
            if (!match.test(path.get(0)))
                return false;
            return successor
                    .map((succ) -> succ.match(path.subList(1, path.size()), skipper))
                    .orElse(path.size() == 1);
        }

        @Override
        public RegexMatch rebindWithSuccessor(Optional<IdentifierMatch> successor) {
            return new RegexMatch(regex, successor);
        }

        @Override
        public StringBuilder populateExpression(StringBuilder buf) {
            buf.append(ConfigSupport.regex(regex));
            return successor
                    .map((succ) -> succ.populateExpression(buf.append('.')))
                    .orElse(buf);
        }

        @Override
        public String toString() {
            return populateExpression(new StringBuilder()).toString();
        }

        @Override
        public void doVisit(Visitor v) {
            v.accept(this);
        }
    }

    public PathMatcher(List<IdentifierMatch> matchers) {
        List<IdentifierMatch> reversed_matchers = new ArrayList<>(matchers);  // Create a copy, since we don't want to mangle the input argument.
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

    private boolean match_(List<String> grp_path) {
        boolean result = matcher_.match(grp_path, new IdentifierMatch.SkipBacktrack());
        LOG.log(Level.FINER, "Attempting to match {0} with {1} -> {2}", new Object[]{grp_path, matcher_, result});
        return result;
    }

    public boolean match(List<String> grp_path) {
        return match_cache_.getUnchecked(grp_path).isMatched();
    }

    public boolean isLiteral() {
        return matcher_.isLiteral();
    }

    public Optional<SimpleGroupPath> asLiteral() {
        return matcher_.asLiteralComponents()
                .map(stream -> stream.toArray(String[]::new))
                .map(SimpleGroupPath::valueOf);
    }

    public <VisitorType extends Visitor> Optional<VisitorType> visitNonLiteral(VisitorType v) {
        if (isLiteral()) return Optional.empty();
        for (IdentifierMatch matchHead = matcher_; matchHead != null; matchHead = matchHead.getSuccessor().orElse(null))
            matchHead.doVisit(v);
        return Optional.of(v);
    }

    public StringBuilder configString() {
        return matcher_.populateExpression(new StringBuilder());
    }

    @Override
    public String toString() {
        return "PathMatcher:" + configString().toString();
    }

    /**
     * Read path matcher from string.
     *
     * @param str A string containing a path expression.
     * @return A PathMatcher corresponding to the parsed input
     * from the string.
     * @throws
     * com.groupon.lex.metrics.PathMatcher.ParseException
     * on invalid path expression.
     */
    public static PathMatcher valueOf(String str) throws ParseException {
        try {
            return valueOf(new StringReader(str));
        } catch (IOException ex) {
            throw new IllegalStateException("StringReader IO error?", ex);
        }
    }

    /**
     * Read path matcher from reader.
     *
     * @param reader A reader supplying the input of a path expression.
     * @return A PathMatcher corresponding to the parsed input
     * from the reader.
     * @throws IOException on IO errors from the reader.
     * @throws
     * com.groupon.lex.metrics.PathMatcher.ParseException
     * on invalid path expression.
     */
    public static PathMatcher valueOf(Reader reader) throws IOException, ParseException {
        class DescriptiveErrorListener extends BaseErrorListener {
            public List<String> errors = new ArrayList<>();

            @Override
            public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol,
                                    int line, int charPositionInLine,
                                    String msg, org.antlr.v4.runtime.RecognitionException e) {
                LOG.log(Level.INFO, "Parse error: {0}:{1} -> {2}", new Object[]{line, charPositionInLine, msg});
                errors.add(String.format("%d:%d: %s", line, charPositionInLine, msg));
            }
        }

        final DescriptiveErrorListener error_listener = new DescriptiveErrorListener();

        final PathMatcherLexer lexer = new PathMatcherLexer(CharStreams.fromReader(reader));
        lexer.removeErrorListeners();
        lexer.addErrorListener(error_listener);

        final PathMatcherGrammar parser = new PathMatcherGrammar(new BufferedTokenStream(lexer));
        parser.removeErrorListeners();
        parser.addErrorListener(error_listener);

        final PathMatcherGrammar.ExprContext expr;
        try {
            expr = parser.expr();
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, "parser yielded exceptional return", ex);
            if (!error_listener.errors.isEmpty())
                throw new ParseException(error_listener.errors, ex);
            else
                throw ex;
        }

        if (!error_listener.errors.isEmpty()) {
            if (expr.exception != null)
                throw new ParseException(error_listener.errors, expr.exception);
            throw new ParseException(error_listener.errors);
        } else if (expr.exception != null) {
            throw new ParseException(expr.exception);
        }
        return expr.s;
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

    /**
     * Exception indicating an invalid expression. The parse errors are recorded
     * as messages and can be retrieved using getParseErrors().
     */
    public static class ParseException extends Exception {
        @Getter
        private final List<String> parseErrors;

        public ParseException(List<String> parseErrors) {
            super();
            this.parseErrors = unmodifiableList(parseErrors);
        }

        public ParseException(List<String> parseErrors, Throwable cause) {
            super(cause);
            this.parseErrors = unmodifiableList(parseErrors);
        }

        public ParseException(List<String> parseErrors, String message, Throwable cause) {
            super(message, cause);
            this.parseErrors = unmodifiableList(parseErrors);
        }

        public ParseException() {
            this(EMPTY_LIST);
        }

        public ParseException(Throwable cause) {
            this(EMPTY_LIST, cause);
        }

        public ParseException(String message, Throwable cause) {
            this(EMPTY_LIST, message, cause);
        }
    }

    public static interface Visitor {
        public void accept(LiteralNameMatch match);
        public void accept(RegexMatch match);
        public void accept(WildcardMatch match);
        public void accept(DoubleWildcardMatch match);
    }
}
