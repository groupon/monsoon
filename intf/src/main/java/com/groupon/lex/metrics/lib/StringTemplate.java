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
package com.groupon.lex.metrics.lib;

import static com.groupon.lex.metrics.ConfigSupport.quotedString;
import com.groupon.lex.metrics.grammar.StringSubstitutionLexer;
import com.groupon.lex.metrics.grammar.StringSubstitutionParser;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import static java.util.Objects.requireNonNull;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.antlr.v4.runtime.BailErrorStrategy;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.UnbufferedTokenStream;

/**
 *
 * @author ariane
 */
public class StringTemplate implements Function<Map<Any2<Integer, String>, String>, String> {
    public static interface Element {
        public void apply(StringBuilder out, Map<Any2<Integer, String>, String> args);

        public void keys(Consumer<Any2<Integer, String>> c);
    }

    public static class LiteralElement implements Element {
        private final String lit_;

        public LiteralElement(String lit) {
            lit_ = requireNonNull(lit);
        }

        @Override
        public void apply(StringBuilder out, Map<Any2<Integer, String>, String> args) {
            out.append(lit_);
        }

        @Override
        public String toString() {
            return lit_.replace("$", "$$");
        }

        @Override
        public void keys(Consumer<Any2<Integer, String>> c) {
        }
    }

    public static class SubstituteElement implements Element {
        private final int idx_;

        public SubstituteElement(int idx) {
            if (idx < 0) throw new IllegalArgumentException("negative index");
            idx_ = idx;
        }

        @Override
        public void apply(StringBuilder out, Map<Any2<Integer, String>, String> args) {
            out.append(args.get(Any2.<Integer, String>left(idx_)));
        }

        public int getIndex() {
            return idx_;
        }

        @Override
        public String toString() {
            return "${" + idx_ + '}';
        }

        @Override
        public void keys(Consumer<Any2<Integer, String>> c) {
            c.accept(Any2.left(getIndex()));
        }
    }

    public static class SubstituteNameElement implements Element {
        private final String name_;

        public SubstituteNameElement(String name) {
            name_ = requireNonNull(name);
            if (name.isEmpty()) throw new IllegalArgumentException();
        }

        @Override
        public void apply(StringBuilder out, Map<Any2<Integer, String>, String> args) {
            out.append(args.get(Any2.<Integer, String>right(name_)));
        }

        public String getName() {
            return name_;
        }

        @Override
        public String toString() {
            return "${" + name_ + '}';
        }

        @Override
        public void keys(Consumer<Any2<Integer, String>> c) {
            c.accept(Any2.right(getName()));
        }
    }

    private final List<Element> elements_;

    public StringTemplate(List<Element> elements) {
        elements_ = new ArrayList<>(elements);
    }

    public static StringTemplate fromString(String pattern) {
        class DescriptiveErrorListener extends BaseErrorListener {
            public List<String> errors = new ArrayList<>();

            @Override
            public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol,
                                    int line, int charPositionInLine,
                                    String msg, RecognitionException e) {
                errors.add(String.format("%d:%d: %s", line, charPositionInLine, msg));
            }
        }

        final DescriptiveErrorListener error_listener = new DescriptiveErrorListener();

        final StringSubstitutionLexer lexer = new StringSubstitutionLexer(CharStreams.fromString(pattern));
        lexer.removeErrorListeners();
        lexer.addErrorListener(error_listener);
        final StringSubstitutionParser parser = new StringSubstitutionParser(new UnbufferedTokenStream(lexer));
        parser.removeErrorListeners();
        parser.addErrorListener(error_listener);

        parser.setErrorHandler(new BailErrorStrategy());
        final StringSubstitutionParser.ExprContext result = parser.expr();
        if (result.exception != null)
            throw new IllegalArgumentException("errors during parsing: " + pattern, result.exception);
        else if (!error_listener.errors.isEmpty())
            throw new IllegalArgumentException("syntax errors during parsing:\n" + String.join("\n", error_listener.errors.stream().map(s -> "  " + s).collect(Collectors.toList())));
        return result.s;
    }

    @Override
    public String apply(Map<Any2<Integer, String>, String> args) {
        StringBuilder out = new StringBuilder();
        elements_.stream().forEach((elem) -> elem.apply(out, args));
        return out.toString();
    }

    public int indexSize() {
        return elements_.stream()
                .filter(e -> e instanceof SubstituteElement)
                .map(e -> (SubstituteElement) e)
                .map(SubstituteElement::getIndex)
                .max(Comparator.naturalOrder())
                .map(x -> x + 1)
                .orElse(0);
    }

    public StringBuilder configString() {
        return quotedString(toString());
    }

    @Override
    public String toString() {
        final StringBuilder result = new StringBuilder();
        elements_.forEach(result::append);
        return result.toString();
    }

    /**
     * Retrieve the variables used to render this string.
     *
     * @return The list of variables used to render this string.
     */
    public List<Any2<Integer, String>> getArguments() {
        final List<Any2<Integer, String>> result = new ArrayList<>();
        elements_.forEach(elem -> elem.keys(result::add));
        return result;
    }
}
