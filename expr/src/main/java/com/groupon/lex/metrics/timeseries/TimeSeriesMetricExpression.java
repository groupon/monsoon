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
package com.groupon.lex.metrics.timeseries;

import com.groupon.lex.metrics.MetricValue;
import com.groupon.lex.metrics.timeseries.expression.Context;
import static com.groupon.lex.metrics.timeseries.expression.Priorities.BRACKETS;
import com.groupon.lex.metrics.timeseries.parser.Expression;
import com.groupon.lex.metrics.timeseries.parser.ExpressionLexer;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import static java.util.Collections.EMPTY_LIST;
import static java.util.Collections.unmodifiableList;
import java.util.List;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import lombok.Getter;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.BufferedTokenStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.Recognizer;

/**
 *
 * @author ariane
 */
public interface TimeSeriesMetricExpression extends Function<Context<?>, TimeSeriesMetricDeltaSet>, PrintableExpression {
    public static final TimeSeriesMetricExpression TRUE = new TimeSeriesMetricExpression() {
        private final TimeSeriesMetricDeltaSet VALUE = new TimeSeriesMetricDeltaSet(MetricValue.TRUE);

        @Override
        public TimeSeriesMetricDeltaSet apply(Context<?> ctx) {
            return VALUE;
        }

        @Override
        public int getPriority() {
            return BRACKETS;
        }

        @Override
        public StringBuilder configString() {
            return new StringBuilder("true");
        }

        @Override
        public Collection<TimeSeriesMetricExpression> getChildren() {
            return Collections.EMPTY_LIST;
        }
    };

    public static final TimeSeriesMetricExpression FALSE = new TimeSeriesMetricExpression() {
        private final TimeSeriesMetricDeltaSet VALUE = new TimeSeriesMetricDeltaSet(MetricValue.FALSE);

        @Override
        public TimeSeriesMetricDeltaSet apply(Context<?> ctx) {
            return VALUE;
        }

        @Override
        public int getPriority() {
            return BRACKETS;
        }

        @Override
        public StringBuilder configString() {
            return new StringBuilder("false");
        }

        @Override
        public Collection<TimeSeriesMetricExpression> getChildren() {
            return Collections.EMPTY_LIST;
        }
    };

    @Override
    public TimeSeriesMetricDeltaSet apply(Context<?> ctx);

    public Collection<TimeSeriesMetricExpression> getChildren();

    /**
     * @return A filter that describes how far back the expression may reach in
     * the collection history.
     */
    public default ExpressionLookBack getLookBack() {
        return ExpressionLookBack.EMPTY.andThen(getChildren().stream().map(TimeSeriesMetricExpression::getLookBack));
    }

    /**
     * @return A filter that describes all groups and metrics that are required
     * to properly evaluate the expression.
     */
    public default TimeSeriesMetricFilter getNameFilter() {
        return getChildren().stream()
                .map(child -> child.getNameFilter())
                .reduce(new TimeSeriesMetricFilter(), TimeSeriesMetricFilter::with);
    }

    /**
     * Read expression from string.
     *
     * @param str A string containing an expression.
     * @return A TimeSeriesMetricExpression corresponding to the parsed input
     * from the reader.
     * @throws
     * com.groupon.lex.metrics.timeseries.TimeSeriesMetricExpression.ParseException
     * on invalid expression.
     */
    public static TimeSeriesMetricExpression valueOf(String str) throws ParseException {
        try {
            return valueOf(new StringReader(str));
        } catch (IOException ex) {
            throw new IllegalStateException("StringReader IO error?", ex);
        }
    }

    /**
     * Read expression from reader.
     *
     * @param reader A reader supplying the input of an expression.
     * @return A TimeSeriesMetricExpression corresponding to the parsed input
     * from the reader.
     * @throws IOException on IO errors from the reader.
     * @throws
     * com.groupon.lex.metrics.timeseries.TimeSeriesMetricExpression.ParseException
     * on invalid expression.
     */
    public static TimeSeriesMetricExpression valueOf(Reader reader) throws IOException, ParseException {
        final Logger LOG = Logger.getLogger(TimeSeriesMetricExpression.class.getName());

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

        final ExpressionLexer lexer = new ExpressionLexer(CharStreams.fromReader(reader));
        lexer.removeErrorListeners();
        lexer.addErrorListener(error_listener);

        final Expression parser = new Expression(new BufferedTokenStream(lexer));
        parser.removeErrorListeners();
        parser.addErrorListener(error_listener);

        final Expression.ExprContext expr;
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
}
