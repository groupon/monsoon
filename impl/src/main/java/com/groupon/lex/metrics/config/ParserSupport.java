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
package com.groupon.lex.metrics.config;

import com.groupon.lex.metrics.grammar.ConfigParser;
import com.groupon.lex.metrics.grammar.ConfigTokenizer;
import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import static java.util.Objects.requireNonNull;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.BufferedTokenStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.Recognizer;

/**
 * A class to make interactions with the parser easier.
 *
 * @author ariane
 */
public class ParserSupport {
    private static final Logger LOG = Logger.getLogger(ParserSupport.class.getName());
    private final Optional<File> dir_;
    private final Reader reader_;

    public ParserSupport(String str) throws IOException {
        this(new StringReader(str));
    }

    public ParserSupport(Reader reader) throws IOException {
        this(Optional.empty(), reader);
    }

    public ParserSupport(Optional<File> dir, Reader reader) throws IOException {
        dir_ = requireNonNull(dir);
        reader_ = requireNonNull(reader);
    }

    private static class DescriptiveErrorListener extends BaseErrorListener {
        public List<String> errors = new ArrayList<>();

        @Override
        public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol,
                                int line, int charPositionInLine,
                                String msg, org.antlr.v4.runtime.RecognitionException e) {
            LOG.log(Level.INFO, "Configuration error: {0}:{1} -> {2}", new Object[]{line, charPositionInLine, msg});
            errors.add(String.format("%d:%d: %s", line, charPositionInLine, msg));
        }
    }

    public Configuration configuration() throws IOException, ConfigurationException {
        final DescriptiveErrorListener error_listener = new DescriptiveErrorListener();
        final ConfigTokenizer lexer = new ConfigTokenizer(CharStreams.fromReader(reader_));
        lexer.removeErrorListeners();
        lexer.addErrorListener(error_listener);
        final ConfigParser parser = new ConfigParser(new BufferedTokenStream(lexer));
        parser.removeErrorListeners();
        parser.addErrorListener(error_listener);
        dir_.ifPresent(parser::setDir);

        final ConfigParser.ExprContext expr;
        try {
            expr = parser.expr();
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, "parser yielded exceptional return", ex);
            if (!error_listener.errors.isEmpty())
                throw new ConfigurationException(error_listener.errors, ex);
            else
                throw ex;
        }

        if (!error_listener.errors.isEmpty()) {
            if (expr.exception != null)
                throw new ConfigurationException(error_listener.errors, expr.exception);
            throw new ConfigurationException(error_listener.errors);
        } else if (expr.exception != null) {
            throw new ConfigurationException(expr.exception);
        }
        return expr.s;
    }
}
