package com.groupon.lex.metrics.collector.httpget.grammar;

import com.groupon.lex.metrics.lib.Any2;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.BailErrorStrategy;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.UnbufferedTokenStream;

/**
 *
 * @author ariane
 */
public class CollectdTags {
    private CollectdTags() {}

    public static Map<String, Any2<String, Number>> parse(String pattern) {
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

        final CollectdTagsLexer lexer = new CollectdTagsLexer(new ANTLRInputStream(pattern));
        lexer.removeErrorListeners();
        lexer.addErrorListener(error_listener);
        final CollectdTagsParser parser = new CollectdTagsParser(new UnbufferedTokenStream(lexer));
        parser.removeErrorListeners();
        parser.addErrorListener(error_listener);

        parser.setErrorHandler(new BailErrorStrategy());
        final CollectdTagsParser.ExprContext result = parser.expr();
        if (result.exception != null)
            throw new IllegalArgumentException("errors during parsing: " + pattern, result.exception);
        else if (!error_listener.errors.isEmpty())
            throw new IllegalArgumentException("syntax errors during parsing:\n" + String.join("\n", error_listener.errors.stream().map(s -> "  " + s).collect(Collectors.toList())));
        return result.result;
    }
}
