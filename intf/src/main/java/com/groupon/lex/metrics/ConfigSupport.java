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

import com.groupon.lex.metrics.builders.collector.AcceptAsPath;
import com.groupon.lex.metrics.builders.collector.AcceptOptAsPath;
import com.groupon.lex.metrics.builders.collector.AcceptTagSet;
import com.groupon.lex.metrics.builders.collector.CollectorBuilder;
import com.groupon.lex.metrics.builders.collector.MainNone;
import com.groupon.lex.metrics.builders.collector.MainString;
import com.groupon.lex.metrics.builders.collector.MainStringList;
import com.groupon.lex.metrics.resolver.NameBoundResolver;
import static java.util.Collections.unmodifiableSet;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Predicate;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import static java.util.regex.Pattern.CANON_EQ;
import java.util.stream.Collectors;
import lombok.NonNull;
import org.joda.time.Duration;

/**
 * Configuration support.
 * Mainly implements the logic that converts to representations in a config file.
 * @author ariane
 */
public class ConfigSupport {
    private ConfigSupport() {} // Prevent instantiation.
    private final static Predicate<String> IDENTIFIER_MATCHER = Pattern.compile("^[_a-zA-Z][_a-zA-Z0-9]*$", CANON_EQ)
            .asPredicate();
    public static final Set<String> KEYWORDS = unmodifiableSet(new HashSet<String>() {{
        add("import");
        add("constants");
        add("collectors");
        add("all");
        add("from");
        add("collect");
        add("constant");
        add("alert");
        add("if");
        add("for");
        add("match");
        add("message");
        add("as");
        add("where");
        add("alias");
        add("tag");
        add("true");
        add("false");
        add("define");
        add("by");
        add("without");
        add("keep_common");
    }});

    private static class EscapeStringResult {
        public final boolean needQuotes;
        public final StringBuilder buffer;

        public EscapeStringResult(boolean needQuotes, StringBuilder buffer) {
            this.needQuotes = needQuotes;
            this.buffer = buffer;
        }
    }

    /**
     * Escape a string configuration element.
     * @param s a string.
     * @return the string, between quotes, escaped such that the configuration parser will accept it as a string.
     */
    private static EscapeStringResult escapeString(String s, char quote) {
        final String[] octal_strings = {
            "\\0",   "\\001", "\\002", "\\003", "\\004", "\\005", "\\006", "\\a",
            "\\b",   "\\t",   "\\n",   "\\v",   "\\f",   "\\r",   "\\016", "\\017",
            "\\020", "\\021", "\\022", "\\023", "\\024", "\\025", "\\026", "\\027",
            "\\030", "\\031", "\\032", "\\033", "\\034", "\\035", "\\036", "\\037",
        };

        StringBuilder result = new StringBuilder(s);
        boolean needQuotes = s.isEmpty();
        for (int i = 0, next_i; i < result.length(); i = next_i) {
            final int code_point = result.codePointAt(i);
            next_i = result.offsetByCodePoints(i, 1);

            if (code_point == 0) {
                result.replace(i, next_i, "\\0");
                next_i = i + 2;
                needQuotes = true;
            } else if (code_point == (int)'\\') {
                result.replace(i, next_i, "\\\\");
                next_i = i + 2;
                needQuotes = true;
            } else if (code_point == (int)quote) {
                result.replace(i, next_i, "\\" + quote);
                next_i = i + 2;
                needQuotes = true;
            } else if (code_point < 32) {
                String octal = octal_strings[code_point];
                result.replace(i, next_i, octal);
                next_i = i + octal.length();
                needQuotes = true;
            } else if (code_point >= 65536) {
                String repl = String.format("\\U%08x", code_point);
                result.replace(i, next_i, repl);
                next_i = i + repl.length();
                needQuotes = true;
            } else if (code_point >= 128) {
                String repl = String.format("\\u%04x", code_point);
                result.replace(i, next_i, repl);
                next_i = i + repl.length();
                needQuotes = true;
            }
        }
        return new EscapeStringResult(needQuotes, result);
    }

    /**
     * Create a quoted string configuration element.
     * @param s a string.
     * @return the string, between quotes, escaped such that the configuration parser will accept it as a string.
     */
    public static StringBuilder quotedString(String s) {
        final EscapeStringResult escapeString = escapeString(s, '\"');
        final StringBuilder result = escapeString.buffer;

        result.insert(0, '\"');
        result.append('\"');
        return result;
    }

    /**
     * Create an identifier configuration element.
     * @param s an identifier
     * @return the identifier as a string, which is escaped and quoted if needed.
     */
    public static StringBuilder maybeQuoteIdentifier(String s) {
        final EscapeStringResult escapeString = escapeString(s, '\'');
        final StringBuilder result = escapeString.buffer;

        final boolean need_quotes = s.isEmpty() ||
                escapeString.needQuotes ||
                KEYWORDS.contains(s) ||
                !IDENTIFIER_MATCHER.test(s);

        if (need_quotes) {
            result.insert(0, '\'');
            result.append('\'');
        }
        return result;
    }

    /**
     * Create a regex configuration element.
     * @param s a regex
     * @return the regex as a configuration regex, which is escaped where needed.
     */
    public static StringBuilder regex(String s) {
        return escapeString(s, '/').buffer
                .insert(0, "//")
                .append("//");
    }

    /**
     * Convert duration to an representation accepted by Configuration parser.
     * @param duration A duration.
     * @return A StringBuilder with the string representation of the duration.
     */
    public static StringBuilder durationConfigString(Duration duration) {
        Duration remainder = duration;

        long days = remainder.getStandardDays();
        remainder = remainder.minus(Duration.standardDays(days));

        long hours = remainder.getStandardHours();
        remainder = remainder.minus(Duration.standardHours(hours));

        long minutes = remainder.getStandardMinutes();
        remainder = remainder.minus(Duration.standardMinutes(minutes));

        long seconds = remainder.getStandardSeconds();
        remainder = remainder.minus(Duration.standardSeconds(seconds));

        if (!remainder.isEqual(Duration.ZERO))
            Logger.getLogger(ConfigSupport.class.getName()).log(Level.WARNING, "Duration is more precise than configuration will handle: {0}, dropping remainder: {1}", new Object[]{duration, remainder});

        StringBuilder result = new StringBuilder();
        if (days != 0) {
            if (result.length() != 0) result.append(' ');
            result.append(days).append('d');
        }
        if (hours != 0) {
            if (result.length() != 0) result.append(' ');
            result.append(hours).append('h');
        }
        if (minutes != 0) {
            if (result.length() != 0) result.append(' ');
            result.append(minutes).append('m');
        }
        if (result.length() == 0 || seconds != 0) {
            if (result.length() != 0) result.append(' ');
            result.append(seconds).append('s');
        }

        return result;
    }

    /**
     * Create a config string for a builder.
     *
     * The function may fail if the builder has not been fully initialized.
     * @param name The name of the collector.
     * @param builder The builder implementation used to create collectors.
     * @return A string with the collect statement.
     *     The statement will be closed (either with a tag set or a semi-colon).
     *     The string will not have a trailing new-line.
     */
    public static StringBuilder collectorConfigString(@NonNull String name, @NonNull CollectorBuilder builder) {
        StringBuilder buf = new StringBuilder()
                .append("collect ")
                .append(name);

        /*
         * Handle main argument.
         */
        if (builder instanceof MainNone) {
            /* SKIP */
        }
        if (builder instanceof MainString) {
            buf
                    .append(' ')
                    .append(quotedString(((MainString)builder).getMain()).toString());
        }
        if (builder instanceof MainStringList) {
            buf
                    .append(' ')
                    .append(((MainStringList)builder).getMain().stream().map(ConfigSupport::quotedString).collect(Collectors.joining(", ")));
        }

        /*
         * Hande asPath argument.
         */
        if (builder instanceof AcceptAsPath) {
            buf
                    .append(" as ")
                    .append(((AcceptAsPath)builder).getAsPath().configString());
        }
        if (builder instanceof AcceptOptAsPath) {
            ((AcceptOptAsPath)builder).getAsPath()
                    .ifPresent(path -> {
                        buf
                                .append(" as ")
                                .append(path.configString());
                    });
        }

        /*
         * Handle tag set.
         * If the collector has no tag set, the collector is closed using a semi-colon.
         */
        if (builder instanceof AcceptTagSet) {
            final NameBoundResolver tagSet = ((AcceptTagSet)builder).getTagSet();
            if (tagSet.isEmpty()) {
                buf.append(';');  // Empty tag set has no meaningful config string.
            } else {
                buf
                        .append(' ')
                        .append(tagSet.configString());
            }
        } else {
            buf.append(';');
        }

        return buf;
    }
}
