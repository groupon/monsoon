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

import static com.groupon.lex.metrics.ConfigSupport.durationConfigString;
import static com.groupon.lex.metrics.ConfigSupport.maybeQuoteIdentifier;
import static com.groupon.lex.metrics.ConfigSupport.quotedString;
import com.groupon.lex.metrics.config.impl.AlertTransformerImpl;
import com.groupon.lex.metrics.lib.Any2;
import com.groupon.lex.metrics.timeseries.TimeSeriesMetricExpression;
import com.groupon.lex.metrics.transformers.NameResolver;
import static java.util.Collections.EMPTY_MAP;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import static java.util.Objects.requireNonNull;
import java.util.Optional;
import java.util.stream.Collectors;
import org.joda.time.Duration;

/**
 *
 * @author ariane
 */
public class AlertStatement implements RuleStatement {
    private final TimeSeriesMetricExpression predicate_;
    private final NameResolver name_;
    private final Duration fire_duration_;
    private final String message_;
    private final Map<String, Any2<TimeSeriesMetricExpression, List<TimeSeriesMetricExpression>>> attributes_;

    @Deprecated  // XXX currently used by grammar.
    public AlertStatement(NameResolver name,
                          TimeSeriesMetricExpression predicate,
                          Optional<Duration> fire_duration,
                          Optional<String> message) {
        this(name, predicate, fire_duration, message, EMPTY_MAP);
    }

    public AlertStatement(NameResolver name,
                          TimeSeriesMetricExpression predicate,
                          Optional<Duration> fire_duration,
                          Optional<String> message,
                          Map<String, Any2<TimeSeriesMetricExpression, List<TimeSeriesMetricExpression>>> attributes) {
        name_ = requireNonNull(name);
        predicate_ = requireNonNull(predicate);
        fire_duration_ = requireNonNull(fire_duration).orElse(Duration.ZERO);
        message_ = requireNonNull(message).orElseGet(() -> predicate_.configString().toString());
        attributes_ = requireNonNull(attributes);
    }

    public TimeSeriesMetricExpression getPredicate() {
        return predicate_;
    }

    public NameResolver getName() {
        return name_;
    }

    public Duration getFireDuration() {
        return fire_duration_;
    }

    public String getMessage() {
        return message_;
    }

    public Map<String, Any2<TimeSeriesMetricExpression, List<TimeSeriesMetricExpression>>> getAttributes() {
        return attributes_;
    }

    @Override
    public AlertTransformerImpl get() {
        return new AlertTransformerImpl(this);
    }

    @Override
    public StringBuilder configString() {
        final StringBuilder result = new StringBuilder()
                .append("alert ")
                .append(getName().configString())
                .append(" if ")
                .append(getPredicate().configString())
                .append(" for ")
                .append(durationConfigString(fire_duration_))
                .append(" message ")
                .append(quotedString(getMessage()));
        if (attributes_.isEmpty()) {
            return result.append(";\n");
        }

        result.append(" attributes {\n");
        attributes_.entrySet().stream()
                .sorted(Comparator.comparing(Map.Entry::getKey))
                .forEachOrdered(attr -> {
                    result
                            .append("    ")
                            .append(maybeQuoteIdentifier(attr.getKey()))
                            .append(" = ")
                            .append(attr.getValue().<CharSequence>mapCombine(
                                    value -> {
                                        return value.configString();
                                    },
                                    values -> {
                                        return values.stream()
                                        .map(TimeSeriesMetricExpression::configString)
                                        .collect(Collectors.joining(", ", "[ ", " ]"));
                                    }))
                            .append(",\n");
                });
        // Last two chars are ",\n".
        return result.replace(result.length() - 2, result.length(), "\n}\n");
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 67 * hash + Objects.hashCode(this.predicate_);
        hash = 67 * hash + Objects.hashCode(this.name_);
        hash = 67 * hash + Objects.hashCode(this.fire_duration_);
        hash = 67 * hash + Objects.hashCode(this.message_);
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
        final AlertStatement other = (AlertStatement) obj;
        if (!Objects.equals(this.predicate_, other.predicate_)) {
            return false;
        }
        if (!Objects.equals(this.name_, other.name_)) {
            return false;
        }
        if (!Objects.equals(this.fire_duration_, other.fire_duration_)) {
            return false;
        }
        if (!Objects.equals(this.message_, other.message_)) {
            return false;
        }
        return true;
    }
}
