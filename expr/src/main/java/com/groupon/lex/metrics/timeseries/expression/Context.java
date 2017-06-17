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
package com.groupon.lex.metrics.timeseries.expression;

import com.groupon.lex.metrics.timeseries.Alert;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollectionPair;
import com.groupon.lex.metrics.timeseries.TimeSeriesMetricDeltaSet;
import com.groupon.lex.metrics.timeseries.TimeSeriesValueSet;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author ariane
 */
public interface Context<TSDataPair extends TimeSeriesCollectionPair> {
    public Consumer<Alert> getAlertManager();
    public TSDataPair getTSData();

    public Map<String, ContextIdentifier> getAllIdentifiers();

    public default <T> Optional<? extends T> getFromIdentifier(Class<T> type, String identifier) {
        final Optional<ContextIdentifier> ctx_identifier = Optional.ofNullable(getAllIdentifiers().get(identifier));
        if (!ctx_identifier.isPresent())
            Logger.getLogger(getClass().getName()).log(Level.WARNING, "{0} not found", identifier);
        return ctx_identifier
                .filter((ContextIdentifier ci) -> type.isAssignableFrom(ci.getClazz()))
                .flatMap((ContextIdentifier ci) -> ci.get(this).map(type::cast));
    }

    public default Optional<TimeSeriesMetricDeltaSet> getMetricFromIdentifier(String identifier) {
        return getFromIdentifier(TimeSeriesMetricDeltaSet.class, identifier).map((x) -> x);
    }

    public default Optional<TimeSeriesValueSet> getGroupFromIdentifier(String identifier) {
        return getFromIdentifier(TimeSeriesValueSet.class, identifier).map((x) -> x);
    }

    public default <U> Optional<U> getAliasFromIdentifier(Class<U> type, String identifier) {
        return Optional.ofNullable(getAllIdentifiers().get(identifier))
                .flatMap((ContextIdentifier ci) -> ci.getAlias(type));
    }

    public default String getDebugString() {
        return new StringBuffer()
                .append("Context{")
                .append("ts_data=")
                .append(getTSData())
                .append(", identifiers=")
                .append(getAllIdentifiers())
                .append('}')
                .toString();
    }
}
