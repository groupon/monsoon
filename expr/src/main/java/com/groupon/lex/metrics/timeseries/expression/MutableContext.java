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

import com.groupon.lex.metrics.GroupName;
import com.groupon.lex.metrics.MetricName;
import com.groupon.lex.metrics.SimpleGroupPath;
import com.groupon.lex.metrics.timeseries.Alert;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollectionPair;
import com.groupon.lex.metrics.timeseries.TimeSeriesMetricDeltaSet;
import com.groupon.lex.metrics.timeseries.TimeSeriesValueSet;
import static java.util.Collections.unmodifiableMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 *
 * @author ariane
 */
public class MutableContext<TSDataPair extends TimeSeriesCollectionPair> extends SimpleContext<TSDataPair> {
    private final Map<String, ContextIdentifier> all_identifiers_ = new HashMap<>();

    private static final class Value<T, U> extends ContextIdentifier<T> {
        private final T value_;
        private final U alias_;

        public Value(Class<T> type, T value, U alias) {
            super(type);
            value_ = Objects.requireNonNull(value);
            alias_ = Objects.requireNonNull(alias);
            if (!type.isInstance(value))
                throw new IllegalArgumentException("Assigned value " + value + " must be instance of declared type " + type + ".");
        }

        @Override
        public Optional<T> get(Context ctx) {
            return Optional.of(value_);
        }

        @Override
        public <X> Optional<X> getAlias(Class<X> alias_type) {
            if (alias_type.isInstance(alias_)) return Optional.of(alias_type.cast(alias_));
            return Optional.empty();
        }

        @Override
        public String toString() {
            return "Value{" + "value_=" + value_ + ", alias_=" + alias_ + '}';
        }
    }

    private static final class OptValue<T, U> extends ContextIdentifier<T> {
        private final Optional<T> value_;
        private final U alias_;

        public OptValue(Class<T> type, Optional<T> value, U alias) {
            super(type);
            value_ = Objects.requireNonNull(value);
            alias_ = Objects.requireNonNull(alias);
            value.ifPresent((v) -> {
                        if (!type.isInstance(v))
                            throw new IllegalArgumentException("Assigned value " + v + " must be instance of declared type " + type + ".");
                    });
        }

        @Override
        public Optional<T> get(Context ctx) {
            return value_;
        }

        @Override
        public <X> Optional<X> getAlias(Class<X> alias_type) {
            if (alias_type.isInstance(alias_)) return Optional.of(alias_type.cast(alias_));
            return Optional.empty();
        }

        @Override
        public String toString() {
            return "OptValue{" + "value_=" + value_ + ", alias_=" + alias_ + '}';
        }
    }

    private static final class ProducedValue<T, U> extends ContextIdentifier<T> {
        private final Function<Context, Optional<? extends T>> function_;
        private final Supplier<U> alias_;

        public ProducedValue(Class<T> type, Function<Context, Optional<? extends T>> function, Supplier<U> alias) {
            super(type);
            function_ = Objects.requireNonNull(function);
            alias_ = Objects.requireNonNull(alias);
        }

        @Override
        public Optional<T> get(Context ctx) {
            Optional<? extends T> value = function_.apply(ctx);
            value.ifPresent((v) -> {
                        if (!getClazz().isInstance(v))
                            throw new IllegalArgumentException("Assigned value " + v + " must be instance of declared type " + getClazz() + ".");
                    });
            return value.map((x) -> x);
        }

        @Override
        public <X> Optional<X> getAlias(Class<X> alias_type) {
            final U alias = alias_.get();
            if (alias == null) return Optional.empty();
            if (alias_type.isInstance(alias)) return Optional.of(alias_type.cast(alias));
            return Optional.empty();
        }
    }

    public MutableContext(TSDataPair ts_data, Consumer<Alert> alert_manager) {
        super(ts_data, alert_manager);
    }

    public MutableContext(Context<? extends TSDataPair> copy) {
        super(copy.getTSData(), copy.getAlertManager());
        all_identifiers_.putAll(copy.getAllIdentifiers());
    }

    public MutableContext(TSDataPair ts_data, Context copy) {
        super(ts_data, copy.getAlertManager());
        all_identifiers_.putAll(copy.getAllIdentifiers());
    }

    public <T, U> void put(String identifier, Class<T> type, T value, U alias) {
        all_identifiers_.put(identifier, new Value<T, U>(type, value, alias));
    }

    public <T, U> void put(String identifier, Class<T> type, Optional<T> value, U alias) {
        all_identifiers_.put(identifier, new OptValue<T, U>(type, value, alias));
    }

    public <T, U> void putSupplied(String identifier, Class<T> type, Function<Context, Optional<? extends T>> function, Supplier<U> alias) {
        all_identifiers_.put(identifier, new ProducedValue<T, U>(type, function, alias));
    }

    private static Optional<TimeSeriesValueSet> group_alias_resolver_(Optional<SimpleGroupPath> group, Context ctx) {
        return group.map((grp_name) -> ctx.getTSData().getTSValue(grp_name));
    }

    public void putGroupAlias(String identifier, Optional<SimpleGroupPath> group) {
        putSupplied(identifier, TimeSeriesValueSet.class, (Context ctx) -> group_alias_resolver_(group, ctx), () -> group);
    }

    public void putGroupAlias(String identifier, SimpleGroupPath group) {
        putSupplied(identifier, TimeSeriesValueSet.class, (Context ctx) -> Optional.of(getTSData().getTSValue(group)), () -> group);
    }

    public void putGroupAliasByName(String identifier, Supplier<GroupName> group) {
        putSupplied(identifier, TimeSeriesValueSet.class, (Context ctx) -> getTSData().getTSDeltaByName(group.get()), () -> group.get().getPath());
    }

    public void putMetricAliasByName(String identifier, Supplier<GroupName> group, Supplier<MetricName> metric) {
        putSupplied(identifier, TimeSeriesMetricDeltaSet.class, (Context ctx) -> getTSData().getTSDeltaByName(group.get()).map(tsv -> tsv.findMetric(metric.get())), metric);
    }

    public void remove(String identifier) {
        all_identifiers_.remove(identifier);
    }

    @Override
    public Map<String, ContextIdentifier> getAllIdentifiers() {
        return unmodifiableMap(all_identifiers_);
    }
}
