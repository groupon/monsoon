package com.groupon.lex.metrics.history.xdr.support;

import com.groupon.lex.metrics.GroupName;
import com.groupon.lex.metrics.MetricName;
import com.groupon.lex.metrics.MetricValue;
import com.groupon.lex.metrics.timeseries.AbstractTimeSeriesValue;
import com.groupon.lex.metrics.timeseries.TimeSeriesValue;
import gnu.trove.map.hash.THashMap;
import static java.util.Collections.unmodifiableMap;
import java.util.Map;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import org.joda.time.DateTime;

@Getter
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public final class ImmutableTimeSeriesValue extends AbstractTimeSeriesValue implements TimeSeriesValue {
    @NonNull
    private final DateTime timestamp;
    @NonNull
    private final GroupName group;
    @NonNull
    private final Map<MetricName, MetricValue> metrics;

    public <T> ImmutableTimeSeriesValue(DateTime ts, GroupName group, Stream<T> metrics, Function<? super T, MetricName> name_fn, Function<? super T, MetricValue> value_fn) {
        this(ts, group, unmodifiableMap(metrics.collect(Collectors.toMap(name_fn, value_fn, throwing_merger_(), hashmap_constructor_()))));
    }

    @Override
    public TimeSeriesValue clone() {
        return this;  // Immutable class doesn't need copy-clone.
    }

    private static <T> BinaryOperator<T> throwing_merger_() {
        return (x, y) -> { throw new IllegalStateException("duplicate key " + x); };
    }

    /** HashMap constructor, so we can create hashmaps with an altered load factor. */
    private static <K, V> Supplier<Map<K, V>> hashmap_constructor_() {
        return () -> new THashMap<K, V>();
    }
}
