package com.groupon.lex.metrics.history.xdr.support;

import com.groupon.lex.metrics.GroupName;
import com.groupon.lex.metrics.MetricName;
import com.groupon.lex.metrics.MetricValue;
import com.groupon.lex.metrics.timeseries.TimeSeriesValue;
import gnu.trove.map.hash.THashMap;
import static java.util.Collections.unmodifiableMap;
import java.util.Map;
import java.util.Objects;
import static java.util.Objects.requireNonNull;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Value;
import org.joda.time.DateTime;

@Value
public final class ImmutableTimeSeriesValue implements TimeSeriesValue {
    private final DateTime timestamp;
    private final GroupName group;
    private final Map<MetricName, MetricValue> metrics;

    private ImmutableTimeSeriesValue(DateTime timestamp, GroupName group, Map<MetricName, MetricValue> metrics) {
        this.timestamp = requireNonNull(timestamp);
        this.group = requireNonNull(group);
        this.metrics = requireNonNull(metrics);
    }

    public <T> ImmutableTimeSeriesValue(DateTime ts, GroupName group, Stream<T> metrics, Function<? super T, MetricName> name_fn, Function<? super T, MetricValue> value_fn) {
        this(ts, group, unmodifiableMap(metrics.collect(Collectors.toMap(name_fn, value_fn, throwing_merger_(), hashmap_constructor_()))));
    }

    @Override
    public TimeSeriesValue clone() {
        return this;  // Immutable class doesn't need copy-clone.
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 59 * hash + Objects.hashCode(getGroup());
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof TimeSeriesValue)) {
            return false;
        }
        final TimeSeriesValue other = (TimeSeriesValue) obj;
        if (!Objects.equals(this.getTimestamp(), other.getTimestamp())) {
            return false;
        }
        if (!Objects.equals(this.getGroup(), other.getGroup())) {
            return false;
        }
        if (!Objects.equals(this.getMetrics(), other.getMetrics())) {
            return false;
        }
        return true;
    }

    private static <T> BinaryOperator<T> throwing_merger_() {
        return (x, y) -> { throw new IllegalStateException("duplicate key " + x); };
    }

    /** HashMap constructor, so we can create hashmaps with an altered load factor. */
    private static <K, V> Supplier<Map<K, V>> hashmap_constructor_() {
        return () -> new THashMap<K, V>();
    }
}
