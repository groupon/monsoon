package com.groupon.lex.metrics.timeseries;

import com.groupon.lex.metrics.MetricMatcher;
import com.groupon.lex.metrics.PathMatcher;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/**
 * Represents a group of metrics that are required for an expression to function.
 */
@Getter
@EqualsAndHashCode
@ToString
public class TimeSeriesMetricFilter {
    private final Set<PathMatcher> groups;
    private final Set<MetricMatcher> metrics;

    public TimeSeriesMetricFilter() {
        this.groups = emptySet();
        this.metrics = emptySet();
    }

    private TimeSeriesMetricFilter(TimeSeriesMetricFilter orig, PathMatcher group) {
        this.metrics = orig.getMetrics();

        if (orig.getGroups().isEmpty()) {
            this.groups = singleton(group);
        } else {
            final HashSet<PathMatcher> newGroups = new HashSet<>(orig.getGroups());
            if (newGroups.add(group)) {
                this.groups = newGroups;
            } else {
                this.groups = orig.getGroups();
            }
        }
    }

    private TimeSeriesMetricFilter(TimeSeriesMetricFilter orig, MetricMatcher metric) {
        this.groups = orig.getGroups();

        if (orig.getMetrics().isEmpty()) {
            this.metrics = singleton(metric);
        } else {
            final HashSet<MetricMatcher> newMetrics = new HashSet<>(orig.getMetrics());
            if (newMetrics.add(metric)) {
                this.metrics = newMetrics;
            } else {
                this.metrics = orig.getMetrics();
            }
        }
    }

    public TimeSeriesMetricFilter withGroup(PathMatcher group) {
        return new TimeSeriesMetricFilter(this, group);
    }

    public TimeSeriesMetricFilter withMetric(MetricMatcher metric) {
        return new TimeSeriesMetricFilter(this, metric);
    }

    public TimeSeriesMetricFilter withGroups(Iterator<PathMatcher> groups) {
        TimeSeriesMetricFilter result = this;
        while (groups.hasNext())
            result = result.withGroup(groups.next());
        return result;
    }

    public TimeSeriesMetricFilter withGroups(Iterable<PathMatcher> groups) {
        return withGroups(groups.iterator());
    }

    public TimeSeriesMetricFilter withMetrics(Iterator<MetricMatcher> metrics) {
        TimeSeriesMetricFilter result = this;
        while (metrics.hasNext())
            result = result.withMetric(metrics.next());
        return result;
    }

    public TimeSeriesMetricFilter withMetrics(Iterable<MetricMatcher> metrics) {
        return withMetrics(metrics.iterator());
    }

    public TimeSeriesMetricFilter with(TimeSeriesMetricFilter other) {
        return withGroups(other.getGroups()).withMetrics(other.getMetrics());
    }

    public boolean isEmpty() {
        return getMetrics().isEmpty() && getGroups().isEmpty();
    }
}
