package com.groupon.lex.metrics.history.xdr;

import com.groupon.lex.metrics.GroupName;
import com.groupon.lex.metrics.MetricName;
import com.groupon.lex.metrics.MetricValue;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import org.joda.time.DateTime;

public interface ColumnMajorTSData {
    /**
     * Retrieve all timestamps covered in this.
     *
     * @return The timestamps covered in this. Timestamps may be unordered.
     */
    public Collection<DateTime> getTimestamps();

    /**
     * Retrieve all group names covered in this.
     *
     * @return The group names present in this TSData.
     */
    public Set<GroupName> getGroupNames();

    /**
     * Retrieve all timestamps at which this group exists.
     *
     * @return The timestamps at which the group exists. Timestamps may be
     * unordered.
     */
    public Collection<DateTime> getGroupTimestamps(GroupName group);

    /**
     * Retrieve all metric names in the specific group.
     *
     * @param group The group for which to look up metric names.
     * @return All metric names for the given group (irrespective of at which
     * timestamp).
     */
    public Set<MetricName> getMetricNames(GroupName group);

    /**
     * Retrieve all metric values, mapped by timestamp.
     *
     * @param group The group in which to look for values.
     * @param metric The metric in the group, for which to retrieve metric
     * values.
     * @return A map of timestamped metric values.
     */
    public Map<DateTime, MetricValue> getMetricValues(GroupName group, MetricName metric);
}
