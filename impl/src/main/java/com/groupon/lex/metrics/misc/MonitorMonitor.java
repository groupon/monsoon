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
package com.groupon.lex.metrics.misc;

import com.groupon.lex.metrics.GroupName;
import com.groupon.lex.metrics.MetricName;
import com.groupon.lex.metrics.MetricRegistryInstance;
import com.groupon.lex.metrics.MetricValue;
import com.groupon.lex.metrics.MutableTimeSeriesCollectionPair;
import com.groupon.lex.metrics.timeseries.Alert;
import com.groupon.lex.metrics.timeseries.ExpressionLookBack;
import com.groupon.lex.metrics.timeseries.TimeSeriesTransformer;
import com.groupon.lex.metrics.timeseries.TimeSeriesValue;
import com.groupon.lex.metrics.timeseries.expression.Context;
import static java.util.Collections.EMPTY_MAP;
import java.util.HashMap;
import java.util.Map;
import static java.util.Objects.requireNonNull;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.joda.time.DateTime;
import org.joda.time.Duration;

/**
 * Provides an alert for wether the monitor is running.
 *
 * The alert never fires, but is intended to be hooked up to an external alert
 * manager, which can alert if the alert hasn't been marked as OK sufficiently
 * recently.
 *
 * @author ariane
 */
public class MonitorMonitor implements TimeSeriesTransformer {
    public static final String ROOT_GROUP = "monsoon";
    public static final GroupName MONITOR_GROUP = GroupName.valueOf(ROOT_GROUP);
    public static final GroupName MONITOR_DOWN_ALERT = GroupName.valueOf(ROOT_GROUP, "down");
    public static final GroupName HAS_CONFIG_ALERT = GroupName.valueOf(ROOT_GROUP, "configuration_missing");
    public static final GroupName MONITOR_FAIL_ALERT = MONITOR_GROUP;
    public static final MetricName FAILED_COLLECTIONS_METRIC = MetricName.valueOf("failed_collections");
    public static final MetricName GROUP_COUNT_METRIC = MetricName.valueOf("groups");
    public static final MetricName METRIC_COUNT_METRIC = MetricName.valueOf("metric");
    public static final MetricName CONFIG_PRESENT_METRIC = MetricName.valueOf("has_config_file");
    public static final MetricName SCRAPE_DURATION = MetricName.valueOf("timing", "collectors");
    public static final MetricName RULE_EVAL_DURATION = MetricName.valueOf("timing", "rule_eval");
    public static final MetricName PROCESSOR_DURATION = MetricName.valueOf("timing", "processor");
    public static final MetricName UPTIME_DURATION = MetricName.valueOf("scrape", "uptime");
    public static final MetricName SCRAPE_COUNT = MetricName.valueOf("scrape", "count");
    public static final MetricName SCRAPE_INTERVAL = MetricName.valueOf("scrape", "interval");
    public static final MetricName SCRAPE_TS = MetricName.valueOf("scrape", "ts");
    public static final Duration MON_ALERT_DURATION = Duration.standardMinutes(5);
    private final MetricRegistryInstance registry_;
    private final AtomicReference<DateTime> first_scrape_ts_ = new AtomicReference<>();
    private Optional<DateTime> last_scrape_ = Optional.empty();
    private long scrape_count_ = 0;

    public MonitorMonitor(MetricRegistryInstance registry) {
        registry_ = requireNonNull(registry);
    }

    /**
     * Get metrics for the monitor.
     *
     * @return All metrics for the monitor.
     */
    private Map<MetricName, MetricValue> get_metrics_(DateTime now, Context ctx) {
        final Consumer<Alert> alert_manager = ctx.getAlertManager();
        final long failed_collections = registry_.getFailedCollections();
        final boolean has_config = registry_.hasConfig();
        final Optional<Duration> scrape_duration = registry_.getScrapeDuration();
        final Optional<Duration> rule_eval_duration = registry_.getRuleEvalDuration();
        final Optional<Duration> processor_duration = registry_.getProcessorDuration();
        first_scrape_ts_.compareAndSet(null, now);  // First time, register the timestamp.
        final Duration uptime = new Duration(first_scrape_ts_.get(), now);

        final Optional<DateTime> last_scrape = last_scrape_;
        last_scrape_ = Optional.of(now);

        final long metric_count = ctx.getTSData().getCurrentCollection().getTSValues().stream()
                .map(TimeSeriesValue::getMetrics)
                .collect(Collectors.summingLong(Map::size));

        alert_manager.accept(new Alert(now, MONITOR_FAIL_ALERT, () -> "builtin rule", Optional.of(failed_collections != 0), MON_ALERT_DURATION, "builtin rule: some collectors failed", EMPTY_MAP));
        alert_manager.accept(new Alert(now, HAS_CONFIG_ALERT, () -> "builtin rule", Optional.of(!has_config), Duration.ZERO, "builtin rule: monitor has no configuration file", EMPTY_MAP));

        Map<MetricName, MetricValue> result = new HashMap<>();
        result.put(FAILED_COLLECTIONS_METRIC, MetricValue.fromIntValue(failed_collections));
        result.put(GROUP_COUNT_METRIC, MetricValue.fromIntValue(ctx.getTSData().getCurrentCollection().getGroups(x -> true).size()));
        result.put(METRIC_COUNT_METRIC, MetricValue.fromIntValue(metric_count));
        result.put(CONFIG_PRESENT_METRIC, MetricValue.fromBoolean(has_config));
        result.put(SCRAPE_DURATION, opt_duration_to_metricvalue_(scrape_duration));
        result.put(RULE_EVAL_DURATION, opt_duration_to_metricvalue_(rule_eval_duration));
        result.put(PROCESSOR_DURATION, opt_duration_to_metricvalue_(processor_duration));
        result.put(UPTIME_DURATION, duration_to_metricvalue_(uptime));
        result.put(SCRAPE_COUNT, MetricValue.fromIntValue(++scrape_count_));
        result.put(SCRAPE_INTERVAL, opt_duration_to_metricvalue_(last_scrape.map(prev -> new Duration(prev, now))));
        result.put(SCRAPE_TS, MetricValue.fromIntValue(now.getMillis()));
        return result;
    }

    /**
     * Emit an alert monitor.down, which is in the OK state.
     *
     * @param ctx Rule evaluation context.
     */
    @Override
    public void transform(Context<MutableTimeSeriesCollectionPair> ctx) {
        DateTime now = ctx.getTSData().getCurrentCollection().getTimestamp();

        ctx.getTSData().getCurrentCollection().addMetrics(MONITOR_GROUP, get_metrics_(now, ctx));

        ctx.getAlertManager().accept(new Alert(now, MONITOR_DOWN_ALERT, () -> "builtin rule", Optional.of(false), Duration.ZERO, "builtin rule: monitor is not running for some time", EMPTY_MAP));
    }

    /**
     * Convert an optional duration to a metric value.
     *
     * The returned metric value is expressed as the duration in milliseconds.
     * If the Optional is empty, an empty metric value is emitted.
     *
     * @param duration The duration to express.
     * @return A metric value representing the duration.
     */
    private static MetricValue opt_duration_to_metricvalue_(Optional<Duration> duration) {
        return duration
                .map(MonitorMonitor::duration_to_metricvalue_)
                .orElse(MetricValue.EMPTY);
    }

    /**
     * Convert a duration to a metric value.
     *
     * The returned metric value is expressed as the duration in milliseconds.
     *
     * @param duration The duration to express.
     * @return A metric value representing the duration.
     */
    private static MetricValue duration_to_metricvalue_(Duration duration) {
        return MetricValue.fromIntValue(duration.getMillis());
    }

    @Override
    public ExpressionLookBack getLookBack() {
        return ExpressionLookBack.EMPTY;
    }
}
