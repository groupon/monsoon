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

import com.groupon.lex.metrics.api.endpoints.ExprEval;
import com.groupon.lex.metrics.api.endpoints.ExprEvalGraphServlet;
import com.groupon.lex.metrics.api.endpoints.ExprValidate;
import com.groupon.lex.metrics.api.endpoints.ListMetrics;
import com.groupon.lex.metrics.history.CollectHistory;
import com.groupon.lex.metrics.httpd.EndpointRegistration;
import com.groupon.lex.metrics.misc.MonitorMonitor;
import com.groupon.lex.metrics.timeseries.Alert;
import com.groupon.lex.metrics.timeseries.ExpressionLookBack;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollection;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollectionPairInstance;
import com.groupon.lex.metrics.timeseries.TimeSeriesTransformer;
import com.groupon.lex.metrics.timeseries.expression.Context;
import com.groupon.lex.metrics.timeseries.expression.MutableContext;
import java.util.ArrayList;
import java.util.Collection;
import static java.util.Collections.unmodifiableMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import static java.util.Objects.requireNonNull;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;

/**
 *
 * @author ariane
 */
public class PushMetricRegistryInstance extends MetricRegistryInstance {
    private static final Logger logger = Logger.getLogger(PushMetricRegistryInstance.class.getName());
    private final TimeSeriesCollectionPairInstance data_ = new TimeSeriesCollectionPairInstance();
    private Map<GroupName, Alert> alerts_ = new HashMap<>();
    private final Collection<TimeSeriesTransformer> decorators_ = new ArrayList<>();
    private Supplier<DateTime> now_;
    private Optional<Duration> rule_eval_duration_ = Optional.empty();
    private Optional<CollectHistory> history_ = Optional.empty();

    public PushMetricRegistryInstance(String package_name, boolean has_config, EndpointRegistration api) {
        this(package_name, () -> DateTime.now(DateTimeZone.UTC), has_config, api);
    }

    public PushMetricRegistryInstance(String package_name, Supplier<DateTime> now, boolean has_config, EndpointRegistration api) {
        super(package_name, has_config, api);
        now_ = requireNonNull(now);
        decorators_.add(new MonitorMonitor(this));
    }

    /** Set the history module that the push processor is to use. */
    public synchronized void setHistory(CollectHistory history) {
        history_ = Optional.of(history);
        getApi().addEndpoint("/monsoon/eval", new ExprEval(history_.get()));
        getApi().addEndpoint("/monsoon/eval/validate", new ExprValidate());
        getApi().addEndpoint("/monsoon/eval/gchart", new ExprEvalGraphServlet(history_.get()));
        data_.initWithHistoricalData(history, ExpressionLookBack.EMPTY.andThen(decorators_.stream().map(TimeSeriesTransformer::getLookBack)));
    }
    /** Clear the history module. */
    public synchronized void clearHistory() { history_ = Optional.empty(); }
    /** Retrieve the history module that the collector uses. */
    public synchronized Optional<CollectHistory> getHistory() { return history_; }

    public synchronized void decorate(TimeSeriesTransformer decorator) {
        decorators_.add(Objects.requireNonNull(decorator));
    }

    public TimeSeriesCollection getCollectionData() { return data_.getCurrentCollection(); }
    public Map<GroupName, Alert> getCollectionAlerts() { return alerts_; }
    @Override
    public Optional<Duration> getRuleEvalDuration() { return rule_eval_duration_; }

    /**
     * Begin a new collection cycle.
     *
     * Note that the cycle isn't stored (a call to commitCollection is required).
     * @return A new collection cycle.
     */
    private synchronized void beginCollection() {
        final DateTime now = requireNonNull(now_.get());
        data_.startNewCycle(now, ExpressionLookBack.EMPTY.andThen(decorators_.stream().map(TimeSeriesTransformer::getLookBack)));
        streamGroups(now).forEachOrdered(data_.getCurrentCollection()::add);
    }

    /**
     * Store a newly completed collection cycle.
     * @param data The collection cycle to store.
     */
    private synchronized void commitCollection(Map<GroupName, Alert> alerts) {
        alerts_ = unmodifiableMap(alerts);
        getHistory().ifPresent(history -> history.add(getCollectionData()));
        ListMetrics.update(data_.getCurrentCollection());
    }

    /**
     * Apply all timeseries decorators.
     * @param ctx Input timeseries.
     */
    private void apply_rules_and_decorators_(Context ctx) {
        decorators_.forEach(tf -> tf.transform(ctx));
    }

    /**
     * Combine a new alert with its previous state.
     * @param alert An emitted alert.
     * @param previous A map with alerts during the previous cycle.
     * @return An alert that its predecessor extended.
     */
    private static Alert combine_alert_with_past_(Alert alert, Map<GroupName, Alert> previous) {
        Alert result = Optional.ofNullable(previous.get(alert.getName()))
                .map((prev) -> prev.extend(alert))
                .orElse(alert);
        logger.log(Level.FINE, "emitting alert {0} -> {1}", new Object[]{result.getName(), result.getAlertState().name()});
        return result;
    }

    /**
     * Run an update cycle.
     * An update cycle consists of:
     * - gathering raw metrics
     * - creating a new, minimal context
     * - applying decorators against the current and previous values
     * - storing the collection values as the most recent capture
     */
    public synchronized void updateCollection() {
        final Map<GroupName, Alert> alerts = new HashMap<>();
        final Map<GroupName, Alert> previous_alerts = alerts_;
        final Consumer<Alert> alert_manager = (Alert alert) -> {
            Alert combined = combine_alert_with_past_(alert, previous_alerts);
            alerts.put(combined.getName(), combined);
        };
        beginCollection();
        final Context ctx = new MutableContext(data_, alert_manager);

        final long t0 = System.nanoTime();
        apply_rules_and_decorators_(ctx);
        final long t_rule_eval = System.nanoTime();
        rule_eval_duration_ = Optional.of(Duration.millis(TimeUnit.NANOSECONDS.toMillis(t_rule_eval - t0)));
        commitCollection(alerts);
    }

    /**
     * Create a plain, uninitialized metric registry.
     *
     * The metric registry is registered under its mbeanObjectName(package_name).
     * @param package_name The name of the package that owns this registry.
     * @param now A function returning DateTime.now(DateTimeZone.UTC).  Allowing specifying it, for the benefit of unit tests.
     * @param has_config True if the metric registry instance should mark monsoon as being supplied with a configuration file.
     * @param api The endpoint registration interface.
     * @return An empty metric registry.
     */
    public static synchronized PushMetricRegistryInstance create(String package_name, Supplier<DateTime> now, boolean has_config, EndpointRegistration api) {
        return new PushMetricRegistryInstance(package_name, now, has_config, api);
    }
}
