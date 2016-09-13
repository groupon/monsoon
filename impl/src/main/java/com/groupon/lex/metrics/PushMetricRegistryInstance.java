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
import com.groupon.lex.metrics.history.CollectHistory;
import com.groupon.lex.metrics.httpd.EndpointRegistration;
import com.groupon.lex.metrics.timeseries.Alert;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollection;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollectionPair;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollectionPairInstance;
import static java.util.Collections.unmodifiableMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.joda.time.DateTime;

/**
 *
 * @author ariane
 */
public class PushMetricRegistryInstance extends MetricRegistryInstance {
    private static final Logger logger = Logger.getLogger(PushMetricRegistryInstance.class.getName());
    private final TimeSeriesCollectionPairInstance data_ = new TimeSeriesCollectionPairInstance();
    private Map<GroupName, Alert> alerts_ = new HashMap<>();
    private Optional<CollectHistory> history_ = Optional.empty();

    public PushMetricRegistryInstance(boolean has_config, EndpointRegistration api) {
        super(has_config, api);
    }

    public PushMetricRegistryInstance(Supplier<DateTime> now, boolean has_config, EndpointRegistration api) {
        super(now, has_config, api);
    }

    /** Set the history module that the push processor is to use. */
    public synchronized void setHistory(CollectHistory history) {
        history_ = Optional.of(history);
        getApi().addEndpoint("/monsoon/eval", new ExprEval(history_.get()));
        getApi().addEndpoint("/monsoon/eval/validate", new ExprValidate());
        getApi().addEndpoint("/monsoon/eval/gchart", new ExprEvalGraphServlet(history_.get()));
        data_.initWithHistoricalData(history, getDecoratorLookBack());
    }
    /** Clear the history module. */
    public synchronized void clearHistory() { history_ = Optional.empty(); }
    /** Retrieve the history module that the collector uses. */
    public synchronized Optional<CollectHistory> getHistory() { return history_; }

    public TimeSeriesCollection getCollectionData() { return data_.getCurrentCollection(); }
    public Map<GroupName, Alert> getCollectionAlerts() { return alerts_; }

    /**
     * Begin a new collection cycle.
     *
     * Note that the cycle isn't stored (a call to commitCollection is required).
     * @return A new collection cycle.
     */
    @Override
    protected synchronized CollectionContext beginCollection(DateTime now) {
        data_.startNewCycle(now, getDecoratorLookBack());

        return new CollectionContext() {
            final Map<GroupName, Alert> alerts = new HashMap<>();

            @Override
            public Consumer<Alert> alertManager() {
                return (Alert alert) -> {
                    Alert combined = combine_alert_with_past_(alert, alerts_);
                    alerts.put(combined.getName(), combined);
                };
            }

            @Override
            public TimeSeriesCollectionPair tsdata() {
                return data_;
            }

            /**
             * Store a newly completed collection cycle.
             */
            @Override
            public void commit() {
                alerts_ = unmodifiableMap(alerts);
                try {
                    getHistory().ifPresent(history -> history.add(getCollectionData()));
                } catch (Exception ex) {
                    logger.log(Level.WARNING, "unable to add collection data to history (dropped)", ex);
                }
            }
        };
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
    @Override
    public synchronized TimeSeriesCollection updateCollection() {
        // We override, so we can ensure only one runs at any given moment.
        return super.updateCollection();
    }

    /**
     * Create a plain, uninitialized metric registry.
     *
     * The metric registry is registered under its mbeanObjectName(package_name).
     * @param now A function returning DateTime.now(DateTimeZone.UTC).  Allowing specifying it, for the benefit of unit tests.
     * @param has_config True if the metric registry instance should mark monsoon as being supplied with a configuration file.
     * @param api The endpoint registration interface.
     * @return An empty metric registry.
     */
    public static synchronized PushMetricRegistryInstance create(Supplier<DateTime> now, boolean has_config, EndpointRegistration api) {
        return new PushMetricRegistryInstance(now, has_config, api);
    }
}
