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

import com.groupon.lex.metrics.api.endpoints.ListMetrics;
import com.groupon.lex.metrics.httpd.EndpointRegistration;
import com.groupon.lex.metrics.timeseries.MutableTimeSeriesValue;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollection;
import com.groupon.lex.metrics.timeseries.TimeSeriesValue;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import static java.util.Objects.requireNonNull;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.joda.time.DateTime;
import org.joda.time.Duration;

/**
 *
 * @author ariane
 */
public class MetricRegistryInstance implements MetricRegistry, AutoCloseable {
    private static final Logger logger = Logger.getLogger(MetricRegistryInstance.class.getName());
    private final Collection<GroupGenerator> generators_ = new ArrayList<>();
    private long failed_collections_ = 0;
    private final boolean has_config_;
    private Optional<Duration> scrape_duration_ = Optional.empty();
    private Optional<Duration> processor_duration_ = Optional.empty();
    private final EndpointRegistration api_;

    protected MetricRegistryInstance(boolean has_config, EndpointRegistration api) {
        api_ = requireNonNull(api);
        has_config_ = has_config;
        api_.addEndpoint("/monsoon/metrics", new ListMetrics());
    }

    @Override
    public EndpointRegistration getApi() { return api_; }

    public synchronized GroupGenerator add(GroupGenerator g) {
        generators_.add(g);
        return g;
    }

    public synchronized void remove(GroupGenerator g) {
        generators_.remove(g);
    }

    /**
     * Retrieve the number of collectors that encountered a failure during the last call to streamGroups().
     * @return The number of collectors that failed.
     */
    public long getFailedCollections() { return failed_collections_; }
    /**
     * Retrieve timing for scrape.
     * @return The duration it took to complete all scrapes.
     */
    public Optional<Duration> getScrapeDuration() { return scrape_duration_; }
    /**
     * Retrieve timing for rule evaluation.
     * @return The duration it took to evaluate all rules.
     */
    public Optional<Duration> getRuleEvalDuration() { return Optional.empty(); }
    /**
     * Retrieve timing for processor to handle the data.
     * @return The duration it took for the processor, to push all the data out.
     */
    public Optional<Duration> getProcessorDuration() { return processor_duration_; }
    /**
     * Update the processor duration.
     * @param duration The time spent in the processor.
     */
    public void updateProcessorDuration(Duration duration) { processor_duration_ = Optional.of(duration); }

    public Stream<TimeSeriesValue> streamGroups() {
        return streamGroups(TimeSeriesCollection.now());
    }

    public synchronized Stream<TimeSeriesValue> streamGroups(DateTime now) {
        final long t0 = System.nanoTime();

        List<GroupGenerator.GroupCollection> collections = generators_.parallelStream()
                .map(GroupGenerator::getGroups)
                .collect(Collectors.toList());

        /* Count collection failures. */
        failed_collections_ = collections.stream()
                .filter((result) -> !result.isSuccessful())
                .count();
        /* Measure end time of collections. */
        final long t_collections = System.nanoTime();
        scrape_duration_ = Optional.of(Duration.millis(TimeUnit.NANOSECONDS.toMillis(t_collections - t0)));

        Stream<TimeSeriesValue> groups = collections.stream()
                .map(GroupGenerator.GroupCollection::getGroups)
                .flatMap(Collection::stream)
                .collect(Collectors.toMap(MetricGroup::getName, Function.identity(), (x, y) -> y))  // Resolve group-name conflict, such that latest metric wins.
                .values()
                .stream()
                .map((mg) -> new MutableTimeSeriesValue(now, mg.getName(), Arrays.stream(mg.getMetrics()), Metric::getName, Metric::getValue));
        return groups;
    }

    @Override
    public synchronized GroupName[] getGroupNames() {
        return streamGroups().map(TimeSeriesValue::getGroup).toArray(GroupName[]::new);
    }

    /**
     * Create a plain, uninitialized metric registry.
     *
     * The metric registry is registered under its mbeanObjectName(package_name).
     * @param has_config True if the metric registry will be configured.
     * @return An empty metric registry.
     */
    public static synchronized MetricRegistryInstance create(boolean has_config, EndpointRegistration api) {
        return new MetricRegistryInstance(has_config, api);
    }

    /**
     * Closes the MetricRegistryInstance.
     */
    @Override
    public void close() {
        generators_.forEach((g) -> {
                    try {
                        g.close();
                    } catch (Throwable t) {
                        logger.log(Level.SEVERE, "failed to close group generator " + g, t);
                    }
                });
    }

    @Override
    public boolean hasConfig() {
        return has_config_;
    }
}
