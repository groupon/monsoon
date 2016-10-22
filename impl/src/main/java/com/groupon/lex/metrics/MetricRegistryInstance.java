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
import com.groupon.lex.metrics.lib.Any2;
import com.groupon.lex.metrics.misc.MonitorMonitor;
import com.groupon.lex.metrics.timeseries.Alert;
import com.groupon.lex.metrics.timeseries.ExpressionLookBack;
import com.groupon.lex.metrics.timeseries.MutableTimeSeriesValue;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollection;
import com.groupon.lex.metrics.timeseries.TimeSeriesTransformer;
import com.groupon.lex.metrics.timeseries.expression.Context;
import com.groupon.lex.metrics.timeseries.expression.MutableContext;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import static java.util.Collections.singleton;
import java.util.List;
import java.util.Objects;
import static java.util.Objects.requireNonNull;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.NonNull;
import org.joda.time.DateTime;
import org.joda.time.Duration;

/**
 *
 * @author ariane
 */
public abstract class MetricRegistryInstance implements MetricRegistry, AutoCloseable {
    private static final Logger LOG = Logger.getLogger(MetricRegistryInstance.class.getName());
    private static final AtomicLong THREADPOOL_ID = new AtomicLong();
    private static final ExecutorService THREADPOOL = new ThreadPoolExecutor(4, 1000,
            5L, TimeUnit.MINUTES,
            new SynchronousQueue<>(),
            (Runnable r) -> {
                Thread thr = new Thread(r);
                thr.setDaemon(true);
                thr.setName("metric-registry-thread-0x" + Long.toUnsignedString(THREADPOOL_ID.incrementAndGet() - 1L, 16));
                return thr;
            });
    private static final long MAX_COLLECTOR_WAIT_MSEC = 29 * 1000;  // msec
    private static final long COLLECTOR_POST_TIMEOUT_MSEC = 1 * 1000;  // msec
    private final Collection<GroupGenerator> generators_ = new ArrayList<>();
    private long failed_collections_ = 0;
    private final boolean has_config_;
    private Optional<Duration> scrape_duration_ = Optional.empty();
    private Optional<Duration> rule_eval_duration_ = Optional.empty();
    private Optional<Duration> processor_duration_ = Optional.empty();
    private final EndpointRegistration api_;
    private final Collection<TimeSeriesTransformer> decorators_ = new ArrayList<>();
    private Supplier<DateTime> now_;
    private final ListMetrics list_metrics_;

    protected MetricRegistryInstance(@NonNull Supplier<DateTime> now, boolean has_config, @NonNull EndpointRegistration api) {
        api_ = api;
        has_config_ = has_config;
        decorators_.add(new MonitorMonitor(this));
        now_ = requireNonNull(now);
        list_metrics_ = new ListMetrics();
        api_.addEndpoint("/monsoon/metrics", list_metrics_);
    }

    protected MetricRegistryInstance(boolean has_config, EndpointRegistration api) {
        this(TimeSeriesCollection::now, has_config, api);
    }

    @Override
    public EndpointRegistration getApi() {
        return api_;
    }

    public synchronized GroupGenerator add(GroupGenerator g) {
        generators_.add(g);
        return g;
    }

    public synchronized void remove(GroupGenerator g) {
        generators_.remove(g);
    }

    /**
     * Retrieve the number of collectors that encountered a failure during the
     * last call to streamGroups().
     *
     * @return The number of collectors that failed.
     */
    public long getFailedCollections() {
        return failed_collections_;
    }

    /**
     * Retrieve timing for scrape.
     *
     * @return The duration it took to complete all scrapes.
     */
    public Optional<Duration> getScrapeDuration() {
        return scrape_duration_;
    }

    /**
     * Retrieve timing for rule evaluation.
     *
     * @return The duration it took to evaluate all rules.
     */
    public Optional<Duration> getRuleEvalDuration() {
        return rule_eval_duration_;
    }

    /**
     * Retrieve timing for processor to handle the data.
     *
     * @return The duration it took for the processor, to push all the data out.
     */
    public Optional<Duration> getProcessorDuration() {
        return processor_duration_;
    }

    /**
     * Update the processor duration.
     *
     * @param duration The time spent in the processor.
     */
    public void updateProcessorDuration(Duration duration) {
        processor_duration_ = Optional.of(duration);
    }

    private synchronized Stream<MutableTimeSeriesValue> streamGroups(DateTime now) {
        final long t0 = System.nanoTime();

        final CompletableFuture<GroupGenerator.TimeoutObject> timeout = new CompletableFuture<>();
        Collection<MetricGroup> collections = derefFutures(generators_.stream()
                .map(generator -> {
                    try {
                        return generator.getGroups(THREADPOOL, timeout);
                    } catch (Exception ex) {
                        CompletableFuture<? extends Collection<? extends MetricGroup>> failure = new CompletableFuture<>();
                        failure.completeExceptionally(ex);
                        return singleton(failure);
                    }
                })
                .flatMap(Collection::stream)
                .collect(Collectors.toList()),
                t0,
                timeout);

        /* Measure end time of collections. */
        final long t_collections = System.nanoTime();
        scrape_duration_ = Optional.of(Duration.millis(TimeUnit.NANOSECONDS.toMillis(t_collections - t0)));

        Stream<MutableTimeSeriesValue> groups = collections.stream()
                .collect(Collectors.toMap(MetricGroup::getName, Function.identity(), (x, y) -> y)) // Resolve group-name conflict, such that latest metric wins.
                .values()
                .stream()
                .map((mg) -> new MutableTimeSeriesValue(now, mg.getName(), Arrays.stream(mg.getMetrics()), Metric::getName, Metric::getValue));
        return groups;
    }

    /**
     * Handles collecting the future arguments, correctly firing the timeout.
     * This function also updates the failed_collections_ member variable.
     *
     * @param futures The futures of MetricGroups to dereference.
     * @param t0_nsec The starting time of the scrape.
     * @param timeout The future that informs collectors of the timeout event.
     * @return The dereferenced data from all futures that completed
     * successfully and on time.
     */
    private Collection<MetricGroup> derefFutures(Collection<CompletableFuture<? extends Collection<? extends MetricGroup>>> futures,
                                                 long t0_nsec,
                                                 CompletableFuture<GroupGenerator.TimeoutObject> timeout) {
        long tDeadline1 = t0_nsec + TimeUnit.MILLISECONDS.toNanos(MAX_COLLECTOR_WAIT_MSEC);
        long tDeadline2 = t0_nsec + TimeUnit.MILLISECONDS.toNanos(COLLECTOR_POST_TIMEOUT_MSEC);

        final List<MetricGroup> result = new ArrayList<>();
        final BlockingQueue<Any2<Collection<? extends MetricGroup>, Throwable>> readyQueue = new LinkedBlockingQueue<>();
        int failCount = 0;
        int pendingCount = futures.size();
        futures.forEach(fut -> {
            fut.handle((value, exc) -> {
                if (exc != null) {
                    if (!(exc instanceof CancellationException))
                        LOG.log(Level.INFO, "collector failed", exc);
                    readyQueue.add(Any2.right(exc));
                }
                if (value != null)
                    readyQueue.add(Any2.left(value));
                return null;
            });
        });

        // Collect everything that completes on time.
        while (pendingCount > 0) {
            final long tNow = System.nanoTime();
            if (tDeadline1 - tNow <= 0) break;  // GUARD

            final Any2<Collection<? extends MetricGroup>, Throwable> readyItem;
            try {
                readyItem = readyQueue.poll(tDeadline1 - tNow, TimeUnit.NANOSECONDS);
            } catch (InterruptedException ex) {
                LOG.log(Level.INFO, "interrupted while waiting for scrape data", ex);
                continue;
            }
            if (readyItem != null) {
                readyItem.getLeft().ifPresent(result::addAll);
                if (readyItem.getRight().isPresent())
                    ++failCount;
                --pendingCount;
            }
        }

        // Fire timeout event.
        timeout.complete(new GroupGenerator.TimeoutObject());

        // Collect everything we can get within the grace period.
        while (pendingCount > 0) {
            final long tNow = System.nanoTime();
            if (tDeadline2 - tNow <= 0) break;  // GUARD

            final Any2<Collection<? extends MetricGroup>, Throwable> readyItem;
            try {
                readyItem = readyQueue.poll(tDeadline2 - tNow, TimeUnit.NANOSECONDS);
            } catch (InterruptedException ex) {
                LOG.log(Level.INFO, "interrupted while waiting for scrape data", ex);
                continue;
            }
            if (readyItem != null) {
                readyItem.getLeft().ifPresent(result::addAll);
                if (readyItem.getRight().isPresent())
                    ++failCount;
                --pendingCount;
            }
        }

        // Collect everything that is present.
        while (pendingCount > 0) {
            final Any2<Collection<? extends MetricGroup>, Throwable> readyItem
                    = readyQueue.poll();
            if (readyItem == null) break;  // GUARD
            readyItem.getLeft().ifPresent(result::addAll);
            if (readyItem.getRight().isPresent())
                ++failCount;
            --pendingCount;
        }

        // Expose failure count.
        failed_collections_ = pendingCount + failCount;
        return result;
    }

    public synchronized void decorate(TimeSeriesTransformer decorator) {
        decorators_.add(Objects.requireNonNull(decorator));
    }

    /**
     * Closes the MetricRegistryInstance.
     */
    @Override
    public void close() {
        generators_.forEach((g) -> {
            try {
                g.close();
            } catch (Exception e) {
                LOG.log(Level.SEVERE, "failed to close group generator " + g, e);
            }
        });
        if (api_ instanceof AutoCloseable) {
            try {
                ((AutoCloseable) api_).close();
            } catch (Exception ex) {
                LOG.log(Level.SEVERE, "unable to close API " + api_.getClass(), ex);
            }
        }
    }

    @Override
    public boolean hasConfig() {
        return has_config_;
    }

    public DateTime now() {
        return requireNonNull(now_.get());
    }

    /**
     * Apply all timeseries decorators.
     *
     * @param ctx Input timeseries.
     */
    protected void apply_rules_and_decorators_(Context ctx) {
        decorators_.forEach(tf -> tf.transform(ctx));
    }

    public ExpressionLookBack getDecoratorLookBack() {
        return ExpressionLookBack.EMPTY.andThen(decorators_.stream().map(TimeSeriesTransformer::getLookBack));
    }

    public static interface CollectionContext {
        public Consumer<Alert> alertManager();

        public MutableTimeSeriesCollectionPair tsdata();

        public void commit();
    }

    protected abstract CollectionContext beginCollection(DateTime now);

    /**
     * Run an update cycle. An update cycle consists of: - gathering raw metrics
     * - creating a new, minimal context - applying decorators against the
     * current and previous values - storing the collection values as the most
     * recent capture
     */
    public TimeSeriesCollection updateCollection() {
        // Scrape metrics from all collectors.
        final DateTime now = now();
        final CollectionContext cctx = beginCollection(now);
        final MutableTimeSeriesCollectionPair tsdata = cctx.tsdata();
        streamGroups(now).forEach(tsdata.getCurrentCollection()::add);

        // Build a rule evaluation context.
        final Context ctx = new MutableContext(tsdata, cctx.alertManager());

        // Apply rules.
        final long t0 = System.nanoTime();
        apply_rules_and_decorators_(ctx);
        final long t_rule_eval = System.nanoTime();
        rule_eval_duration_ = Optional.of(Duration.millis(TimeUnit.NANOSECONDS.toMillis(t_rule_eval - t0)));

        // Publish new set of metrics.
        list_metrics_.update(tsdata.getCurrentCollection());

        // Inform derived class that we are done.
        cctx.commit();

        // Return tsdata.
        return tsdata.getCurrentCollection();
    }
}
