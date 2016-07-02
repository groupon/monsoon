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

import com.groupon.lex.metrics.timeseries.Alert;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollection;
import java.util.ArrayList;
import static java.util.Collections.singletonList;
import static java.util.Collections.unmodifiableList;
import java.util.List;
import java.util.Map;
import static java.util.Objects.requireNonNull;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.joda.time.Duration;

/**
 * A push pipeline.
 *
 * The pipeline periodically performs a scrape, processes it and then emits it
 * to all processors it holds.
 * @author ariane
 */
public class PushProcessorPipeline extends AbstractProcessor<PushMetricRegistryInstance> implements Runnable {
    private static final Logger logger = Logger.getLogger(PushProcessorPipeline.class.getName());
    private static final int INITIAL_RUN_DELAY = 5;  /* seconds */
    private final int interval_seconds_;
    private Optional<ScheduledExecutorService> owner_executor_ = Optional.empty();
    private Optional<ScheduledFuture<?>> my_task_ = Optional.empty();
    private final List<PushProcessor> processors_;

    public PushProcessorPipeline(PushMetricRegistryInstance registry, int interval_seconds, List<PushProcessor> processors) {
        super(registry);
        interval_seconds_ = interval_seconds;
        processors_ = unmodifiableList(new ArrayList<>(processors));
    }

    public PushProcessorPipeline(PushMetricRegistryInstance registry, int interval_seconds, PushProcessor processor) {
        this(registry, interval_seconds, singletonList(requireNonNull(processor)));
    }

    /**
     * Run the implementation of uploading the values and alerts.
     * @throws java.lang.Exception thrown by implementation.
     */
    private void run_implementation_(PushProcessor p) throws Exception {
        final Map<GroupName, Alert> alerts = registry_.getCollectionAlerts();
        final TimeSeriesCollection tsdata = registry_.getCollectionData();
        p.accept(tsdata, alerts);
    }

    /**
     * Run the collection cycle.
     */
    @Override
    public final void run() {
        registry_.updateCollection();

        final long t0 = System.nanoTime();

        processors_.forEach(p -> {
            try {
                run_implementation_(p);
            } catch (Exception ex) {
                logger.log(Level.SEVERE, p.getClass().getName() + " failed to run properly, some or all metrics may be missed this cycle", ex);
            }
        });

        final long t_processor = System.nanoTime();
        registry_.updateProcessorDuration(Duration.millis(TimeUnit.NANOSECONDS.toMillis(t_processor - t0)));
    }

    public final Support getSupport() { return registry_.getSupport(); }
    public final int getIntervalSeconds() { return interval_seconds_; }

    private synchronized void stop_() {
        my_task_.ifPresent((sf) -> {
                    logger.log(Level.INFO, "Stopping thread for push processor for {0}", getMetricRegistry().getPackageName());
                    sf.cancel(false);
                });
        my_task_ = Optional.empty();

        owner_executor_.ifPresent(ScheduledExecutorService::shutdown);
        owner_executor_ = Optional.empty();
    }

    private synchronized void start_(ScheduledExecutorService ses, boolean own_ses) {
        stop_();
        logger.log(Level.INFO, "Starting thread for push processor for {0}", getMetricRegistry().getPackageName());
        my_task_ = Optional.of(ses.scheduleAtFixedRate(this, INITIAL_RUN_DELAY, getIntervalSeconds(), TimeUnit.SECONDS));
        if (own_ses) owner_executor_ = Optional.of(ses);
    }

    /**
     * Start the push processor, using a daemon thread.
     * @param daemon If the thread should be a daemon thread.
     * @param thread_priority The priority of the thread running the push processor.
     */
    public void start(boolean daemon, int thread_priority) {
        if (thread_priority < Thread.MIN_PRIORITY) throw new IllegalArgumentException("thread priority too low");
        if (thread_priority > Thread.MAX_PRIORITY) throw new IllegalArgumentException("thread priority too high");

        start_(Executors.newSingleThreadScheduledExecutor((Runnable r) -> {
                    logger.entering(getClass().getName(), "start_", r);
                    Thread t = new Thread(r, getMetricRegistry().getPackageName());
                    t.setDaemon(daemon);
                    t.setPriority(thread_priority);
                    return t;
                }),
                true);
    }

    /**
     * Start the push processor.
     */
    public void start() {
        start(false, Thread.NORM_PRIORITY);
    }

    /**
     * Start the push processor.
     * @param daemon If the thread should be a daemon thread.
     */
    public void start(boolean daemon) {
        start(daemon, Thread.NORM_PRIORITY);
    }

    /**
     * Start the push processor.
     * @param thread_priority The priority of the thread running the push processor.
     */
    public void start(int thread_priority) {
        start(false, thread_priority);
    }

    /**
     * Start the push processor, using the supplied ScheduledExecutorService.
     * @param ses ScheduledExecutorService that will execute this PushProcessor.
     */
    public void start(ScheduledExecutorService ses) {
        start_(ses, false);
    }

    /**
     * Schedule this task.
     * @param ses ScheduledExecutorService that will execute the this.
     * @return A ScheduledFuture, that provides cancellation.
     */
    public ScheduledFuture<?> createTimerTask(ScheduledExecutorService ses) {
        return ses.scheduleAtFixedRate(this, INITIAL_RUN_DELAY, getIntervalSeconds(), TimeUnit.SECONDS);
    }

    @Override
    public void close() {
        logger.log(Level.INFO, "Closing push processor for {0}", getMetricRegistry().getPackageName());
        stop_();
        super.close();

        processors_.forEach(p -> {
            try {
                p.close();
            } catch (Exception ex) {
                logger.log(Level.SEVERE, "failed to close " + p.getClass().getName(), ex);
            }
        });
    }
}
