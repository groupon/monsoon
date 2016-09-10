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

import com.groupon.lex.metrics.api.ApiServer;
import com.groupon.lex.metrics.config.Configuration;
import com.groupon.lex.metrics.config.ConfigurationException;
import com.groupon.lex.metrics.history.CollectHistory;
import com.groupon.lex.metrics.httpd.EndpointRegistration;
import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import static java.util.Collections.singletonList;
import java.util.List;
import lombok.NonNull;

public class PipelineBuilder {
    /**
     * A supplier for Push Processors.
     *
     * The supplier takes an endpoint registration object, which can be used to
     * create additional endpoints for this processor.
     */
    public static interface PushProcessorSupplier {
        public PushProcessor build(EndpointRegistration epr) throws Exception;
    }

    public static final int DEFAULT_API_PORT = 9998;
    public static final int DEFAULT_COLLECT_INTERVAL_SECONDS = 60;

    @NonNull
    private final Configuration cfg_;
    private InetSocketAddress api_sockaddr_ = new InetSocketAddress(DEFAULT_API_PORT);
    private CollectHistory history_;
    private EndpointRegistration epr_;
    private int collect_interval_seconds_ = DEFAULT_COLLECT_INTERVAL_SECONDS;

    /** Create a new pipeline with the given configuration. */
    public PipelineBuilder(@NonNull Configuration cfg) {
        cfg_ = cfg;
    }

    /** Create a new pipeline builder from a reader. */
    public PipelineBuilder(File dir, Reader reader) throws IOException, ConfigurationException {
        cfg_ = Configuration.readFromFile(dir, reader).resolve();
    }

    /** Create a new pipeline builder from the given file. */
    public PipelineBuilder(File file) throws IOException, ConfigurationException {
        cfg_ = Configuration.readFromFile(file).resolve();
    }

    /** Make the API listen on the specified port. */
    public PipelineBuilder withApiPort(int api_port) {
        return withApiSockaddr(new InetSocketAddress(api_port));
    }

    /** Make the API listen on the specified address. */
    public PipelineBuilder withApiSockaddr(@NonNull InetSocketAddress api_sockaddr) {
        api_sockaddr_ = api_sockaddr;
        return this;
    }

    /** Use a non-standard API server. */
    public PipelineBuilder withApi(EndpointRegistration epr) {
        epr_ = epr;
        return this;
    }

    /** Add a history module. */
    public PipelineBuilder withHistory(CollectHistory history) {
        history_ = history;
        return this;
    }

    /** Use the specified seconds as scrape interval. */
    public PipelineBuilder withCollectIntervalSeconds(int seconds) {
        if (seconds <= 0) throw new IllegalArgumentException("not enough seconds: " + seconds);
        collect_interval_seconds_ = seconds;
        return this;
    }

    /**
     * Creates a push processor.
     *
     * The API server is started by this method.
     * The returned push processor is not started.
     * @param processor_suppliers A list of PushProcessorSupplier, that will instantiate the push processors.
     * @return A Push Processor pipeline.  You'll need to start it yourself.
     * @throws Exception indicating construction failed.
     *     Push Processors that were created before the exception was thrown, will be closed.
     */
    public PushProcessorPipeline build(List<PushProcessorSupplier> processor_suppliers) throws Exception {
        ApiServer api = null;
        PushMetricRegistryInstance registry = null;
        final List<PushProcessor> processors = new ArrayList<>(processor_suppliers.size());
        try {
            final EndpointRegistration epr;
            if (epr_ == null)
                epr = api = new ApiServer(api_sockaddr_);
            else
                epr = epr_;

            registry = cfg_.create(PushMetricRegistryInstance::new, epr);
            for (PushProcessorSupplier pps : processor_suppliers)
                processors.add(pps.build(epr));

            if (history_ != null)
                registry.setHistory(history_);
            if (api != null) api.start();
            return new PushProcessorPipeline(registry, collect_interval_seconds_, processors);
        } catch (Exception ex) {
            try {
                if (api != null) api.close();
            } catch (Exception ex1) {
                ex.addSuppressed(ex1);
            }

            try {
                if (registry != null) registry.close();
            } catch (Exception ex1) {
                ex.addSuppressed(ex1);
            }

            for (PushProcessor pp : processors) {
                try {
                    pp.close();
                } catch (Exception ex1) {
                    ex.addSuppressed(ex1);
                }
            }

            throw ex;
        }
    }

    /**
     * Creates a push processor.
     *
     * The API server is started by this method.
     * The returned push processor is not started.
     * @param processor_supplier A PushProcessorSupplier, that will instantiate the push processor.
     * @return A Push Processor pipeline.  You'll need to start it yourself.
     * @throws Exception indicating construction failed.
     *     Push Processors that were created before the exception was thrown, will be closed.
     */
    public PushProcessorPipeline build(PushProcessorSupplier processor_supplier) throws Exception {
        return build(singletonList(processor_supplier));
    }

    /*
     * Creates a pull processor.
     *
     * The API server is started by this method.
     * The returned pull processor is thread-safe.
     *
     * Note that the history will not be used.
     * @return A PullMetricRegistryInstance that performs a scrape and evaluates rules.
     * @throws Exception indicating construction failed.
     */
    public PullProcessorPipeline build() throws Exception {
        ApiServer api = null;
        PullMetricRegistryInstance registry = null;
        try {
            final EndpointRegistration epr;
            if (epr_ == null)
                epr = api = new ApiServer(api_sockaddr_);
            else
                epr = epr_;

            registry = cfg_.create(PullMetricRegistryInstance::new, epr);

            if (api != null) api.start();
            return new PullProcessorPipeline(registry);
        } catch (Exception ex) {
            // Close API server.
            try {
                if (api != null) api.close();
            } catch (Exception ex1) {
                ex.addSuppressed(ex1);
            }

            // Close registry.
            try {
                if (registry != null) registry.close();
            } catch (Exception ex1) {
                ex.addSuppressed(ex1);
            }

            throw ex;
        }
    }
}
