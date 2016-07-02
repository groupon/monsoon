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
import java.util.ArrayList;
import java.util.List;
import lombok.NonNull;

public class PipelineBuilder {
    public static interface PushProcessorSupplier {
        public PushProcessor build(EndpointRegistration epr) throws Exception;
    }

    @NonNull
    private final Configuration cfg_;
    private int api_port_ = 9998;
    private CollectHistory history_;
    private int interval_ = 60;

    public PipelineBuilder(Configuration cfg) {
        cfg_ = cfg;
    }

    public PipelineBuilder(File dir, Reader reader) throws IOException, ConfigurationException {
        cfg_ = Configuration.readFromFile(dir, reader).resolve();
    }

    public PipelineBuilder(File file) throws IOException, ConfigurationException {
        cfg_ = Configuration.readFromFile(file).resolve();
    }

    public PipelineBuilder withApiPort(int api_port) {
        if (api_port <= 0 || api_port >= 65536) throw new IllegalArgumentException("port must be a valid TCP port");
        api_port_ = api_port;
        return this;
    }

    public PipelineBuilder withHistory(CollectHistory history) {
        history_ = history;
        return this;
    }

    public PipelineBuilder withCollectIntervalSeconds(int seconds) {
        if (seconds <= 0) throw new IllegalArgumentException("not enough seconds: " + seconds);
        interval_ = seconds;
        return this;
    }

    public PushProcessorPipeline build(List<PushProcessorSupplier> processor_suppliers) throws Exception {
        ApiServer api = null;
        PushMetricRegistryInstance registry = null;
        final List<PushProcessor> processors = new ArrayList<>(processor_suppliers.size());
        try {
            api = new ApiServer(api_port_);
            for (PushProcessorSupplier pps : processor_suppliers)
                processors.add(pps.build(api));

            registry = cfg_.create(api);
            if (history_ != null)
                registry.setHistory(history_);
            return new PushProcessorPipeline(registry, interval_, processors);
        } catch (Exception ex) {
            if (api != null) api.close();
            if (registry != null) registry.close();
            for (PushProcessor pp : processors)
                pp.close();
            throw ex;
        }
    }
}
