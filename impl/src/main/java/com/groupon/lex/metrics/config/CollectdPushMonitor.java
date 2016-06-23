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
package com.groupon.lex.metrics.config;

import static com.groupon.lex.metrics.ConfigSupport.quotedString;
import com.groupon.lex.metrics.MetricRegistryInstance;
import com.groupon.lex.metrics.SimpleGroupPath;
import com.groupon.lex.metrics.collector.httpget.CollectdPushCollector;

/**
 *
 * @author ariane
 */
public class CollectdPushMonitor implements MonitorStatement {
    private final String api_name_;
    private final SimpleGroupPath base_name_;

    public CollectdPushMonitor(String api_name, SimpleGroupPath base_name) {
        api_name_ = api_name;
        base_name_ = base_name;
    }

    @Override
    public void apply(MetricRegistryInstance registry) throws Exception {
        registry.add(new CollectdPushCollector(registry.getApi(), base_name_, api_name_));
    }

    @Override
    public StringBuilder configString() {
        return new StringBuilder()
                .append("collect collectd_push ")
                .append(quotedString(api_name_))
                .append(" as ")
                .append(base_name_.configString())
                .append(';');
    }
}
