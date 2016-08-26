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

import com.groupon.lex.metrics.MetricRegistryInstance;
import com.groupon.lex.metrics.SimpleGroupPath;
import com.groupon.lex.metrics.collector.httpget.UrlGetCollector;
import com.groupon.lex.metrics.collector.httpget.UrlPattern;
import com.groupon.lex.metrics.resolver.NameResolverSet;
import lombok.NonNull;

/**
 *
 * @author ariane
 */
public class UrlGetCollectorMonitor implements MonitorStatement {
    private final UrlPattern pattern_;
    private final SimpleGroupPath base_name_;

    public UrlGetCollectorMonitor(@NonNull SimpleGroupPath base_name, @NonNull String pattern, @NonNull NameResolverSet args) {
        pattern_ = new UrlPattern(pattern, args);
        base_name_ = base_name;
    }

    @Override
    public void apply(MetricRegistryInstance registry) throws Exception {
        registry.add(new UrlGetCollector(base_name_, pattern_));
    }

    @Override
    public StringBuilder configString() {
        final StringBuilder buf = new StringBuilder()
                .append("collect url ")
                .append(pattern_.getUrlTemplate().configString())
                .append(" as ")
                .append(base_name_.configString());

        if (!pattern_.getTemplateArgs().isEmpty())
            buf.append(pattern_.getTemplateArgs().configString());
        else
            buf.append(';');

        return buf.append('\n');
    }
}
