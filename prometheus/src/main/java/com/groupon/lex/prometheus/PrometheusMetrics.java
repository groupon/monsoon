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
package com.groupon.lex.prometheus;

import com.groupon.lex.metrics.GroupName;
import com.groupon.lex.metrics.MetricName;
import com.groupon.lex.metrics.MetricValue;
import com.groupon.lex.metrics.PullMetricRegistryInstance;
import com.groupon.lex.metrics.timeseries.TimeSeriesValue;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 *
 * @author nolofsson
 */
public class PrometheusMetrics {
    private static final Logger LOG = Logger.getLogger(PrometheusMetrics.class.getName());
    public final static String PACKAGE_NAME = PrometheusMetrics.class.getPackage().getName();

     /**
     * @param registry
     * @return
     * @throws java.lang.Exception
     * @FilteredMetrics()
     * Returns a Stream of PrometheusMetrics
     * It will filter out None values and replace all Characters that
     * Does not confirm to Prometheus Metric Format.
     */
    public static Stream<PrometheusMetric> filteredMetrics(PullMetricRegistryInstance registry) throws Exception {
        Stream <PrometheusMetric> m = registry.updateCollection().getTSValues().stream()
                .flatMap((TimeSeriesValue i) -> {
                    Map<MetricName, MetricValue> metrics = i.getMetrics();
                    GroupName group = i.getGroup();
                    return metrics.entrySet().stream()
                            .filter((kv) -> kv.getValue().value().isPresent())
                            .map((kv) -> {
                                final String metric_group = toPrometheusString_(group.getPath().getPath());
                                final Map<String, String> tags = group.getTags().stream()
                                        .filter(tag -> tag.getValue().asString().isPresent())
                                        .collect(Collectors.toMap(
                                                tag -> escapeprometheus(tag.getKey()),
                                                tag -> escapeLabelValue_(tag.getValue().asString().get())));
                                final String metric_name = toPrometheusString_(kv.getKey().getPath());
                                final Number metric_value = kv.getValue().value().get();
                                return new PrometheusMetric(metric_group, tags, metric_name, metric_value);
                            });
                });
        return m;
    }

    /**
     *This gets passed a list and concatenate them together making sure that there
     * is no forbidden characters and remove the if necessary.
     *
     */
    private static String toPrometheusString_(List<String> s) {
         return String.join("_",
                s.stream()
                .map(PrometheusMetrics::escapeprometheus)
                .collect(Collectors.toList()));
    }

    private static String escapeLabelValue_(String v) {
        return '"' + v.replaceAll("[\\\\\n\"]", "\\$0") + '"';
    }

    /**
     * [a-zA-Z_:][a-zA-Z0-9_:]* This is what i need to match
     * Removing all characters that do not meet the regex.
     */
    private static String escapeprometheus(String s) {
       s = s.replaceAll("^[^a-zA-Z_:]+", "");
       s = s.replaceAll("[^a-zA-Z0-9_:]+", "");
       return s.toLowerCase();
    }
}


