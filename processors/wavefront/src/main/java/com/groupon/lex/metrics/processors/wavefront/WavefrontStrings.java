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
package com.groupon.lex.metrics.processors.wavefront;

import com.groupon.lex.metrics.GroupName;
import com.groupon.lex.metrics.Histogram;
import com.groupon.lex.metrics.MetricName;
import com.groupon.lex.metrics.MetricValue;
import com.groupon.lex.metrics.NameCache;
import com.groupon.lex.metrics.SimpleGroupPath;
import com.groupon.lex.metrics.Tags;
import com.groupon.lex.metrics.lib.SimpleMapEntry;
import com.groupon.lex.metrics.timeseries.TimeSeriesValue;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.joda.time.DateTime;

public class WavefrontStrings {
    public final static int MAX_TAG_KEY_VAL_CHARS = 254;  // Max of 255 chars, but need to exclude the '=' sign between tag name and tag value.
    public final static int TRUNCATE_TAG_NAME = 127;  // We truncate tag names to 128 chars, so we don't have to truncate values so much.

    private WavefrontStrings() {}

    /**
     * Convert a name to a name that wavefront will accept.
     *
     * Wavefront documentation specifies a metric consists of the characters [-a-z_.]
     * Any disallowed characters are replaced with underscores.
     */
    public static final String name(String name) {
        return name
                .toLowerCase(Locale.ROOT)
                .replaceAll("[^-a-z_.]", "_");
    }

    /**
     * Convert a group+metric to a wavefront name.
     *
     * Concatenates the paths of a Group and a Metric, separating each path element with a dot ('.').
     *
     * Example:
     * - group path: [ 'example', 'group', 'path' ]
     * - and metric: [ 'metric', 'name' ]
     * are concatenated into "example.group.path.metric.name".
     *
     * The concatenated name is cleaned to only contain characters allowed by wavefront.
     * (See the WavefrontString.name(String) function.)
     */
    public static final String name(SimpleGroupPath group, MetricName metric) {
        return name(Stream.concat(group.getPath().stream(), metric.getPath().stream())
                .collect(Collectors.joining(".")));
    }

    /** Create a map of tags for wavefront. */
    public static Map<String, String> tags(Tags tags) {
        return tags.stream()
                .map(tag -> {
                    return tag.getValue().asString()
                            .map(v -> v.replace('"', '\''))
                            .map(s -> SimpleMapEntry.create(name(tag.getKey()), s));
                })
                .flatMap(opt -> opt.map(Stream::of).orElseGet(Stream::empty))
                .map(entry -> {
                    String k = entry.getKey();
                    String v = entry.getValue();
                    if (k.length() + v.length() > MAX_TAG_KEY_VAL_CHARS - 2) {  // 2 chars for the quotes around the value
                        if (k.length() > TRUNCATE_TAG_NAME) k = k.substring(0, TRUNCATE_TAG_NAME);
                        if (k.length() + v.length() > MAX_TAG_KEY_VAL_CHARS)
                            v = v.substring(0, MAX_TAG_KEY_VAL_CHARS - 2 - k.length());
                    }

                    return SimpleMapEntry.create(k, '"' + v + '"');
                })
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    /**
     * Create a wavefront compatible string representation of the metric value.
     *
     * If the metric value is empty or not representable in wavefront, an empty optional will be returned.
     */
    public static Optional<String> wavefrontValue(MetricValue mv) {
        // Omit NaN and Inf.
        if (mv.getFltValue() != null && (mv.getFltValue().isInfinite() || mv.getFltValue().isNaN())) return Optional.empty();
        return mv.value().map(Number::toString);
    }

    /** Convert a timestamp to the string representation that wavefront will accept. */
    public static String timestamp(DateTime ts) {
        return Long.toString(ts.getMillis() / 1000);
    }

    /**
     * Convert a metric to a wavefront string.
     *
     * Empty metrics and histograms do not emit a value.
     *
     * Note: the line is not terminated with a newline.
     */
    public static Optional<String> wavefrontLine(DateTime ts, GroupName group, MetricName metric, MetricValue metric_value) {
        return wavefrontValue(metric_value)
                .map(value -> {
                    final Map<String, String> tag_map = tags(group.getTags());
                    final String source = Optional.ofNullable(tag_map.remove("source"))
                            .orElseGet(() -> {
                                return Optional.ofNullable(tag_map.get("cluster"))
                                        .orElseGet(() -> tag_map.getOrDefault("moncluster", "\"monsoon\""));
                            });

                    return new StringBuilder()
                            .append(name(group.getPath(), metric))
                            .append(' ')
                            .append(value)
                            .append(' ')
                            .append(timestamp(ts))
                            .append(' ')
                            .append("source=").append(source)
                            .append(' ')
                            .append(tag_map.entrySet().stream()
                                    .map(entry -> entry.getKey() + "=" + entry.getValue())
                                    .collect(Collectors.joining(" "))
                            )
                            .toString();
                });
    }

    /**
     * Emits a series of histogram metrics, based on an input metric value.
     *
     * If the metric value does not hold a histogram, an empty Stream is returned.
     */
    public static Stream<Map.Entry<String, MetricValue>> expandHistogram(MetricValue v) {
        return v.histogram()
                .map(h -> {
                    /** hne: histogram, not empty. */
                    final Optional<Histogram> hne = Optional.of(h)
                            .filter(hh -> !hh.isEmpty());

                    final Stream<Map.Entry<String, Optional<Double>>> extra = Stream.of(
                            SimpleMapEntry.create("min", h.min()),
                            SimpleMapEntry.create("max", h.max()),
                            SimpleMapEntry.create("avg", h.avg()),
                            SimpleMapEntry.create("sum", Optional.of(h.sum())),
                            SimpleMapEntry.create("count", Optional.of(h.getEventCount())),
                            SimpleMapEntry.create("p50", hne.map(hh -> hh.percentile(50))),
                            SimpleMapEntry.create("p75", hne.map(hh -> hh.percentile(75))),
                            SimpleMapEntry.create("p85", hne.map(hh -> hh.percentile(85))),
                            SimpleMapEntry.create("p90", hne.map(hh -> hh.percentile(90))),
                            SimpleMapEntry.create("p95", hne.map(hh -> hh.percentile(95))),
                            SimpleMapEntry.create("p99", hne.map(hh -> hh.percentile(99))),
                            SimpleMapEntry.create("p995", hne.map(hh -> hh.percentile(99.5))),
                            SimpleMapEntry.create("p999", hne.map(hh -> hh.percentile(99.9))),
                            SimpleMapEntry.create("p9995", hne.map(hh -> hh.percentile(99.95))),
                            SimpleMapEntry.create("p9999", hne.map(hh -> hh.percentile(99.99))),
                            SimpleMapEntry.create("p99995", hne.map(hh -> hh.percentile(99.995))),
                            SimpleMapEntry.create("p99999", hne.map(hh -> hh.percentile(99.999))),
                            SimpleMapEntry.create("p999995", hne.map(hh -> hh.percentile(99.9995))),
                            SimpleMapEntry.create("p999999", hne.map(hh -> hh.percentile(99.9999))));
                    return extra;
                })
                .orElseGet(Stream::empty)
                .map(entry -> SimpleMapEntry.create(entry.getKey(), entry.getValue().map(MetricValue::fromDblValue)))
                .map(entry -> entry.getValue().map(val -> SimpleMapEntry.create(entry.getKey(), val)))
                .flatMap(opt -> opt.map(Stream::of).orElseGet(Stream::empty));
    }

    /**
     * Convert a time series value into the string entries for wavefront.
     *
     * Note: the line is not terminated with a newline.
     */
    public static Stream<String> wavefrontLine(TimeSeriesValue tsv) {
        return tsv.getMetrics().entrySet().stream()
                .flatMap(metric_entry -> {
                    return Stream.concat(
                            expandHistogram(metric_entry.getValue())
                                    .map(v -> {
                                        final MetricName mname = NameCache.singleton.newMetricName(Stream.concat(
                                                        metric_entry.getKey().getPath().stream(),
                                                        Stream.of(v.getKey()))
                                                .collect(Collectors.toList()));
                                        return SimpleMapEntry.create(mname, v.getValue());
                                    }),
                            Stream.of(metric_entry));
                })
                .map(metric_entry -> wavefrontLine(tsv.getTimestamp(), tsv.getGroup(), metric_entry.getKey(), metric_entry.getValue()))
                .flatMap(opt -> opt.map(Stream::of).orElseGet(Stream::empty));
    }
}
