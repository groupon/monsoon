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
import com.groupon.lex.metrics.MetricName;
import com.groupon.lex.metrics.MetricValue;
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

    /**
     * Transform a tag entry into a wavefront tag.
     *
     * Double quotes in the tag value will be escaped.
     */
    private static Optional<Map.Entry<String, String>> createTagEntry(Map.Entry<String, MetricValue> tag_entry) {
        final Optional<String> opt_tag_value = tag_entry.getValue().asString();
        return opt_tag_value
                .map(tag_value -> SimpleMapEntry.create(name(tag_entry.getKey()), tag_value.replace("\"", "\\\"")));
    }

    /**
     * Truncate tag keys and values, to prevent them from exceeding the max length of a tag entry.
     */
    private static Map.Entry<String, String> maybeTruncateTagEntry(Map.Entry<String, String> tag_entry) {
        String k = tag_entry.getKey();
        String v = tag_entry.getValue();
        if (k.length() + v.length() <= MAX_TAG_KEY_VAL_CHARS - 2)  // 2 chars for the quotes around the value
            return tag_entry;

        if (k.length() > TRUNCATE_TAG_NAME)
            k = k.substring(0, TRUNCATE_TAG_NAME);
        if (k.length() + v.length() > MAX_TAG_KEY_VAL_CHARS - 2)
            v = v.substring(0, MAX_TAG_KEY_VAL_CHARS - 2 - k.length());
        return SimpleMapEntry.create(k, v);
    }

    /**
     * Create a map of tags for wavefront.
     *
     * The tag values are escaped and should be surrounded by double quotes.
     * This function does not put the surrounding quotes around the tag values.
     */
    public static Map<String, String> tags(Tags tags) {
        return tags.stream()
                .map(WavefrontStrings::createTagEntry)
                .flatMap(opt -> opt.map(Stream::of).orElseGet(Stream::empty))
                .map(WavefrontStrings::maybeTruncateTagEntry)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    /**
     * Create a wavefront compatible string representation of the metric value.
     *
     * If the metric value is empty or not representable in wavefront, an empty optional will be returned.
     */
    public static Optional<String> wavefrontValue(MetricValue mv) {
        // Omit NaN and Inf.
        if (mv.isInfiniteOrNaN()) return Optional.empty();
        return mv.value().map(Number::toString);
    }

    /** Convert a timestamp to the string representation that wavefront will accept. */
    public static String timestamp(DateTime ts) {
        return Long.toString(ts.getMillis() / 1000);
    }

    /**
     * Extract the 'source' tag from the tag_map.
     *
     * Wavefront requires the 'source' tag to be the first tag on the line, hence
     * the special handling.  It also *must* be present, so we can never return null.
     */
    private static String extractTagSource(Map<String, String> tag_map) {
        return Optional.ofNullable(tag_map.remove("source"))
                .orElseGet(() -> {
                    return Optional.ofNullable(tag_map.get("cluster"))
                            .orElseGet(() -> tag_map.getOrDefault("moncluster", "\"monsoon\""));
                });
    }

    /** Build the wavefront line from its parts. */
    private static String wavefrontLine(DateTime ts, SimpleGroupPath group, MetricName metric, String value, String source, Map<String, String> tag_map) {
        return new StringBuilder()
                .append(name(group, metric))
                .append(' ')
                .append(value)
                .append(' ')
                .append(timestamp(ts))
                .append(' ')
                .append("source=").append(source)
                .append(' ')
                .append(tag_map.entrySet().stream()
                        .map(entry -> entry.getKey() + "=\"" + entry.getValue() + '\"')
                        .collect(Collectors.joining(" "))
                )
                .toString();
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
                    final String source = extractTagSource(tag_map);  // Modifies tag_map.
                    return wavefrontLine(ts, group.getPath(), metric, value, source, tag_map);
                });
    }

    /**
     * Convert a time series value into the string entries for wavefront.
     *
     * Note: the line is not terminated with a newline.
     */
    public static Stream<String> wavefrontLine(TimeSeriesValue tsv) {
        return tsv.getMetrics().entrySet().stream()
                .map(metric_entry -> wavefrontLine(tsv.getTimestamp(), tsv.getGroup(), metric_entry.getKey(), metric_entry.getValue()))
                .flatMap(opt -> opt.map(Stream::of).orElseGet(Stream::empty));
    }
}
