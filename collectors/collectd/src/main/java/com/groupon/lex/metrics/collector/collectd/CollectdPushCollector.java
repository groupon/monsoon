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
package com.groupon.lex.metrics.collector.collectd;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.groupon.lex.metrics.GroupName;
import com.groupon.lex.metrics.Metric;
import com.groupon.lex.metrics.MetricGroup;
import com.groupon.lex.metrics.MetricName;
import com.groupon.lex.metrics.MetricValue;
import com.groupon.lex.metrics.SimpleGroupPath;
import com.groupon.lex.metrics.SimpleMetric;
import com.groupon.lex.metrics.SimpleMetricGroup;
import com.groupon.lex.metrics.SynchronousGroupGenerator;
import com.groupon.lex.metrics.Tags;
import com.groupon.lex.metrics.collector.collectd.grammar.CollectdTags;
import com.groupon.lex.metrics.lib.Any2;
import com.groupon.lex.metrics.lib.SimpleMapEntry;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import static java.util.Collections.EMPTY_MAP;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static java.util.Collections.unmodifiableMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import static javax.servlet.http.HttpServletResponse.SC_OK;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;

/**
 *
 * @author ariane
 */
public class CollectdPushCollector extends SynchronousGroupGenerator {
    public static final String API_ENDPOINT_BASE = "/collectd/jsonpush/";
    public static final Duration DROP_DURATION = Duration.standardHours(1);
    private static final Type COLLECTD_TYPE = new TypeToken<List<CollectdMessage>>() {
    }.getType();
    private static final Metric UP_METRIC = new SimpleMetric(MetricName.valueOf("up"), Boolean.TRUE);
    private static final Metric DOWN_METRIC = new SimpleMetric(MetricName.valueOf("up"), Boolean.FALSE);

    private Map<String, DateTime> knownHosts = EMPTY_MAP;  // Replaced on each collection.
    private final ReadWriteLock messagesGuard = new ReentrantReadWriteLock();  // Protects messages.
    private final Map<CollectdKey, CollectdMessage> messages = new ConcurrentHashMap<>();
    @Getter
    private final SimpleGroupPath basePath;

    @EqualsAndHashCode
    public static class CollectdKey {
        public final String host, plugin, plugin_instance, type, type_instance;
        public final Map<String, Any2<String, Number>> tags;

        public CollectdKey(String host, String plugin, String plugin_instance, String type, String type_instance) {
            final Map<String, Any2<String, Number>> tags = new HashMap<>();
            this.host = host;
            this.plugin = remove_tags_(plugin, tags);
            this.plugin_instance = remove_tags_(plugin_instance, tags);
            this.type = type;
            this.type_instance = type_instance;
            this.tags = unmodifiableMap(tags);
        }

        private static String remove_tags_(String s, Map<String, Any2<String, Number>> out_tagmap) {
            final int tags_brace_open = s.indexOf('[');
            if (tags_brace_open == -1 || !s.endsWith("]"))
                return s;

            final String tag_string = s.substring(tags_brace_open + 1, s.length() - 1);
            if (!tag_string.isEmpty())
                out_tagmap.putAll(CollectdTags.parse(tag_string));

            return s.substring(0, tags_brace_open);
        }
    }

    public static class CollectdMessage {
        public List<Number> values;
        public List<String> dstypes;
        public List<String> dsnames;
        public double time;
        public long interval;
        public String host;
        public String plugin;
        public String plugin_instance;
        public String type;
        public String type_instance;

        public CollectdKey getKey() {
            return new CollectdKey(host, plugin, plugin_instance, type, type_instance);
        }

        public int metricCount() {
            return values.size();
        }

        /**
         * Work around for Google gson parser emitting numbers as 'lazily
         * parsed' numbers.
         *
         * @param elem An instance of a gson lazily parsed number.
         * @return A metric value containing the number.
         */
        private static MetricValue number_to_metric_value_(Number elem) {
            if (elem == null) return MetricValue.EMPTY;

            final String num = elem.toString();
            try {
                return MetricValue.fromIntValue(Long.parseLong(num));
            } catch (NumberFormatException ex) {
                /* SKIP */
            }
            try {
                return MetricValue.fromDblValue(Double.parseDouble(num));
            } catch (NumberFormatException ex) {
                /* SKIP */
            }
            return MetricValue.fromStrValue(num);
        }

        private MetricName to_metric_name_(String name) {
            if ("value".equals(name) && metricCount() == 1)
                name = null;

            final List<String> path = Stream.of(type, type_instance, name)
                    .filter(x -> x != null)
                    .filter(s -> !s.isEmpty())
                    .collect(Collectors.toList());
            return MetricName.valueOf(path.isEmpty() ? singletonList("value") : path);
        }

        public MetricGroup toMetricGroup(SimpleGroupPath base_path) {
            final CollectdKey key = getKey();  // Use key, as it parses the tags for us.
            final Map<String, MetricValue> tag_map = new HashMap<String, MetricValue>() {
                {
                    put("host", MetricValue.fromStrValue(host));
                    putAll(key.tags.entrySet().stream()
                            .collect(Collectors.toMap(
                                    Map.Entry::getKey,
                                    (entry -> entry.getValue().mapCombine(MetricValue::fromStrValue, MetricValue::fromNumberValue)))));
                }
            };

            final GroupName group = GroupName.valueOf(
                    SimpleGroupPath.valueOf(Stream.concat(
                            base_path.getPath().stream(),
                            Stream.of(key.plugin, key.plugin_instance)
                            .filter(x -> x != null)
                            .filter(s -> !s.isEmpty()))
                            .collect(Collectors.toList())),
                    Tags.valueOf(tag_map));

            final List<SimpleMetric> entries = new ArrayList<>(metricCount());
            for (int i = 0; i < metricCount(); ++i)
                entries.add(new SimpleMetric(to_metric_name_(dsnames.get(i)), number_to_metric_value_(values.get(i))));

            return new SimpleMetricGroup(group, entries);
        }
    }

    private class Endpoint extends HttpServlet {
        private final Gson gson = new Gson();

        @Override
        protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
            final List<CollectdMessage> msgs = gson.fromJson(req.getReader(), COLLECTD_TYPE);

            final Lock lck = messagesGuard.readLock();
            lck.lock();
            try {
                msgs.forEach(msg -> messages.put(msg.getKey(), msg));
            } finally {
                lck.unlock();
            }

            resp.setStatus(SC_OK);
            resp.getWriter().write("Monsoon accepted " + msgs.size() + " metric groups\n");
        }
    }

    public CollectdPushCollector(@NonNull BiConsumer<String, HttpServlet> er, @NonNull SimpleGroupPath base_path, @NonNull String name) {
        if (name.isEmpty())
            throw new IllegalArgumentException("empty endpoint name");

        er.accept(API_ENDPOINT_BASE + name, new Endpoint());
        basePath = base_path;
    }

    @Override
    public Collection<? extends MetricGroup> getGroups(CompletableFuture<TimeoutObject> timeout) {
        final DateTime now = new DateTime(DateTimeZone.UTC);
        final DateTime drop = now.minus(DROP_DURATION);

        Lock lck = messagesGuard.writeLock();
        lck.lock();
        try {
            // Transform all messages into metrics.
            final Stream<MetricGroup> msg_stream = messages.values().stream()
                    .map(cm -> cm.toMetricGroup(basePath))
                    .flatMap(mg -> Arrays.stream(mg.getMetrics()).map(m -> SimpleMapEntry.create(mg.getName(), m)))
                    .collect(Collectors.groupingBy(Map.Entry::getKey, Collectors.mapping(Map.Entry::getValue, Collectors.toList())))
                    .entrySet()
                    .stream()
                    .map((Map.Entry<GroupName, List<Metric>> entry) -> new SimpleMetricGroup(entry.getKey(), entry.getValue()));
            // Collect the set of hosts that are 'up'.
            final Set<String> up_hosts = messages.values().stream()
                    .map(msg -> msg.host)
                    .distinct()
                    .collect(Collectors.toSet());
            // Collect the set of hosts that are 'down'.
            final Map<String, DateTime> down_hosts = knownHosts.entrySet().stream()
                    .filter(entry -> entry.getValue().isAfter(drop)) // Don't remember host names for ever.
                    .filter(entry -> !up_hosts.contains(entry.getKey())) // No need to remember hosts that emitted metrics.
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            // Create metrics for all up/down hosts.
            final Stream<MetricGroup> up_hosts_stream = up_hosts.stream()
                    .map(host -> up_down_host_(host, true));
            final Stream<MetricGroup> down_hosts_stream = down_hosts.keySet().stream()
                    .map(host -> up_down_host_(host, false));
            // Replace the map of known hosts.
            knownHosts = Stream.concat(down_hosts.entrySet().stream(), up_hosts.stream().map(host -> SimpleMapEntry.create(host, now)))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            // Done, return result.
            return Stream.of(msg_stream, up_hosts_stream, down_hosts_stream)
                    .flatMap(Function.identity())
                    .collect(Collectors.toList());
        } finally {
            messages.clear();
            lck.unlock();
        }
    }

    /**
     * Return a metric group indicating if the host is up or down.
     */
    private MetricGroup up_down_host_(String host, boolean up) {
        final GroupName group = GroupName.valueOf(
                getBasePath(),
                Tags.valueOf(singletonMap("host", MetricValue.fromStrValue(host))));
        return new SimpleMetricGroup(group, singleton(up ? UP_METRIC : DOWN_METRIC));
    }
}
