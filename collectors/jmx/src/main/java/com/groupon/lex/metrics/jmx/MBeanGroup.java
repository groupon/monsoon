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
package com.groupon.lex.metrics.jmx;

import static com.groupon.lex.metrics.AttributeConverter.resolve_property;
import com.groupon.lex.metrics.GroupName;
import com.groupon.lex.metrics.MetricGroup;
import com.groupon.lex.metrics.MetricName;
import com.groupon.lex.metrics.MetricValue;
import com.groupon.lex.metrics.SimpleMetric;
import com.groupon.lex.metrics.SimpleMetricGroup;
import com.groupon.lex.metrics.resolver.NamedResolverMap;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.IntrospectionException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanException;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import javax.management.RuntimeErrorException;
import javax.management.RuntimeMBeanException;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;

/**
 *
 * @author ariane
 */
public class MBeanGroup {
    private static final Logger LOG = Logger.getLogger(MBeanGroup.class.getName());
    private static final Duration SILENCE_EXCEPTION_LOG = Duration.standardHours(4);
    private final Map<String, DateTime> last_exception_log_ = new HashMap<>();

    private static class Tag {
        private final String name_;
        private final MetricValue value_;

        public Tag(String name, MetricValue value) {
            name_ = name;
            value_ = value;
        }

        public String getName() {
            return name_;
        }

        public MetricValue getValue() {
            return value_;
        }

        public static Tag valueOf(Entry<String, String> entry) {
            String key = entry.getKey();
            String value = entry.getValue();

            if ("true".equals(value))
                return new Tag(key, MetricValue.fromBoolean(true));
            if ("false".equals(value))
                return new Tag(key, MetricValue.fromBoolean(false));

            try {
                final long int_value = Long.parseLong(value);
                return new Tag(key, MetricValue.fromIntValue(int_value));
            } catch (NumberFormatException ex) {
                /* SKIP */
            }

            try {
                final double dbl_value = Double.parseDouble(value);
                return new Tag(key, MetricValue.fromDblValue(dbl_value));
            } catch (NumberFormatException ex) {
                /* SKIP */
            }

            return new Tag(key, MetricValue.fromStrValue(value));
        }
    }

    private final GroupName name_;
    private final ObjectName obj_name_;

    /**
     * Extract a metric group name from a JMX ObjectName.
     *
     * @param obj_name a JMX object name from which to derive a metric name.
     * @param resolvedMap a resolver map to use when generating the group name.
     * @return A metric name for the given ObjectName, with tags.
     */
    private static GroupName nameFromObjectName(ObjectName obj_name, NamedResolverMap resolvedMap) {
        String name = obj_name.getKeyProperty("name");
        String type = obj_name.getKeyProperty("type");
        String domain = obj_name.getDomain();

        Map<String, MetricValue> tags = obj_name.getKeyPropertyList().entrySet().stream()
                .filter((entry) -> !entry.getKey().equals("name"))
                .filter((entry) -> !entry.getKey().equals("type"))
                .map(Tag::valueOf)
                .collect(Collectors.toMap((Tag t) -> t.getName(), (Tag t) -> t.getValue()));

        final List<String> path = new ArrayList<>();
        if (name != null) {
            path.addAll(Arrays.asList(name.split("\\.")));
        } else if (type != null) {
            path.addAll(Arrays.asList(domain.split("\\.")));
            path.add(type);
        } else {
            path.addAll(Arrays.asList(domain.split("\\.")));
        }
        return resolvedMap.getGroupName(path, tags);
    }

    public MBeanGroup(GroupName name, ObjectName obj_name) {
        name_ = name;
        obj_name_ = obj_name;

        if (obj_name_.isPattern())
            throw new IllegalArgumentException("ObjectName may not be a pattern");
    }

    public MBeanGroup(ObjectName obj_name, NamedResolverMap resolvedMap) {
        this(nameFromObjectName(obj_name, resolvedMap), obj_name);
    }

    private Stream<Map.Entry<MetricName, MetricValue>> resolve_(MBeanServerConnection conn, String attribute) {
        final DateTime last_exception_log = last_exception_log_.computeIfAbsent(attribute, (ignored) -> new DateTime(0L, DateTimeZone.UTC));

        /*
         * Step 1:
         * try to load the property from the mbean and store the object.
         */
        Object property;
        try {
            property = conn.getAttribute(obj_name_, attribute);
        } catch (MBeanException | IOException | RuntimeMBeanException | RuntimeErrorException ex) {
            /* Don't spam continuously this error, just emit it every once in a while. */
            final DateTime now_ts = DateTime.now(DateTimeZone.UTC);
            if (new Duration(last_exception_log, now_ts).isLongerThan(SILENCE_EXCEPTION_LOG)) {
                LOG.log(Level.WARNING, "exception while reading property, skipping", ex.getCause());
                last_exception_log_.put(attribute, now_ts);
            }
            return Stream.empty();
        } catch (AttributeNotFoundException ex) {
            LOG.log(Level.WARNING, "MBean present, but property not found, skipping");
            return Stream.empty();
        } catch (InstanceNotFoundException ex) {
            LOG.log(Level.WARNING, "MBean not present, skipping");
            return Stream.empty();
        } catch (ReflectionException ex) {
            LOG.log(Level.WARNING, "exception invoking the setter (huh?)", ex);
            return Stream.empty();
        }

        return resolve_property(Arrays.asList(attribute), property);
    }

    public Optional<MetricGroup> getMetrics(MBeanServerConnection conn) {
        MBeanAttributeInfo[] attributes;
        try {
            attributes = conn.getMBeanInfo(obj_name_).getAttributes();
        } catch (InstanceNotFoundException ex) {
            return Optional.empty();
        } catch (IntrospectionException | ReflectionException | IOException ex) {
            LOG.log(Level.WARNING, "failed to load properties on " + obj_name_, ex);
            return Optional.empty();
        }

        return Optional.of(new SimpleMetricGroup(
                getName(),
                Arrays.stream(attributes)
                .map(MBeanAttributeInfo::getName)
                .distinct()
                .flatMap(attribute -> resolve_(conn, attribute))
                .map(m -> new SimpleMetric(m.getKey(), m.getValue()))));
    }

    public GroupName getName() {
        return name_;
    }

    public ObjectName getMonitoredMBeanName() {
        return obj_name_;
    }
}
