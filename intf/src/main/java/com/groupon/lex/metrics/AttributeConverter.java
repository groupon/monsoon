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

import com.groupon.lex.metrics.lib.SimpleMapEntry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;
import javax.management.openmbean.CompositeData;

/**
 *
 * @author ariane
 */
public class AttributeConverter {
    private static final Logger LOG = Logger.getLogger(AttributeConverter.class.getName());

    public static Stream<Map.Entry<MetricName, MetricValue>> resolve_property(List<String> attrPath, Object attrObj) {
        /*
         * Step 1:
         * Handle null values.
         */
        if (attrObj == null) {
            LOG.log(Level.WARNING, "expected a {0} or {1}, but got null", new Object[]{Number.class.getName(), String.class.getName()});
            return Stream.empty();
        }

        /*
         * Step 2a:
         * try to convert the property to a Boolean.
         */
        if (attrObj instanceof Boolean)
            return Stream.of(SimpleMapEntry.create(MetricName.valueOf(attrPath), MetricValue.fromBoolean((Boolean)attrObj)));

        /*
         * Step 2b:
         * try to convert the property to a Number.
         */
        if (attrObj instanceof Number)
            return Stream.of(SimpleMapEntry.create(MetricName.valueOf(attrPath), MetricValue.fromNumberValue((Number)attrObj)));

        /*
         * Step 2c:
         * try to convert the property to a string value.
         */
        if (attrObj instanceof String)
            return Stream.of(SimpleMapEntry.create(MetricName.valueOf(attrPath), MetricValue.fromStrValue((String)attrObj)));

        /*
         * Step 3a:
         * try to convert the property to a map value.
         */
        if (attrObj instanceof Map)
            return resolve_map_property_(attrPath, (Map)attrObj);

        /*
         * Step 3b:
         * try to convert the property to a list value.
         */
        if (attrObj instanceof List)
            return resolve_list_property_(attrPath, (List)attrObj);

        /*
         * Step 3c:
         * try to handle a collection type, by failing it.
         */
        if (attrObj instanceof Collection) {
            LOG.log(Level.WARNING, "expected a {0} or {1}, but got a Collection: {2}", new Object[]{Number.class.getName(), String.class.getName(), attrObj.getClass().getName()});
            return Stream.empty();
        }

        /*
         * Step 4:
         * try to handle a class type.
         */
        if (attrObj instanceof CompositeData)
            return resolve_composite_property_(attrPath, (CompositeData)attrObj);

        /*
         * Step 5:
         * return an empty Optional if the value is null.
         */
        LOG.log(Level.WARNING, "{1}: measured unparsable value {1}", new Object[]{attrPath, attrObj});
        return Stream.empty();
    }

    private static Stream<Map.Entry<MetricName, MetricValue>> resolve_map_property_(List<String> attrPath, Map<?, ?> attrObj) {
        return attrObj.entrySet().stream()
                .filter(e -> e.getKey() != null)
                .filter(e -> e.getKey() instanceof String)
                .flatMap(e -> {
                    final ArrayList<String> subPath = new ArrayList<>(attrPath);
                    subPath.add((String)e.getKey());
                    return resolve_property(subPath, e.getValue());
                });
    }

    private static Stream<Map.Entry<MetricName, MetricValue>> resolve_list_property_(List<String> attrPath, List<?> attrObj) {
        final List<Stream<Map.Entry<MetricName, MetricValue>>> result = new ArrayList<>(attrObj.size());

        final ListIterator<?> i = attrObj.listIterator();
        while (i.hasNext()) {
            final ArrayList<String> subPath = new ArrayList<>(attrPath);
            subPath.add(String.valueOf(i.nextIndex()));
            final Object v = i.next();

            result.add(resolve_property(subPath, v));
        }

        return result.stream()
                .flatMap(Function.identity());
    }

    private static Stream<Map.Entry<MetricName, MetricValue>> resolve_composite_property_(List<String> attrPath, CompositeData attrObj) {
        return attrObj.getCompositeType().keySet().stream()
                .map(key -> SimpleMapEntry.create(key, attrObj.get(key)))
                .map(e -> {
                    ArrayList<String> subPath = new ArrayList<>(attrPath);
                    subPath.add(e.getKey());
                    return SimpleMapEntry.create(subPath, e.getValue());
                })
                .flatMap(e -> resolve_property(e.getKey(), e.getValue()));
    }

}
