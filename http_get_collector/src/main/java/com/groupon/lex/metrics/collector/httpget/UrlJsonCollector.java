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
package com.groupon.lex.metrics.collector.httpget;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import com.groupon.lex.metrics.Metric;
import com.groupon.lex.metrics.MetricValue;
import com.groupon.lex.metrics.NameCache;
import com.groupon.lex.metrics.SimpleGroupPath;
import com.groupon.lex.metrics.SimpleMetric;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import static java.util.Collections.singletonList;
import java.util.List;
import java.util.Optional;
import static java.util.function.Function.identity;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;
import org.apache.http.Header;

/**
 *
 * @author ariane
 */
public class UrlJsonCollector extends UrlGetCollector {
    private static final Logger LOG = Logger.getLogger(UrlJsonCollector.class.getName());
    private final JsonParser parser_ = new JsonParser();
    private final List<String> ROOT = singletonList("body");

    public UrlJsonCollector(SimpleGroupPath base_group_name, UrlPattern patterns) {
        super(base_group_name, patterns);
    }

    @Override
    protected Stream<Metric> processStream(Header[] response_headers, String contentType, Optional<Charset> charset, InputStream in) throws IOException {
        try {
            return decode_(ROOT, parser_.parse(new InputStreamReader(in, charset.orElseGet(() -> Charset.forName("UTF-8")))));
        } catch (JsonParseException ex) {
            LOG.log(Level.WARNING, "unable to parse as json", ex);
            return Stream.empty();
        }
    }

    private static Stream<Metric> decode_(List<String> prefix, JsonElement elem) {
        if (elem.isJsonNull())
            return Stream.of(new SimpleMetric(NameCache.singleton.newMetricName(prefix), MetricValue.EMPTY));

        if (elem.isJsonArray()) {
            return decode_array_(prefix, elem.getAsJsonArray());
        }

        if (elem.isJsonObject()) {
            return decode_object_(prefix, elem.getAsJsonObject());
        }

        return Stream.of(new SimpleMetric(NameCache.singleton.newMetricName(prefix), decode_primitive_(elem)));
    }

    private static Stream<Metric> decode_array_(List<String> prefix, JsonArray elem) {
        final int size = elem.size();
        final Collection<Stream<Metric>> result = new ArrayList<>(size);

        for (int i = 0; i < size; ++i) {
            final List<String> sub_prefix = new ArrayList<>(prefix.size() + 1);
            sub_prefix.addAll(prefix);
            sub_prefix.add(String.valueOf(i));

            result.add(decode_(sub_prefix, elem.get(i)));
        }
        return result.stream().flatMap(identity());
    }

    private static Stream<Metric> decode_object_(List<String> prefix, JsonObject elem) {
        return elem.entrySet().stream()
                .flatMap((e) -> {
                    final List<String> sub_prefix = new ArrayList<>(prefix.size() + 1);
                    sub_prefix.addAll(prefix);
                    sub_prefix.add(e.getKey());

                    return decode_(sub_prefix, e.getValue());
                });
    }

    private static MetricValue decode_primitive_(JsonElement base_elem) {
        final JsonPrimitive elem = base_elem.getAsJsonPrimitive();

        if (elem.isBoolean())
            return MetricValue.fromBoolean(elem.getAsBoolean());

        if (elem.isNumber()) {
            String num = elem.getAsNumber().toString();
            try {
                return MetricValue.fromIntValue(Long.parseLong(num));
            } catch (NumberFormatException ex) {
                /* SKIP */
            }
            try {
                return MetricValue.fromDblValue(Double.parseDouble(num));
            } catch (NumberFormatException ex ) {
                /* SKIP */
            }
            return MetricValue.fromStrValue(num);
        }

        if (elem.isString())
            return MetricValue.fromStrValue(elem.getAsString());

        // Should be unreachable.
        LOG.log(Level.WARNING, "failed to correctly decode primitive JSON value {0}", base_elem);
        return MetricValue.EMPTY;
    }
}
