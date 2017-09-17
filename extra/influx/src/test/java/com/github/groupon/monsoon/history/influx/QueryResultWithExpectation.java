/*
 * Copyright (c) 2017, ariane
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * * Redistributions of source code must retain the above copyright notice, this
 *   list of conditions and the following disclaimer.
 * * Redistributions in binary form must reproduce the above copyright notice,
 *   this list of conditions and the following disclaimer in the documentation
 *   and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
package com.github.groupon.monsoon.history.influx;

import com.groupon.lex.metrics.GroupName;
import com.groupon.lex.metrics.MetricName;
import com.groupon.lex.metrics.MetricValue;
import com.groupon.lex.metrics.timeseries.ImmutableTimeSeriesValue;
import com.groupon.lex.metrics.timeseries.SimpleTimeSeriesCollection;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollection;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Data;
import lombok.Getter;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.influxdb.dto.QueryResult;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

@Getter
public class QueryResultWithExpectation {
    private final Matcher<Iterable<? extends TimeSeriesCollection>> expectation;
    private final QueryResult queryResult;

    public QueryResultWithExpectation(String baseFileName, GroupName group) throws IOException {
        this.queryResult = JsonUtil.loadJson(baseFileName + ".json", QueryResult.class);
        if (this.queryResult.getResults() != null)
            Collections.shuffle(this.queryResult.getResults());
        this.expectation = JsonUtil.loadJson(baseFileName + "__expect.json", Expected.class).build(group);
    }

    @Data
    private static class Expected {
        private List<String> columns;
        private List<List<Object>> values;

        public Matcher<Iterable<? extends TimeSeriesCollection>> build(GroupName group) {
            final int timeColumn = columns.indexOf("time");
            final List<TimeSeriesCollection> result = new ArrayList<>(values.size());

            for (List<Object> row : values) {
                ListIterator<String> columnIter = columns.listIterator();
                Iterator<Object> fieldIter = row.iterator();

                final long timestamp = ((Number) row.get(timeColumn)).longValue();
                Map<MetricName, MetricValue> valueMap = new HashMap<>();
                while (columnIter.hasNext()) {
                    int columnIdx = columnIter.nextIndex();
                    String column = columnIter.next();
                    Number field = (Number) fieldIter.next();
                    if (columnIdx == timeColumn) continue;
                    valueMap.put(MetricName.valueOf(column.split(Pattern.quote("."))), MetricValue.fromNumberValue(field));
                }

                result.add(new SimpleTimeSeriesCollection(new DateTime(timestamp, DateTimeZone.UTC), Stream.of(new ImmutableTimeSeriesValue(group, valueMap))));
            }
            return Matchers.containsInAnyOrder(result.stream()
                    .map(Matchers::equalTo)
                    .collect(Collectors.toList()));
        }
    }
}
