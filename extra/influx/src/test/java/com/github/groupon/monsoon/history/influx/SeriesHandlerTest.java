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

import static com.github.groupon.monsoon.history.influx.JsonUtil.createOrderingExceptation;
import com.groupon.lex.metrics.GroupName;
import com.groupon.lex.metrics.Histogram;
import com.groupon.lex.metrics.MetricName;
import com.groupon.lex.metrics.MetricValue;
import com.groupon.lex.metrics.SimpleGroupPath;
import com.groupon.lex.metrics.Tags;
import com.groupon.lex.metrics.timeseries.ImmutableTimeSeriesValue;
import com.groupon.lex.metrics.timeseries.SimpleTimeSeriesCollection;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollection;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;
import java.util.List;
import java.util.stream.Collectors;
import org.hamcrest.Matchers;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import static org.junit.Assert.assertThat;
import org.junit.Test;

public class SeriesHandlerTest {
    private static final GroupName GROUP = GroupName.valueOf(SimpleGroupPath.valueOf("run", "time"), Tags.valueOf(singletonMap("hostname", MetricValue.fromStrValue("dragoon"))));

    private static final Histogram EXPECTED_HISTOGRAM = new Histogram(
            new Histogram.RangeWithCount(0, 1, 0.5),
            new Histogram.RangeWithCount(1, 2, 2),
            new Histogram.RangeWithCount(2, 3, 6),
            new Histogram.RangeWithCount(3, 4, 12));

    @Test
    public void noSeries() throws Exception {
        SeriesHandler handler = new SeriesHandler();

        assertThat(handler.build().collect(Collectors.toList()), Matchers.empty());
    }

    @Test
    public void loadAllocFreesQueryResult() throws Exception {
        final QueryResultWithExpectation qrwe = new QueryResultWithExpectation("AllocFrees_internal_queryResult", GROUP);

        SeriesHandler handler = new SeriesHandler();
        qrwe.getQueryResult()
                .getResults()
                .stream()
                .flatMap(result -> result.getSeries().stream())
                .forEach(handler::addSeries);
        final List<TimeSeriesCollection> handlerResult = handler.build().collect(Collectors.toList());

        assertThat(handlerResult, createOrderingExceptation(handlerResult));
        assertThat(handlerResult, qrwe.getExpectation());
    }

    @Test
    public void seriesHandlerMergesProperly() throws Exception {
        final QueryResultWithExpectation qrwe = new QueryResultWithExpectation("AllocFrees_internal_queryResult", GROUP);

        SeriesHandler handler = new SeriesHandler();
        qrwe.getQueryResult()
                .getResults()
                .stream()
                .map(result -> {
                    SeriesHandler tmp = new SeriesHandler();
                    result.getSeries().forEach(tmp::addSeries);
                    return tmp;
                })
                .forEach(handler::merge);
        final List<TimeSeriesCollection> handlerResult = handler.build().collect(Collectors.toList());

        assertThat(handlerResult, createOrderingExceptation(handlerResult));
        assertThat(handlerResult, qrwe.getExpectation());
    }

    @Test
    public void collisionResolution() throws Exception {
        final QueryResultWithExpectation qrwe = new QueryResultWithExpectation("AllocFrees_internal_queryResult", GROUP);

        SeriesHandler handler = new SeriesHandler();
        qrwe.getQueryResult()
                .getResults()
                .stream()
                .flatMap(result -> result.getSeries().stream())
                .forEach(handler::addSeries);
        qrwe.getQueryResult()
                .getResults()
                .stream()
                .flatMap(result -> result.getSeries().stream())
                .forEach(handler::addSeries);
        final List<TimeSeriesCollection> handlerResult = handler.build().collect(Collectors.toList());

        assertThat(handlerResult, createOrderingExceptation(handlerResult));
        assertThat(handlerResult, qrwe.getExpectation());
    }

    @Test
    public void histogram() throws Exception {
        final JsonQueryResult jqr = new JsonQueryResult("Histogram_queryResult");

        SeriesHandler handler = new SeriesHandler();
        jqr.getQueryResult()
                .getResults()
                .stream()
                .flatMap(result -> result.getSeries().stream())
                .forEach(handler::addSeries);
        final List<TimeSeriesCollection> handlerResult = handler.build().collect(Collectors.toList());

        assertThat(handlerResult,
                Matchers.contains(
                        new SimpleTimeSeriesCollection(
                                new DateTime(0, DateTimeZone.UTC),
                                singleton(
                                        new ImmutableTimeSeriesValue(
                                                GROUP,
                                                singletonMap(
                                                        MetricName.valueOf("foobar"),
                                                        MetricValue.fromHistValue(EXPECTED_HISTOGRAM)))))));
    }

    @Test(expected = IllegalStateException.class)
    public void missingTime() throws Exception {
        final JsonQueryResult jqr = new JsonQueryResult("MissingTime");

        SeriesHandler handler = new SeriesHandler();
        jqr.getQueryResult()
                .getResults()
                .stream()
                .flatMap(result -> result.getSeries().stream())
                .forEach(handler::addSeries);
        final List<TimeSeriesCollection> handlerResult = handler.build().collect(Collectors.toList());
    }

    @Test
    public void trueFalseOrStringValue() throws Exception {
        final JsonQueryResult jqr = new JsonQueryResult("TrueFalse");

        SeriesHandler handler = new SeriesHandler();
        jqr.getQueryResult()
                .getResults()
                .stream()
                .flatMap(result -> result.getSeries().stream())
                .forEach(handler::addSeries);
        final List<TimeSeriesCollection> handlerResult = handler.build().collect(Collectors.toList());

        assertThat(handlerResult,
                Matchers.contains(Matchers.allOf(
                        Matchers.hasProperty("timestamp", Matchers.equalTo(new DateTime(0, DateTimeZone.UTC))),
                        Matchers.hasProperty("TSValues", Matchers.containsInAnyOrder(
                                new ImmutableTimeSeriesValue(GroupName.valueOf("string"), singletonMap(MetricName.valueOf("foobar"), MetricValue.fromStrValue("a string"))),
                                new ImmutableTimeSeriesValue(GroupName.valueOf("true"), singletonMap(MetricName.valueOf("foobar"), MetricValue.TRUE)),
                                new ImmutableTimeSeriesValue(GroupName.valueOf("false"), singletonMap(MetricName.valueOf("foobar"), MetricValue.FALSE))
                        ))
                )));
    }
}
