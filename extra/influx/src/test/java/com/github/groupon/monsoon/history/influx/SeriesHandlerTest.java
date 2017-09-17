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
import com.groupon.lex.metrics.timeseries.TimeSeriesCollection;
import java.util.List;
import java.util.stream.Collectors;
import org.hamcrest.Matchers;
import static org.junit.Assert.assertThat;
import org.junit.Test;

public class SeriesHandlerTest {
    @Test
    public void noSeries() throws Exception {
        SeriesHandler handler = new SeriesHandler();

        assertThat(handler.build().collect(Collectors.toList()), Matchers.empty());
    }

    @Test
    public void loadAllocFreesQueryResult() throws Exception {
        final QueryResultWithExpectation qrwe = new QueryResultWithExpectation("AllocFrees_internal_queryResult");

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
    public void collisionResolution() throws Exception {
        final QueryResultWithExpectation qrwe = new QueryResultWithExpectation("AllocFrees_internal_queryResult");

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
}
