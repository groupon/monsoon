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

import com.groupon.lex.metrics.timeseries.TimeSeriesCollection;
import com.groupon.lex.metrics.timeseries.TimeSeriesMetricFilter;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;
import lombok.NonNull;
import org.influxdb.InfluxDB;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.joda.time.DateTime;

/**
 *
 * @author ariane
 */
public class SelectStatement extends InfluxUtil {
    private static final Logger LOG = Logger.getLogger(SelectStatement.class.getName());

    private final SelectHandler selectHandler;

    public SelectStatement(@NonNull InfluxDB influxDB, @NonNull String database, @NonNull TimeSeriesMetricFilter filter) {
        super(influxDB, database);
        this.selectHandler = new SelectHandler(filter);
    }

    public Stream<TimeSeriesCollection> execute(DateTime begin, DateTime end) {
        return selectHandler.queriesForInterval(begin, end)
                .peek(query -> LOG.log(Level.INFO, "{0}", query))
                .map(queryStr -> new Query(queryStr, getDatabase()))
                .map(query -> getInfluxDB().query(query, TimeUnit.MILLISECONDS))
                .peek(InfluxUtil::throwOnResultError)
                .map(QueryResult::getResults)
                .flatMap(Collection::stream)
                .filter(result -> !result.hasError())
                .filter(r -> r.getSeries() != null)
                .flatMap(r -> r.getSeries().stream())
                .collect(SeriesHandler::new, SeriesHandler::addSeries, SeriesHandler::merge)
                .build();
    }
}
