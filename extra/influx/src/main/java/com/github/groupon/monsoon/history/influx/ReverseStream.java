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

import static com.github.groupon.monsoon.history.influx.InfluxUtil.extractTimestamps;
import static com.github.groupon.monsoon.history.influx.InfluxUtil.isSorted;
import static com.github.groupon.monsoon.history.influx.InfluxUtil.throwOnResultError;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollection;
import com.groupon.lex.metrics.timeseries.TimeSeriesMetricFilter;
import static java.util.Collections.emptyList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Spliterator;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.NonNull;
import org.influxdb.InfluxDB;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.joda.time.DateTime;
import org.joda.time.Duration;

/**
 * Reverse streaming of influx data.
 *
 * @author ariane
 */
public class ReverseStream implements Iterator<TimeSeriesCollection> {
    public static final int SPLITERATOR_CHARACTERISTICS = Spliterator.ORDERED | Spliterator.IMMUTABLE | Spliterator.DISTINCT | Spliterator.CONCURRENT;
    private static final Duration QUERY_INTERVAL = Duration.standardHours(1);
    private DateTime end;
    private final SelectStatement selector;
    private List<TimeSeriesCollection> pending = emptyList();

    public ReverseStream(@NonNull InfluxDB influxDb, @NonNull String database, @NonNull DateTime end) {
        this.end = end;
        this.selector = new SelectStatement(influxDb, database, TimeSeriesMetricFilter.ALL_GROUPS);
    }

    @Override
    public boolean hasNext() {
        update();
        return pending != null;
    }

    @Override
    public TimeSeriesCollection next() {
        if (!hasNext())
            throw new NoSuchElementException();
        return pending.remove(pending.size() - 1);
    }

    private void update() {
        while (pending != null && pending.isEmpty()) {
            final DateTime begin = end.minus(QUERY_INTERVAL);
            final List<TimeSeriesCollection> updateList = selector.execute(begin, end).collect(Collectors.toList());
            assert isSorted(updateList);
            end = begin;
            if (!updateList.isEmpty()) {
                pending = updateList;
                return;
            }

            /*
             * We don't have any data to emit.
             * Start a search for the next end-timestamp that will return data.
             */
            final Query searchEndQuery = new Query(String.format("select * from /.*/ where time <= '%s' order by time desc limit 1", end.toString()), selector.getDatabase());
            final QueryResult searchEndQueryResult = selector.getInfluxDB().query(searchEndQuery, TimeUnit.MILLISECONDS);
            throwOnResultError(searchEndQueryResult);
            final Optional<DateTime> optNewEnd = extractTimestamps(searchEndQueryResult).max(Comparator.naturalOrder());
            if (optNewEnd.isPresent())
                end = optNewEnd.get();
            else
                pending = null; // No more data.
        }
    }
}
