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
import java.util.Iterator;
import java.util.Optional;
import java.util.stream.Stream;
import lombok.Getter;
import lombok.NonNull;
import org.influxdb.InfluxDB;
import org.influxdb.dto.QueryResult;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

@Getter
public class InfluxUtil {
    /**
     * Special tag name used to indicate monsoon range.
     */
    public static final String MONSOON_RANGE_TAG = "__monsoon_range__";
    /**
     * Name of the column holding the timestamp.
     */
    public static final String TIME_COLUMN = "time";

    private final InfluxDB influxDB;
    private final String database;

    protected InfluxUtil(@NonNull InfluxDB influxDB, @NonNull String database) {
        this.influxDB = influxDB;
        this.database = database;
    }

    protected static void throwOnResultError(QueryResult result) {
        if (result.hasError())
            throw new IllegalStateException("influx error: " + result);
    }

    protected static Stream<DateTime> extractTimestamps(QueryResult result) {
        return result.getResults().stream()
                .filter(resultEntry -> !resultEntry.hasError())
                .filter(resultEntry -> resultEntry.getSeries() != null)
                .flatMap(resultEntry -> resultEntry.getSeries().stream())
                .map(series -> getColumnFromSeries(series, TIME_COLUMN))
                .filter(Optional::isPresent)
                .flatMap(Optional::get)
                .map(Number.class::cast)
                .map(number -> new DateTime(number.longValue(), DateTimeZone.UTC));
    }

    protected static Optional<Integer> getColumnIndexFromSeries(QueryResult.Series series, String columnName) {
        return Optional.of(series.getColumns().indexOf(columnName))
                .filter(idx -> idx >= 0);
    }

    protected static Optional<Stream<Object>> getColumnFromSeries(QueryResult.Series series, String columnName) {
        return getColumnIndexFromSeries(series, columnName)
                .map(idx -> {
                    return series.getValues().stream()
                            .map(row -> row.get(idx));
                });
    }

    /**
     * Validation function, to check if an iterable is sorted with ascending
     * timestamps.
     *
     * @param tscIterable An iterable type.
     * @return True iff the iterable is sorted, false otherwise.
     */
    static boolean isSorted(Iterable<? extends TimeSeriesCollection> tscIterable) {
        final Iterator<? extends TimeSeriesCollection> iter = tscIterable.iterator();
        if (!iter.hasNext()) return true; // Empty collection is ordered.

        DateTime timestamp = iter.next().getTimestamp();
        while (iter.hasNext()) {
            final DateTime nextTimestamp = iter.next().getTimestamp();
            if (!nextTimestamp.isAfter(timestamp)) return false;
            timestamp = nextTimestamp;
        }
        return true;
    }
}
