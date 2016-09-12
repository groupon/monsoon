/*
 * Copyright (c) 2016, Ariane van der Steldt
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
package com.groupon.monsoon.remote.history;

import com.groupon.lex.metrics.history.CollectHistory;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollection;
import com.groupon.lex.metrics.timeseries.TimeSeriesMetricExpression;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;
import lombok.NonNull;
import org.acplt.oncrpc.OncRpcException;
import org.joda.time.DateTime;
import org.joda.time.Duration;

public class CollectHistoryServer extends AbstractServer {
    private static final Logger LOG = Logger.getLogger(CollectHistoryServer.class.getName());
    private final CollectHistory history;

    public CollectHistoryServer(@NonNull CollectHistory history) throws OncRpcException, IOException {
        super();
        this.history = history;
    }

    public CollectHistoryServer(@NonNull CollectHistory history, int port) throws OncRpcException, IOException {
        super(port);
        this.history = history;
    }

    public CollectHistoryServer(@NonNull CollectHistory history, InetAddress bindAddr, int port) throws OncRpcException, IOException {
        super(bindAddr, port);
        this.history = history;
    }

    @Override
    public boolean addTSData(List<TimeSeriesCollection> c) {
        LOG.log(Level.FINE, "adding {0} collections", c.size());
        return history.addAll(c);
    }

    @Override
    public long getFileSize() {
        LOG.log(Level.FINE, "request received");
        return history.getFileSize();
    }

    @Override
    public DateTime getEnd() {
        LOG.log(Level.FINE, "request received");
        return history.getEnd();
    }

    @Override
    public Stream<TimeSeriesCollection> streamReverse() {
        LOG.log(Level.FINE, "request received");
        return history.streamReversed();
    }

    @Override
    public Stream<TimeSeriesCollection> stream() {
        LOG.log(Level.FINE, "request received()");
        return history.stream();
    }

    @Override
    public Stream<TimeSeriesCollection> stream(DateTime begin) {
        LOG.log(Level.FINE, "request received({0})", begin);
        return history.stream(begin);
    }

    @Override
    public Stream<TimeSeriesCollection> stream(DateTime begin, DateTime end) {
        LOG.log(Level.FINE, "request received({0}, {1})", new Object[]{begin, end});
        return history.stream(begin, end);
    }

    @Override
    public Stream<TimeSeriesCollection> stream(Duration stepSize) {
        LOG.log(Level.FINE, "request received({0})", stepSize);
        return history.stream(stepSize);
    }

    @Override
    public Stream<TimeSeriesCollection> stream(DateTime begin, Duration stepSize) {
        LOG.log(Level.FINE, "request received({0}, {1})", new Object[]{begin, stepSize});
        return history.stream(begin, stepSize);
    }

    @Override
    public Stream<TimeSeriesCollection> stream(DateTime begin, DateTime end, Duration stepSize) {
        LOG.log(Level.FINE, "request received({0}, {1}, {2})", new Object[]{begin, end, stepSize});
        return history.stream(begin, end, stepSize);
    }

    @Override
    public Stream<Collection<CollectHistory.NamedEvaluation>> evaluate(Map<String, ? extends TimeSeriesMetricExpression> query, Duration stepSize) {
        LOG.log(Level.FINE, "request received({0})", stepSize);
        Stream<Collection<CollectHistory.NamedEvaluation>> result = history.evaluate(query, stepSize);
        LOG.log(Level.FINE, "returning({0}) => {1}", new Object[]{stepSize, result});
        return result;
    }

    @Override
    public Stream<Collection<CollectHistory.NamedEvaluation>> evaluate(Map<String, ? extends TimeSeriesMetricExpression> query, DateTime begin, Duration stepSize) {
        LOG.log(Level.FINE, "request received({0}, {1})", new Object[]{begin, stepSize});
        Stream<Collection<CollectHistory.NamedEvaluation>> result = history.evaluate(query, begin, stepSize);
        LOG.log(Level.FINE, "returning({0}, {1}) => {2}", new Object[]{begin, stepSize, result});
        return result;
    }

    @Override
    public Stream<Collection<CollectHistory.NamedEvaluation>> evaluate(Map<String, ? extends TimeSeriesMetricExpression> query, DateTime begin, DateTime end, Duration stepSize) {
        LOG.log(Level.FINE, "request received({0}, {1}, {2})", new Object[]{begin, end, stepSize});
        Stream<Collection<CollectHistory.NamedEvaluation>> result = history.evaluate(query, begin, end, stepSize);
        LOG.log(Level.FINE, "returning({0}, {1}, {2}) => {3}", new Object[]{begin, end, stepSize, result});
        return result;
    }
}
