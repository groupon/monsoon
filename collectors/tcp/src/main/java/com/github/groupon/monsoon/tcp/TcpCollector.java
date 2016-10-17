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
package com.github.groupon.monsoon.tcp;

import com.groupon.lex.metrics.GroupGenerator;
import com.groupon.lex.metrics.GroupName;
import com.groupon.lex.metrics.Metric;
import com.groupon.lex.metrics.MetricGroup;
import com.groupon.lex.metrics.MetricName;
import com.groupon.lex.metrics.MetricValue;
import com.groupon.lex.metrics.SimpleMetric;
import com.groupon.lex.metrics.SimpleMetricGroup;
import java.io.IOException;
import java.net.BindException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.NoRouteToHostException;
import java.net.PortUnreachableException;
import java.net.ProtocolException;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.net.UnknownServiceException;
import java.util.ArrayList;
import java.util.Collection;
import static java.util.Collections.singleton;
import java.util.Optional;
import java.util.function.LongSupplier;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Getter
public class TcpCollector implements GroupGenerator {
    /**
     * MetricName under which the timing is published.
     */
    private static final MetricName TIMING_METRIC = MetricName.valueOf("timing");
    /**
     * MetricName under which the error message is published.
     */
    private static final MetricName ERROR_MSG = MetricName.valueOf("error", "msg");
    /**
     * MetricName under which the error type is published.
     */
    private static final MetricName ERROR_TYPE = MetricName.valueOf("error", "type");
    /**
     * Timeout for connect attempts.
     */
    public static final int CONNECT_TIMEOUT_MSEC = 5000;  // 5 seconds

    /**
     * Destination address to check.
     */
    @NonNull
    private final InetSocketAddress dst;
    /**
     * Group name under which to publish metrics.
     */
    @NonNull
    private final GroupName groupName;

    /**
     * The result code of a connect attempt.
     */
    @RequiredArgsConstructor
    public static enum ConnectResult {
        OK(MetricName.valueOf("up")),
        TIMED_OUT(MetricName.valueOf("error", "timed_out")),
        NO_ROUTE_TO_HOST(MetricName.valueOf("error", "no_route_to_host")),
        PORT_UNREACHABLE(MetricName.valueOf("error", "port_unreachable")),
        UNKNOWN_HOST(MetricName.valueOf("error", "unknown_host")),
        UNKNOWN_SERVICE(MetricName.valueOf("error", "unknown_service")),
        CONNECT_FAILED(MetricName.valueOf("error", "connect_failed")),
        PROTOCOL_ERROR(MetricName.valueOf("error", "protocol_error")),
        BIND_FAILED(MetricName.valueOf("error", "bind_failed")),
        IO_ERROR(MetricName.valueOf("error", "io_error"));

        /**
         * MetricName under which a value of true or false is generated, based
         * on if this ConnectResult was yielded by the connect attempt.
         */
        @Getter
        @NonNull
        private final MetricName metricName;
    }

    @RequiredArgsConstructor
    @Getter
    static class ConnectDatum {  // Package visibility for testing purposes.
        private final ConnectResult result;
        private final long msec;
        private final Optional<String> message;
    }

    @Override
    public GroupCollection getGroups() {
        final ConnectDatum connect = tryConnect();
        return GroupGenerator.successResult(singleton(mainGroup(connect)));
    }

    private MetricGroup mainGroup(ConnectDatum connect) {
        Collection<Metric> metrics = new ArrayList<>();
        if (connect.getResult() == ConnectResult.OK)
            metrics.add(new SimpleMetric(TIMING_METRIC, connect.getMsec()));
        metrics.add(new SimpleMetric(ERROR_MSG, connect.getMessage().map(MetricValue::fromStrValue).orElse(MetricValue.EMPTY)));
        metrics.add(new SimpleMetric(ERROR_TYPE, connect.getResult().toString()));
        for (ConnectResult cr : ConnectResult.values())
            metrics.add(new SimpleMetric(cr.getMetricName(), cr == connect.getResult()));
        return new SimpleMetricGroup(groupName, metrics);
    }

    /**
     * Attempt to connect to an address and diagnose failure.
     *
     * @return A ConnectDatum containing the result, the time-delta and an
     * optional error message.
     */
    private ConnectDatum tryConnect() {
        try (Socket dstSocket = new Socket()) {
            return tryConnect(dstSocket);
        } catch (IOException ex) {
            throw new RuntimeException("socket creation failed", ex);
        }
    }

    ConnectDatum tryConnect(Socket dstSocket) {  // Package visibility for testing purposes.
        final Timer timer = new Timer();
        try {
            dstSocket.connect(dst, CONNECT_TIMEOUT_MSEC);
            return new ConnectDatum(ConnectResult.OK, timer.getAsLong(), Optional.empty());
        } catch (SocketTimeoutException ex) {
            return new ConnectDatum(ConnectResult.TIMED_OUT, timer.getAsLong(), Optional.ofNullable(ex.getMessage()));
        } catch (NoRouteToHostException ex) {
            return new ConnectDatum(ConnectResult.NO_ROUTE_TO_HOST, timer.getAsLong(), Optional.ofNullable(ex.getMessage()));
        } catch (PortUnreachableException ex) {
            return new ConnectDatum(ConnectResult.PORT_UNREACHABLE, timer.getAsLong(), Optional.ofNullable(ex.getMessage()));
        } catch (UnknownHostException ex) {
            return new ConnectDatum(ConnectResult.UNKNOWN_HOST, timer.getAsLong(), Optional.ofNullable(ex.getMessage()));
        } catch (UnknownServiceException ex) {
            return new ConnectDatum(ConnectResult.UNKNOWN_SERVICE, timer.getAsLong(), Optional.ofNullable(ex.getMessage()));
        } catch (ProtocolException ex) {
            return new ConnectDatum(ConnectResult.PROTOCOL_ERROR, timer.getAsLong(), Optional.ofNullable(ex.getMessage()));
        } catch (BindException ex) {
            return new ConnectDatum(ConnectResult.BIND_FAILED, timer.getAsLong(), Optional.ofNullable(ex.getMessage()));
        } catch (ConnectException ex) {
            // ConnectException and SocketException seem to cover the same error cases..?
            return new ConnectDatum(ConnectResult.CONNECT_FAILED, timer.getAsLong(), Optional.ofNullable(ex.getMessage()));
        } catch (SocketException ex) {
            // ConnectException and SocketException seem to cover the same error cases..?
            return new ConnectDatum(ConnectResult.CONNECT_FAILED, timer.getAsLong(), Optional.ofNullable(ex.getMessage()));
        } catch (IOException ex) {
            return new ConnectDatum(ConnectResult.IO_ERROR, timer.getAsLong(), Optional.ofNullable(ex.getMessage()));
        }
    }

    /**
     * A simple timer class, that measures time since its construction, in msec.
     */
    private static class Timer implements LongSupplier {
        private final long t0 = System.currentTimeMillis();

        /**
         * Returns the number of msec since the constructor was called.
         */
        @Override
        public long getAsLong() {
            return System.currentTimeMillis() - t0;
        }
    }
}
