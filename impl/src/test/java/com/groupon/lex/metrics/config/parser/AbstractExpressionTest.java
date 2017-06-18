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
package com.groupon.lex.metrics.config.parser;

import com.groupon.lex.metrics.GroupName;
import com.groupon.lex.metrics.Histogram;
import com.groupon.lex.metrics.MetricName;
import com.groupon.lex.metrics.MetricValue;
import com.groupon.lex.metrics.PushMetricRegistryInstance;
import com.groupon.lex.metrics.config.Configuration;
import com.groupon.lex.metrics.config.ConfigurationException;
import com.groupon.lex.metrics.config.parser.ReplayCollector.DataPointIdentifier;
import com.groupon.lex.metrics.config.parser.ReplayCollector.DataPointStream;
import com.groupon.lex.metrics.httpd.EndpointRegistration;
import com.groupon.lex.metrics.timeseries.Alert;
import com.groupon.lex.metrics.timeseries.AlertState;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.Arrays;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import static org.junit.Assert.assertEquals;

/**
 *
 * @author ariane
 */
public abstract class AbstractExpressionTest {
    private static final Logger LOG = Logger.getLogger(AbstractExpressionTest.class.getName());
    protected static final GroupName GROUP = GroupName.valueOf("TEST");
    private static final GroupName ALERT_NAME = GroupName.valueOf("TEST_ALERT");

    protected AbstractExpressionTest() {
    }

    private static Configuration configurationForExpr(String expr, Stream<DataPointIdentifier> vars) throws IOException, ConfigurationException {
        StringBuilder config = new StringBuilder()
                .append("alias ").append(GROUP.configString()).append(" as test;")
                .append("alert ").append(ALERT_NAME).append(" if !(").append(expr).append(");");

        try (Reader input = new StringReader(config.toString())) {
            LOG.log(Level.INFO, "using configuration: {0}", config);
            return Configuration.readFromFile(null, input);
        } catch (ConfigurationException ex) {
            ex.getParseErrors().forEach((error) -> LOG.log(Level.SEVERE, "parse error: {0}", error));
            throw ex;
        }
    }

    protected DataPointStream newDatapoint(String var, Histogram... values) {
        return newDatapoint(MetricName.valueOf(var), values);
    }

    protected DataPointStream newDatapoint(String var, Number... values) {
        return newDatapoint(MetricName.valueOf(var), values);
    }

    protected DataPointStream newDatapoint(String var, Boolean... values) {
        return newDatapoint(MetricName.valueOf(var), values);
    }

    protected DataPointStream newDatapoint(String var, String... values) {
        return newDatapoint(MetricName.valueOf(var), values);
    }

    protected DataPointStream newDatapoint(MetricName var, Histogram... values) {
        return new ReplayCollector.DataPointStream(new ReplayCollector.DataPointIdentifier(GROUP, var),
                Arrays.stream(values)
                .map(Optional::ofNullable)
                .map((opt) -> opt.map(MetricValue::fromHistValue)));
    }

    protected DataPointStream newDatapoint(MetricName var, Number... values) {
        return new ReplayCollector.DataPointStream(new ReplayCollector.DataPointIdentifier(GROUP, var),
                Arrays.stream(values)
                .map(Optional::ofNullable)
                .map((opt) -> opt.map(MetricValue::fromNumberValue)));
    }

    protected DataPointStream newDatapoint(MetricName var, Boolean... values) {
        return new ReplayCollector.DataPointStream(new ReplayCollector.DataPointIdentifier(GROUP, var),
                Arrays.stream(values)
                .map(Optional::ofNullable)
                .map((opt) -> opt.map(MetricValue::fromBoolean)));
    }

    protected DataPointStream newDatapoint(MetricName var, String... values) {
        return new ReplayCollector.DataPointStream(new ReplayCollector.DataPointIdentifier(GROUP, var),
                Arrays.stream(values)
                .map(Optional::ofNullable)
                .map((opt) -> opt.map(MetricValue::fromStrValue)));
    }

    protected void validateExpression(String expr, DataPointStream... input) throws Exception {
        try (final PushMetricRegistryInstance registry = configurationForExpr(expr, Arrays.stream(input).map(DataPointStream::getIdentifier))
                .create(PushMetricRegistryInstance::new, new NowSupplier(), EndpointRegistration.DUMMY)) {
            ReplayCollector replay_collector = new ReplayCollector(input);
            registry.add(replay_collector);

            while (replay_collector.hasNext()) {
                registry.updateCollection();
                if (registry.getCollectionData().isEmpty()) return;
                LOG.log(Level.INFO, "processing: {0}", registry.getCollectionData());

                final Alert alert = registry.getCollectionAlerts().get(ALERT_NAME);
                assertEquals(AlertState.OK, alert.getAlertState());
            }
        }
    }

    private static class NowSupplier implements Supplier<DateTime> {
        private DateTime cur = DateTime.now();

        @Override
        public DateTime get() {
            final DateTime rv = cur;
            cur = cur.plus(Duration.standardMinutes(1));
            return rv;
        }
    }
}
