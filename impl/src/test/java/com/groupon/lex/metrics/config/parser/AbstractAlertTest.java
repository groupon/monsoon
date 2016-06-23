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
import com.groupon.lex.metrics.MetricName;
import com.groupon.lex.metrics.MetricValue;
import com.groupon.lex.metrics.PushMetricRegistryInstance;
import com.groupon.lex.metrics.config.Configuration;
import com.groupon.lex.metrics.config.ConfigurationException;
import com.groupon.lex.metrics.config.parser.ReplayCollector.AlertStream;
import com.groupon.lex.metrics.config.parser.ReplayCollector.DataPointIdentifier;
import com.groupon.lex.metrics.config.parser.ReplayCollector.DataPointStream;
import com.groupon.lex.metrics.timeseries.Alert;
import com.groupon.lex.metrics.timeseries.AlertState;
import java.io.StringReader;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 *
 * @author ariane
 */
public abstract class AbstractAlertTest {
    private static final Logger LOG = Logger.getLogger(AbstractAlertTest.class.getName());

    protected DataPointStream newDatapoint(GroupName group, String metric, Number... values) {
        return newDatapoint(group, new MetricName(metric), values);
    }

    protected DataPointStream newDatapoint(GroupName group, String var, Boolean... values) {
        return newDatapoint(group, new MetricName(var), values);
    }

    protected DataPointStream newDatapoint(GroupName group, String var, String... values) {
        return newDatapoint(group, new MetricName(var), values);
    }

    protected DataPointStream newDatapoint(GroupName group, MetricName metric, Number... values) {
        return new DataPointStream(new DataPointIdentifier(group, metric),
                Arrays.stream(values)
                        .map(Optional::ofNullable)
                        .map((opt) -> opt.map(MetricValue::fromNumberValue)));
    }

    protected DataPointStream newDatapoint(GroupName group, MetricName var, Boolean... values) {
        return new DataPointStream(new DataPointIdentifier(group, var),
                Arrays.stream(values)
                        .map(Optional::ofNullable)
                        .map((opt) -> opt.map(MetricValue::fromBoolean)));
    }

    protected DataPointStream newDatapoint(GroupName group, MetricName var, String... values) {
        return new DataPointStream(new DataPointIdentifier(group, var),
                Arrays.stream(values)
                        .map(Optional::ofNullable)
                        .map((opt) -> opt.map(MetricValue::fromStrValue)));
    }

    protected AlertStream newDatapoint(GroupName group, AlertState... values) {
        return new AlertStream(group, Arrays.stream(values));
    }

    public static final class AlertValidator implements AutoCloseable {
        private final PushMetricRegistryInstance registry_;

        private AlertValidator(PushMetricRegistryInstance registry) { registry_ = registry; }

        public void validate(AlertStream... alerts) {
            final Map<GroupName, Iterator<AlertState>> expect = Arrays.stream(alerts).collect(Collectors.toMap(AlertStream::getIdentifier, AlertStream::iterator));

            while (expect.values().stream().anyMatch((iter) -> iter.hasNext())) {
                registry_.updateCollection();
                LOG.log(Level.INFO, "processing: {0}", registry_.getCollectionData());

                expect.forEach((name, iter) -> {
                    if (!iter.hasNext()) return;
                    final AlertState expected_alertstate = iter.next();

                    final Alert alert = registry_.getCollectionAlerts().get(name);
                    switch (expected_alertstate) {
                        case UNKNOWN:
                            break;
                        default:
                            assertNotNull(name.toString() + " present", alert);
                    }
                    if (alert != null) {
                        assertEquals(name.toString(), expected_alertstate, alert.getAlertState());
                    }
                });
            }
        }

        public void close() {
            registry_.close();
        }
    }

    protected AlertValidator replay(String configuration, int interval_seconds, DataPointStream... input) throws Exception {
        Supplier<DateTime> datetime_supplier = new Supplier<DateTime>() {
            private DateTime now = new DateTime(DateTimeZone.UTC);

            @Override
            public synchronized DateTime get() {
                DateTime result = now;
                now = now.plus(Duration.standardSeconds(interval_seconds));
                return result;
            }
        };

        try {
            final PushMetricRegistryInstance registry = Configuration.readFromFile(null, new StringReader(configuration))
                    .create(getClass().getName(), datetime_supplier, (pattern, handler) -> {});
            registry.add(new ReplayCollector(input));
            return new AlertValidator(registry);
        } catch (ConfigurationException ex) {
            ex.getParseErrors().forEach((error) -> LOG.log(Level.SEVERE, "parse error: {0}", error));
            throw ex;
        }
    }
}
