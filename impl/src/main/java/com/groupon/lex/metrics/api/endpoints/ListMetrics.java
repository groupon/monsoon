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
package com.groupon.lex.metrics.api.endpoints;

import com.groupon.lex.metrics.MetricName;
import com.groupon.lex.metrics.MetricValue;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollection;
import com.groupon.lex.metrics.timeseries.TimeSeriesValue;
import java.nio.charset.Charset;
import static java.util.Collections.EMPTY_LIST;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 *
 * @author ariane
 */
public class ListMetrics extends HttpServlet {
    private static final Logger LOG = Logger.getLogger(ListMetrics.class.getName());
    private final static Charset UTF8 = Charset.forName("UTF-8");
    private final static String INDENT = "    ";
    private final static AtomicReference<List<TimeSeriesValue>> ts_data_ = new AtomicReference<>(EMPTY_LIST);

    private static void render_(StringBuilder out, String indent, List<TimeSeriesValue> ts_data) {
        ts_data.stream()
                .sorted(Comparator.comparing(TimeSeriesValue::getGroup))
                .forEach(tsv -> render_(out, indent, tsv));
    }

    private static void render_(StringBuilder out, String indent, MetricName metric, MetricValue value) {
        out
                .append(indent)
                .append(metric.configString())
                .append(" = ")
                .append(value.toString())
                .append('\n');
    }

    private static void render_(StringBuilder out, String indent, TimeSeriesValue tsv) {
        final Map<MetricName, MetricValue> metrics = tsv.getMetrics();
        if (metrics.isEmpty()) {
            out
                    .append(indent)
                    .append(tsv.getGroup().configString())
                    .append(" {}\n");
        } else {
            final String subindent = indent + INDENT;
            out
                    .append(indent)
                    .append(tsv.getGroup().configString())
                    .append(" {\n");
            metrics.entrySet().stream()
                    .sorted(Comparator.comparing(Map.Entry::getKey))
                    .forEach(metric_value -> render_(out, subindent, metric_value.getKey(), metric_value.getValue()));
            out
                    .append(indent)
                    .append("}\n");
        }
    }

    private static StringBuilder render_(List<TimeSeriesValue> ts_data) throws NoSuchElementException {
        if (ts_data == null) throw new NoSuchElementException();

        StringBuilder buf = new StringBuilder();
        render_(buf, "", ts_data);
        return buf;
    }

    private static StringBuilder render_() {
        return render_(ts_data_.get());
    }

    public static void update(TimeSeriesCollection ts_data) {
        ts_data_.set(ts_data.getTSValues().stream().collect(Collectors.toList()));
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) {
        try {
            final StringBuilder result = render_();

            resp.setStatus(HttpServletResponse.SC_OK);
            resp.getWriter().write(result.toString());
        } catch (NoSuchElementException ex) {
            resp.setStatus(HttpServletResponse.SC_NOT_FOUND);
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, "error generating metric list", ex);
            resp.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        }
    }
}
