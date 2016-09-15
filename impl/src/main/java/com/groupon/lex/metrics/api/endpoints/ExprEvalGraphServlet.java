/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.groupon.lex.metrics.api.endpoints;

import com.google.visualization.datasource.DataSourceServlet;
import com.google.visualization.datasource.base.DataSourceException;
import com.google.visualization.datasource.base.ReasonType;
import com.google.visualization.datasource.datatable.ColumnDescription;
import com.google.visualization.datasource.datatable.DataTable;
import com.google.visualization.datasource.datatable.TableRow;
import com.google.visualization.datasource.datatable.value.DateTimeValue;
import com.google.visualization.datasource.datatable.value.NumberValue;
import com.google.visualization.datasource.datatable.value.ValueType;
import com.google.visualization.datasource.query.Query;
import com.groupon.lex.metrics.MetricValue;
import com.groupon.lex.metrics.Tags;
import com.groupon.lex.metrics.config.ConfigurationException;
import com.groupon.lex.metrics.config.ParserSupport;
import com.groupon.lex.metrics.history.CollectHistory;
import com.groupon.lex.metrics.lib.SimpleMapEntry;
import com.groupon.lex.metrics.timeseries.TimeSeriesMetricDeltaSet;
import com.groupon.lex.metrics.timeseries.TimeSeriesMetricExpression;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import static java.util.Objects.requireNonNull;
import java.util.Optional;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.servlet.http.HttpServletRequest;
import org.joda.time.DateTime;
import org.joda.time.Duration;

/**
 *
 * @author ariane
 */
public class ExprEvalGraphServlet extends DataSourceServlet {
    private static final Logger LOG = Logger.getLogger(ExprEvalGraphServlet.class.getName());
    private final static Charset UTF8 = Charset.forName("UTF-8");
    private final CollectHistory history_;
    private final static String EXPR_PREFIX = "expr:";
    private final static Duration DEFAULT_INTERVAL = Duration.standardSeconds(5);

    private Stream<Collection<CollectHistory.NamedEvaluation>> eval_(Map<String, ? extends TimeSeriesMetricExpression> expr, Optional<DateTime> begin, Optional<DateTime> end, Optional<Duration> stepsize) {
        if (end.isPresent())
            return history_.evaluate(expr, begin.get(), end.get(), stepsize.orElse(DEFAULT_INTERVAL));
        else if (begin.isPresent())
            return history_.evaluate(expr, begin.get(), stepsize.orElse(DEFAULT_INTERVAL));
        else
            return history_.evaluate(expr, stepsize.orElse(DEFAULT_INTERVAL));
    }

    private static Map<String, String> expressions_(HttpServletRequest hsr) {
        return hsr.getParameterMap().entrySet().stream()
                .flatMap(entry -> Arrays.stream(entry.getValue()).map(v -> SimpleMapEntry.create(entry.getKey(), v)))
                .filter(entry -> entry.getKey().startsWith(EXPR_PREFIX))
                .collect(Collectors.toMap(entry -> entry.getKey().substring(EXPR_PREFIX.length()), entry -> entry.getValue()));
    }

    private DateTimeValue to_graph_datetime_(DateTime ts) {
        return new DateTimeValue(ts.getYear(), ts.getMonthOfYear() - 1, ts.getDayOfMonth(), ts.getHourOfDay(), ts.getMinuteOfHour(), ts.getSecondOfMinute(), ts.getMillisOfSecond());
    }

    public ExprEvalGraphServlet(CollectHistory history) {
        history_ = requireNonNull(history);
    }

    @Override
    protected boolean isRestrictedAccessMode() { return false; }

    @Override
    public DataTable generateDataTable(Query query, HttpServletRequest hsr) throws DataSourceException {
        final Optional<DateTime> begin, end;
        final Optional<Duration> stepsize;
        final Map<String, TimeSeriesMetricExpression> exprs;

        try {
            final Optional<String> s_begin = Optional.ofNullable(hsr.getParameter("begin"));
            final Optional<String> s_end = Optional.ofNullable(hsr.getParameter("end"));
            final Map<String, String> s_exprs = expressions_(hsr);
            final Optional<String> s_stepsize = Optional.ofNullable(hsr.getParameter("stepsize"));

            if (s_begin.isPresent())
                begin = Optional.of(Long.valueOf(s_begin.get()))
                        .map(DateTime::new);
            else
                begin = Optional.empty();
            if (s_end.isPresent())
                end = Optional.of(Long.valueOf(s_end.get()))
                        .map(DateTime::new);
            else
                end = Optional.empty();
            if (s_stepsize.isPresent())
                stepsize = Optional.of(Long.valueOf(s_stepsize.get()))
                        .map(Duration::new);
            else
                stepsize = Optional.empty();

            if (end.isPresent() && !begin.isPresent())
                throw new Exception("end specified without begin");
            if (s_exprs.isEmpty())
                throw new Exception("no expressions defined");
            exprs = s_exprs.entrySet().stream()
                    .collect(Collectors.toMap(entry -> entry.getKey(), entry -> {
                        try {
                            return new ParserSupport(entry.getValue()).expression();
                        } catch (IOException ex) {
                            throw new RuntimeException(ex);
                        } catch (ConfigurationException ex) {
                            if (ex.getParseErrors().isEmpty()) throw new RuntimeException(ex);
                            throw new RuntimeException(String.join("\n", ex.getParseErrors()), ex);
                        }
                    }));
        } catch (Exception ex) {
            throw new DataSourceException(ReasonType.INVALID_QUERY, ex.getMessage());
        }

        DataTable dt = new DataTable();
        dt.addColumn(new ColumnDescription("timestamp", ValueType.DATETIME, "timestamp"));

        final Iterator<Collection<CollectHistory.NamedEvaluation>> eval_iter = eval_(exprs, begin, end, stepsize).iterator();
        while (eval_iter.hasNext()) {
            final Collection<CollectHistory.NamedEvaluation> ts_tsdata = eval_iter.next();
            final DateTime ts = ts_tsdata.stream().findAny().map(CollectHistory.NamedEvaluation::getDatetime).orElseThrow(() -> new IllegalStateException("empty evaluation"));
            final TableRow row = new TableRow();
            row.addCell(to_graph_datetime_(ts));

            for (CollectHistory.NamedEvaluation eval : ts_tsdata) {
                final String name = eval.getName();
                assert(ts.equals(eval.getDatetime()));
                final TimeSeriesMetricDeltaSet tsdata = eval.getTS();

                tsdata.streamAsMap()
                        .map(tag_val -> {
                            final Tags tags = tag_val.getKey();
                            final String tag_string = (tags.isEmpty() ? name : name + tags.toString());
                            final MetricValue val = tag_val.getValue();
                            if (!dt.containsColumn(tag_string))
                                dt.addColumn(new ColumnDescription(tag_string, ValueType.NUMBER, tag_string));
                            final int column = dt.getColumnIndex(tag_string);

                            return SimpleMapEntry.create(column, val);
                        })
                        .sorted(Comparator.comparing(Map.Entry::getKey))
                        .forEachOrdered(value -> {
                            while (row.getCells().size() < value.getKey())
                                row.addCell(NumberValue.getNullValue());
                            row.addCell(value.getValue().value().map(Number::doubleValue).map(NumberValue::new).orElseGet(NumberValue::getNullValue));
                        });
            }
            dt.addRow(row);
        }

        return dt;
    }
}
