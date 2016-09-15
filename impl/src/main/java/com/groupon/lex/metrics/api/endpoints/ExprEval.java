package com.groupon.lex.metrics.api.endpoints;

import com.google.gson.annotations.SerializedName;
import com.groupon.lex.metrics.MetricValue;
import com.groupon.lex.metrics.Tags;
import com.groupon.lex.metrics.config.ConfigurationException;
import com.groupon.lex.metrics.config.ParserSupport;
import com.groupon.lex.metrics.history.CollectHistory;
import com.groupon.lex.metrics.lib.SimpleMapEntry;
import com.groupon.lex.metrics.timeseries.TimeSeriesMetricDeltaSet;
import com.groupon.lex.metrics.timeseries.TimeSeriesMetricExpression;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import static java.util.Objects.requireNonNull;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.servlet.AsyncContext;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.joda.time.DateTime;
import org.joda.time.Duration;

/**
 *
 * @author ariane
 */
public class ExprEval extends HttpServlet {
    private static final Logger LOG = Logger.getLogger(ExprEval.class.getName());
    private final CollectHistory history_;
    private final static String EXPR_PREFIX = "expr:";
    private final static Duration DEFAULT_INTERVAL = Duration.standardSeconds(5);

    public ExprEval(CollectHistory history) {
        history_ = requireNonNull(history);
    }

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

    private static Stream<TSV> encode_evaluation_result_(Stream<Collection<CollectHistory.NamedEvaluation>> evaluated) {
        return evaluated
                .map(c -> {
                    final DateTime dt = c.stream().findAny().map(CollectHistory.NamedEvaluation::getDatetime).orElseThrow(() -> new IllegalStateException("no expression result"));
                    return new TSV(dt, c);
                });
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        final Optional<DateTime> begin, end;
        final Optional<Duration> stepsize;
        final Map<String, TimeSeriesMetricExpression> exprs;

        try {
            final Optional<String> s_begin = Optional.ofNullable(req.getParameter("begin"));
            final Optional<String> s_end = Optional.ofNullable(req.getParameter("end"));
            final Map<String, String> s_exprs = expressions_(req);
            final Optional<String> s_stepsize = Optional.ofNullable(req.getParameter("stepsize"));

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
            resp.sendError(HttpServletResponse.SC_BAD_REQUEST, ex.getMessage());
            return;
        }

        LOG.log(Level.INFO, "expression evaluation requested {0}-{1} stepping {2}: {3}", new Object[]{begin, end, stepsize, exprs});

        try {
            resp.setStatus(HttpServletResponse.SC_OK);
            resp.setContentType("application/json");
            resp.setCharacterEncoding("UTF-8");

            final AsyncContext ctx = req.startAsync();
            final ServletOutputStream out = resp.getOutputStream();

            final StreamingJsonListEntity expr_result = new StreamingJsonListEntity(ctx, out, encode_evaluation_result_(eval_(exprs, begin, end, stepsize)));
            out.setWriteListener(expr_result);
        } finally {
            LOG.log(Level.INFO, "leaving expression evaluation: {0}", exprs);
        }
    }

    private static class Metric {
        @SerializedName("tags")
        public Map<String, Object> tags;
        @SerializedName("value")
        public Object value;
        @SerializedName("name_tags")
        public String name_tags;

        public Metric(String name, Tags mv_tags, MetricValue mv) {
            name_tags = name + (mv_tags.isEmpty() ? "" : mv_tags.toString());

            tags = mv_tags.stream()
                    .map(tag_entry -> {
                        final String key = tag_entry.getKey();
                        final MetricValue m = tag_entry.getValue();

                        if (m.getBoolValue() != null) return SimpleMapEntry.create(key, m.getBoolValue());
                        if (m.getIntValue() != null) return SimpleMapEntry.create(key, m.getIntValue());
                        if (m.getFltValue() != null) return SimpleMapEntry.create(key, m.getFltValue());
                        if (m.getStrValue() != null) return SimpleMapEntry.create(key, m.getStrValue());
                        return SimpleMapEntry.create(key, null);
                    })
                    .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue()));

            if (mv.getBoolValue() != null)
                value = mv.getBoolValue();
            else if (mv.getIntValue() != null)
                value = mv.getIntValue();
            else if (mv.getFltValue() != null)
                value = mv.getFltValue();
            else if (mv.getStrValue() != null)
                value = mv.getStrValue();
            else
                value = null;
        }
    }

    private static class TSV {
        @SerializedName("timestamp_msec")
        public long timestamp_msec;
        @SerializedName("timestamp_str")
        public String timestamp_iso8601;
        @SerializedName("metrics")
        public Map<String, List<Metric>> metrics;

        public TSV(DateTime timestamp, Collection<CollectHistory.NamedEvaluation> named_data) {
            timestamp_msec = timestamp.getMillis();
            timestamp_iso8601 = timestamp.toString();
            metrics = named_data.stream()
                    .map(nd -> {
                        final String name = nd.getName();
                        final TimeSeriesMetricDeltaSet data = nd.getTS();
                        final List<Metric> ts_metrics = data.streamAsMap()
                                .map(entry -> new Metric(name, entry.getKey(), entry.getValue()))
                                .collect(Collectors.toList());
                        return SimpleMapEntry.create(name, ts_metrics);
                    })
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        }
    }
}
