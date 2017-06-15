package com.groupon.lex.metrics.api.endpoints;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.gson.annotations.SerializedName;
import static com.groupon.lex.metrics.ConfigSupport.quotedString;
import com.groupon.lex.metrics.MetricValue;
import com.groupon.lex.metrics.Tags;
import com.groupon.lex.metrics.config.ConfigurationException;
import com.groupon.lex.metrics.config.ParserSupport;
import com.groupon.lex.metrics.history.CollectHistory;
import com.groupon.lex.metrics.lib.BufferedIterator;
import com.groupon.lex.metrics.lib.SimpleMapEntry;
import com.groupon.lex.metrics.timeseries.TimeSeriesMetricDeltaSet;
import com.groupon.lex.metrics.timeseries.TimeSeriesMetricExpression;
import java.io.IOException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import static java.util.Objects.requireNonNull;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.servlet.AsyncContext;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;

/**
 *
 * @author ariane
 */
public class ExprEval extends HttpServlet {
    private static final Logger LOG = Logger.getLogger(ExprEval.class.getName());
    private static final Cache<String, IteratorAndCookie> ITERATORS = CacheBuilder.newBuilder()
            .expireAfterAccess(5, TimeUnit.MINUTES)
            .softValues()
            .build();
    private static final int ITER_BUFCOUNT = 4;
    private static final AtomicLong ITERATORS_SEQUENCE = new AtomicLong();
    private final static String EXPR_PREFIX = "expr:";
    private final CollectHistory history_;

    public ExprEval(CollectHistory history) {
        history_ = requireNonNull(history);
    }

    private Stream<Collection<CollectHistory.NamedEvaluation>> eval_(Map<String, ? extends TimeSeriesMetricExpression> expr, Optional<DateTime> begin, Optional<DateTime> end, Optional<Duration> stepsize) {
        if (end.isPresent())
            return history_.evaluate(expr, begin.get(), end.get(), stepsize.orElse(Duration.ZERO));
        else if (begin.isPresent())
            return history_.evaluate(expr, begin.get(), stepsize.orElse(Duration.ZERO));
        else
            return history_.evaluate(expr, stepsize.orElse(Duration.ZERO));
    }

    private static Map<String, String> expressions_(HttpServletRequest hsr) {
        return hsr.getParameterMap().entrySet().stream()
                .flatMap(entry -> Arrays.stream(entry.getValue()).map(v -> SimpleMapEntry.create(entry.getKey(), v)))
                .filter(entry -> entry.getKey().startsWith(EXPR_PREFIX))
                .collect(Collectors.toMap(entry -> entry.getKey().substring(EXPR_PREFIX.length()), entry -> entry.getValue()));
    }

    private static Stream<TSV> encode_evaluation_result_(Stream<Collection<CollectHistory.NamedEvaluation>> evaluated, boolean numericOnly) {
        return evaluated
                .map(c -> {
                    final DateTime dt = c.stream().findAny().map(CollectHistory.NamedEvaluation::getDatetime).orElseThrow(() -> new IllegalStateException("no expression result"));
                    return new TSV(dt, c, numericOnly);
                });
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        final Optional<Long> deadline;
        final DateTime begin;
        try {
            deadline = Optional.ofNullable(req.getParameter("delay"))
                    .map(Long::parseLong)
                    .map(delay -> System.currentTimeMillis() + delay);
            begin = Optional.ofNullable(req.getParameter("begin"))
                    .map(Long::valueOf)
                    .map(msecSinceEpoch -> new DateTime(msecSinceEpoch, DateTimeZone.UTC))
                    .orElseGet(() -> new DateTime(0, DateTimeZone.UTC));
        } catch (Exception ex) {
            resp.sendError(HttpServletResponse.SC_BAD_REQUEST, ex.getMessage());
            return;
        }

        final IteratorAndCookie iterator;
        final String iteratorName;

        final Optional<Map.Entry<String, IteratorAndCookie>> cachedIterator = getCachedIterator(req);
        if (cachedIterator.isPresent()) {
            // Use cached iterator, ignore other arguments.
            iterator = cachedIterator.get().getValue();
            iteratorName = cachedIterator.get().getKey();
        } else {
            // Build new cached iterator from arguments.
            try {
                Map.Entry<String, IteratorAndCookie> newIterator = newIterator(req);
                iterator = newIterator.getValue();
                iteratorName = newIterator.getKey();
            } catch (Exception ex) {
                resp.sendError(HttpServletResponse.SC_BAD_REQUEST, ex.getMessage());
                return;
            }
        }

        resp.setStatus(HttpServletResponse.SC_OK);
        resp.setContentType("application/json");
        resp.setCharacterEncoding("UTF-8");

        final AsyncContext ctx = req.startAsync();
        final ServletOutputStream out = resp.getOutputStream();

        final StreamingJsonListEntity<TSV> expr_result = new StreamingJsonListEntity<>(
                ctx,
                out,
                iterator.getIterator(),
                iteratorName,
                iterator.getCookie(),
                begin,
                iterator.getStepSize(),
                (TSV tsv) -> tsv.timestamp,
                () -> ITERATORS.invalidate(iteratorName),
                deadline);
        out.setWriteListener(expr_result);
    }

    private static class Metric {
        @SerializedName("tags")
        public Map<String, Object> tags;
        @SerializedName("value")
        public Object value;
        @SerializedName("name_tags")
        public String name_tags;

        public Metric(String name, Tags mv_tags, MetricValue mv, boolean numericOnly) {
            name_tags = name + (mv_tags.isEmpty() ? "" : mv_tags.toString());

            tags = mv_tags.stream()
                    .map(tag_entry -> {
                        final String key = tag_entry.getKey();
                        final MetricValue m = tag_entry.getValue();

                        if (m.getBoolValue() != null)
                            return SimpleMapEntry.create(key, m.getBoolValue());
                        if (m.getIntValue() != null)
                            return SimpleMapEntry.create(key, m.getIntValue());
                        if (m.getFltValue() != null)
                            return SimpleMapEntry.create(key, m.getFltValue());
                        if (m.getStrValue() != null)
                            return SimpleMapEntry.create(key, m.getStrValue());
                        return SimpleMapEntry.create(key, null);
                    })
                    .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue()));

            if (numericOnly)
                value = mv.value().orElse(null);
            else if (mv.getBoolValue() != null)
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
        public transient DateTime timestamp;  // Transient keyword excludes it from GSon serialization/deserialization.
        @SerializedName("timestamp_msec")
        public long timestamp_msec;
        @SerializedName("timestamp_str")
        public String timestamp_iso8601;
        @SerializedName("metrics")
        public Map<String, List<Metric>> metrics;

        public TSV(DateTime timestamp, Collection<CollectHistory.NamedEvaluation> named_data, boolean numericOnly) {
            this.timestamp = timestamp;
            timestamp_msec = timestamp.getMillis();
            timestamp_iso8601 = timestamp.toString();
            metrics = named_data.stream()
                    .map(nd -> {
                        final String name = nd.getName();
                        final TimeSeriesMetricDeltaSet data = nd.getTS();
                        final List<Metric> ts_metrics = data.streamAsMap()
                                .map(entry -> new Metric(name, entry.getKey(), entry.getValue(), numericOnly))
                                .collect(Collectors.toList());
                        return SimpleMapEntry.create(name, ts_metrics);
                    })
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        }
    }

    @RequiredArgsConstructor
    @Getter
    private static class IteratorAndCookie {
        private static final SecureRandom RANDOM = new SecureRandom();
        @NonNull
        private final BufferedIterator<TSV> iterator;
        @NonNull
        private final Duration stepSize;
        private String cookie = Long.toHexString(RANDOM.nextLong());

        /**
         * Change the cookie.
         *
         * @return True indicating the expected value matched and the cookie was
         * changed. If the operation fails, false is returned.
         */
        synchronized public boolean update(String expected) {
            if (!cookie.equals(expected)) return false;

            String newCookie;
            do {
                newCookie = Long.toHexString(RANDOM.nextLong());
            } while (cookie.equals(newCookie));  // Always change the cookie.
            cookie = newCookie;

            return true;
        }
    }

    private static Optional<Map.Entry<String, IteratorAndCookie>> getCachedIterator(HttpServletRequest req) {
        final String iter = req.getParameter("iter");
        final String cookie = req.getParameter("cookie");
        if (iter == null || cookie == null) return Optional.empty();
        return Optional.ofNullable(ITERATORS.getIfPresent(iter))
                .filter(ic -> ic.update(cookie))
                .map(cached -> SimpleMapEntry.create(iter, cached));
    }

    private Map.Entry<String, IteratorAndCookie> newIterator(HttpServletRequest req) throws Exception {
        final Optional<DateTime> begin = Optional.ofNullable(req.getParameter("begin"))
                .map(Long::valueOf)
                .map(msecSinceEpoch -> new DateTime(msecSinceEpoch, DateTimeZone.UTC));
        final Optional<DateTime> end = Optional.ofNullable(req.getParameter("end"))
                .map(Long::valueOf)
                .map(msecSinceEpoch -> new DateTime(msecSinceEpoch, DateTimeZone.UTC));
        final Optional<Duration> stepsize = Optional.ofNullable(req.getParameter("stepsize"))
                .map(Long::valueOf)
                .map(Duration::new);
        final boolean numericOnly = Optional.ofNullable(req.getParameter("numeric"))
                .map(s -> {
                    if ("true".equalsIgnoreCase(s))
                        return true;
                    if ("false".equalsIgnoreCase(s))
                        return false;
                    throw new IllegalArgumentException("numeric specified, but does not match true or false");
                })
                .orElse(Boolean.FALSE);
        final Map<String, String> s_exprs = expressions_(req);

        if (end.isPresent() && !begin.isPresent())
            throw new Exception("end specified without begin");
        if (s_exprs.isEmpty())
            throw new Exception("no expressions defined");
        final Map<String, TimeSeriesMetricExpression> exprs = s_exprs.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> {
                    try {
                        return new ParserSupport(entry.getValue()).expression();
                    } catch (IOException ex) {
                        throw new RuntimeException(ex);
                    } catch (ConfigurationException ex) {
                        if (ex.getParseErrors().isEmpty())
                            throw new RuntimeException(ex);
                        throw new RuntimeException(String.join("\n", ex.getParseErrors()), ex);
                    }
                }));

        final IteratorAndCookie iterator = new IteratorAndCookie(
                new BufferedIterator<>(encode_evaluation_result_(eval_(exprs, begin, end, stepsize), numericOnly).iterator(), ITER_BUFCOUNT),
                stepsize.filter(d -> d.getMillis() >= 1000).orElseGet(() -> Duration.standardSeconds(1)));
        final String iteratorName = Long.toHexString(ITERATORS_SEQUENCE.incrementAndGet());
        ITERATORS.put(iteratorName, iterator);

        LOG.log(Level.INFO, "expression evaluation 0x{4} requested {0}-{1} stepping {2}: {3}", new Object[]{begin, end, stepsize, new exprsMapToString(exprs), iteratorName});

        return SimpleMapEntry.create(iteratorName, iterator);
    }

    /**
     * Tiny toString adapter for the logging of expressions.
     */
    @RequiredArgsConstructor
    private static class exprsMapToString {
        private final Map<String, TimeSeriesMetricExpression> exprs;

        @Override
        public String toString() {
            return exprs.entrySet().stream()
                    .sorted(Comparator.comparing(Map.Entry::getKey))
                    .map(entry -> quotedString(entry.getKey()).toString() + '=' + entry.getValue().configString().toString())
                    .collect(Collectors.joining(", ", "{", "}"));
        }
    }
}
