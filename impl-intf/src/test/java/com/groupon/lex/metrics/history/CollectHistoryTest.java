package com.groupon.lex.metrics.history;

import com.groupon.lex.metrics.GroupName;
import com.groupon.lex.metrics.MetricName;
import com.groupon.lex.metrics.MetricValue;
import com.groupon.lex.metrics.SimpleGroupPath;
import com.groupon.lex.metrics.Tags;
import com.groupon.lex.metrics.lib.SimpleMapEntry;
import com.groupon.lex.metrics.timeseries.ExpressionLookBack;
import com.groupon.lex.metrics.timeseries.ImmutableTimeSeriesValue;
import com.groupon.lex.metrics.timeseries.SimpleTimeSeriesCollection;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollection;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollectionPair;
import com.groupon.lex.metrics.timeseries.TimeSeriesMetricDeltaSet;
import com.groupon.lex.metrics.timeseries.TimeSeriesMetricExpression;
import com.groupon.lex.metrics.timeseries.TimeSeriesMetricFilter;
import com.groupon.lex.metrics.timeseries.TimeSeriesValue;
import com.groupon.lex.metrics.timeseries.expression.Context;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import static java.util.Collections.singletonMap;
import static java.util.Collections.unmodifiableList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import lombok.RequiredArgsConstructor;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.Test;

public class CollectHistoryTest {
    private CollectHistory history;

    private List<TimeSeriesCollection> data;
    private List<CollectHistory.NamedEvaluation> evaluations;
    private static final DateTime TS0 = DateTime.now(DateTimeZone.UTC);

    private static final Tags TAGS_FOO = Tags.valueOf(singletonMap("key", MetricValue.fromStrValue("foo")));
    private static final Tags TAGS_BAR = Tags.valueOf(singletonMap("key", MetricValue.fromStrValue("bar")));

    @Before
    public void setup() {
        data = unmodifiableList(IntStream.rangeClosed(0, 10)
                .mapToObj(streamIdx -> {
                    final int idx = 10 - streamIdx;

                    List<TimeSeriesValue> tsv = new ArrayList<>();
                    tsv.add(new ImmutableTimeSeriesValue(GroupName.valueOf(SimpleGroupPath.valueOf("G"), TAGS_FOO), singletonMap(MetricName.valueOf("x"), MetricValue.fromIntValue(idx))));
                    tsv.add(new ImmutableTimeSeriesValue(GroupName.valueOf(SimpleGroupPath.valueOf("G"), TAGS_BAR), singletonMap(MetricName.valueOf("x"), MetricValue.fromIntValue(idx))));

                    return new SimpleTimeSeriesCollection(TS0.minus(Duration.standardMinutes(idx)), tsv);
                })
                .collect(Collectors.toList()));

        evaluations = unmodifiableList(IntStream.rangeClosed(0, 10)
                .mapToObj(streamIdx -> {
                    final int idx = 10 - streamIdx;

                    final TimeSeriesMetricDeltaSet contents = new TimeSeriesMetricDeltaSet(Stream.of(
                            SimpleMapEntry.create(TAGS_FOO, MetricValue.fromIntValue(idx)),
                            SimpleMapEntry.create(TAGS_BAR, MetricValue.fromIntValue(idx))));

                    return new CollectHistory.NamedEvaluation("label", TS0.minus(Duration.standardMinutes(idx)),
                            contents);
                })
                .collect(Collectors.toList()));

        history = new Impl(data);
    }

    @Test
    public void getContextTest() {
        List<TimeSeriesCollection> entries = history.getContext(Duration.ZERO, ExpressionLookBack.EMPTY, TimeSeriesMetricFilter.ALL_GROUPS)
                .map(Context::getTSData)
                .map(TimeSeriesCollectionPair::getCurrentCollection)
                .collect(Collectors.toList());

        assertEquals(data, entries);
    }

    @Test
    public void getContextWithSkipTest() {
        List<TimeSeriesCollection> entries = history.getContext(Duration.standardMinutes(5), ExpressionLookBack.EMPTY, TimeSeriesMetricFilter.ALL_GROUPS)
                .map(Context::getTSData)
                .map(TimeSeriesCollectionPair::getCurrentCollection)
                .collect(Collectors.toList());

        assertEquals(Arrays.asList(data.get(0), data.get(5), data.get(10)), entries);
    }

    @Test
    public void evaluateTest() throws Exception {
        List<CollectHistory.NamedEvaluation> entries = history.evaluate(singletonMap("label", TimeSeriesMetricExpression.valueOf("G x")), Duration.ZERO)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());

        assertEquals(evaluations, entries);
    }

    @Test
    public void evaluateWithSkipTest() throws Exception {
        List<CollectHistory.NamedEvaluation> entries = history.evaluate(singletonMap("label", TimeSeriesMetricExpression.valueOf("G x")), Duration.standardMinutes(5))
                .flatMap(Collection::stream)
                .collect(Collectors.toList());

        assertEquals(Arrays.asList(evaluations.get(0), evaluations.get(5), evaluations.get(10)), entries);
    }

    @RequiredArgsConstructor
    private static class Impl implements CollectHistory {
        private final List<TimeSeriesCollection> data;

        @Override
        public boolean add(TimeSeriesCollection tsv) {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public boolean addAll(Collection<? extends TimeSeriesCollection> c) {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public long getFileSize() {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public DateTime getEnd() {
            return data.get(data.size() - 1).getTimestamp();
        }

        @Override
        public Stream<TimeSeriesCollection> stream() {
            return data.stream();
        }
    }
}
