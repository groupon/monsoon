package com.groupon.lex.metrics.timeseries;

import com.groupon.lex.metrics.lib.ForwardIterator;
import static java.util.Collections.unmodifiableList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.hamcrest.Matchers;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.sameInstance;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import static org.mockito.Matchers.any;
import org.mockito.Mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ExpressionLookBackTest {
    private List<TimeSeriesCollection> tsc;
    @Mock
    private TimeSeriesCollection tsc0, tsc1;
    @Mock
    private ExpressionLookBack mockLookBack1, mockLookBack2;

    private ForwardIterator<TimeSeriesCollection> tsc() { return new ForwardIterator<>(tsc.iterator()); }

    @Before
    public void setup() {
        final DateTime now = new DateTime(DateTimeZone.UTC);
        tsc = unmodifiableList(Stream.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
                .map(Duration::standardMinutes)
                .map(d -> now.minus(d))
                .map(MutableTimeSeriesCollection::new)
                .collect(Collectors.toList()));
    }

    @Test
    public void empty_filter() {
        final List<TimeSeriesCollection> filtered = ExpressionLookBack.EMPTY.filter(tsc()).collect(Collectors.toList());

        assertTrue(filtered.isEmpty());
    }

    @Test
    public void scrapeCount_filter() {
        final List<TimeSeriesCollection> filtered = ExpressionLookBack.fromScrapeCount(3).filter(tsc()).collect(Collectors.toList());

        assertEquals(tsc.subList(0, 3), filtered);
    }

    @Test
    public void interval_filter_exact() {
        final List<TimeSeriesCollection> filtered = ExpressionLookBack.fromInterval(Duration.standardMinutes(5)).filter(tsc()).collect(Collectors.toList());

        assertEquals(tsc.subList(0, 6), filtered);
    }

    @Test
    public void interval_filter_inexact() {
        Duration delta = Duration.standardMinutes(3).plus(Duration.standardSeconds(30));
        final List<TimeSeriesCollection> filtered = ExpressionLookBack.fromInterval(delta).filter(tsc()).collect(Collectors.toList());

        assertEquals(tsc.subList(0, 5), filtered);
    }

    @Test
    public void empty_andThen_filter() {
        List<TimeSeriesCollection> filtered = ExpressionLookBack.EMPTY.andThen(ExpressionLookBack.EMPTY).filter(tsc()).collect(Collectors.toList());
        assertTrue(filtered.isEmpty());

        filtered = ExpressionLookBack.EMPTY.andThen(ExpressionLookBack.fromScrapeCount(3)).filter(tsc()).collect(Collectors.toList());
        assertEquals(tsc.subList(0, 3), filtered);

        filtered = ExpressionLookBack.EMPTY.andThen(ExpressionLookBack.fromInterval(Duration.standardMinutes(5))).filter(tsc()).collect(Collectors.toList());
        assertEquals(tsc.subList(0, 6), filtered);

        filtered = ExpressionLookBack.EMPTY.andThen(ExpressionLookBack.fromInterval(Duration.standardMinutes(3).plus(Duration.standardSeconds(30)))).filter(tsc()).collect(Collectors.toList());
        assertEquals(tsc.subList(0, 5), filtered);
    }

    @Test
    public void scrapeCount_andThen_filter() {
        List<TimeSeriesCollection> filtered = ExpressionLookBack.fromScrapeCount(2).andThen(ExpressionLookBack.EMPTY).filter(tsc()).collect(Collectors.toList());
        assertEquals(tsc.subList(0, 2), filtered);

        filtered = ExpressionLookBack.fromScrapeCount(2).andThen(ExpressionLookBack.fromScrapeCount(3)).filter(tsc()).collect(Collectors.toList());
        assertEquals(tsc.subList(0, 2+3), filtered);

        filtered = ExpressionLookBack.fromScrapeCount(2).andThen(ExpressionLookBack.fromInterval(Duration.standardMinutes(5))).filter(tsc()).collect(Collectors.toList());
        assertEquals(tsc.subList(0, 2+6), filtered);

        filtered = ExpressionLookBack.fromScrapeCount(2).andThen(ExpressionLookBack.fromInterval(Duration.standardMinutes(3).plus(Duration.standardSeconds(30)))).filter(tsc()).collect(Collectors.toList());
        assertEquals(tsc.subList(0, 2+5), filtered);
    }

    @Test
    public void interval_exact_andThen_filter() {
        Duration delta = Duration.standardMinutes(2);
        List<TimeSeriesCollection> filtered = ExpressionLookBack.fromInterval(delta).andThen(ExpressionLookBack.EMPTY).filter(tsc()).collect(Collectors.toList());
        assertEquals(tsc.subList(0, 3), filtered);

        filtered = ExpressionLookBack.fromInterval(delta).andThen(ExpressionLookBack.fromScrapeCount(3)).filter(tsc()).collect(Collectors.toList());
        assertEquals(tsc.subList(0, 3+3), filtered);

        filtered = ExpressionLookBack.fromInterval(delta).andThen(ExpressionLookBack.fromInterval(Duration.standardMinutes(5))).filter(tsc()).collect(Collectors.toList());
        assertEquals(tsc.subList(0, 3+6), filtered);

        filtered = ExpressionLookBack.fromInterval(delta).andThen(ExpressionLookBack.fromInterval(Duration.standardMinutes(3).plus(Duration.standardSeconds(30)))).filter(tsc()).collect(Collectors.toList());
        assertEquals(tsc.subList(0, 3+5), filtered);
    }

    @Test
    public void interval_inexact_andThen_filter() {
        Duration delta = Duration.standardMinutes(2).minus(Duration.standardSeconds(30));
        List<TimeSeriesCollection> filtered = ExpressionLookBack.fromInterval(delta).andThen(ExpressionLookBack.EMPTY).filter(tsc()).collect(Collectors.toList());
        assertEquals(tsc.subList(0, 3), filtered);

        filtered = ExpressionLookBack.fromInterval(delta).andThen(ExpressionLookBack.fromScrapeCount(3)).filter(tsc()).collect(Collectors.toList());
        assertEquals(tsc.subList(0, 3+3), filtered);

        filtered = ExpressionLookBack.fromInterval(delta).andThen(ExpressionLookBack.fromInterval(Duration.standardMinutes(5))).filter(tsc()).collect(Collectors.toList());
        assertEquals(tsc.subList(0, 3+6), filtered);

        filtered = ExpressionLookBack.fromInterval(delta).andThen(ExpressionLookBack.fromInterval(Duration.standardMinutes(3).plus(Duration.standardSeconds(30)))).filter(tsc()).collect(Collectors.toList());
        assertEquals(tsc.subList(0, 3+5), filtered);
    }

    @Test
    public void empty_andThen_many() {
        when(mockLookBack1.filter(any())).thenReturn(Stream.of(tsc0));
        when(mockLookBack2.filter(any())).thenReturn(Stream.of(tsc1));

        List<TimeSeriesCollection> filtered = ExpressionLookBack.EMPTY.andThen(Stream.of(mockLookBack1, mockLookBack2)).filter(tsc()).collect(Collectors.toList());

        verify(mockLookBack1, times(1)).filter(any());
        verify(mockLookBack2, times(1)).filter(any());
        assertThat(filtered, containsInAnyOrder(sameInstance(tsc0), sameInstance(tsc1)));
        verifyNoMoreInteractions(mockLookBack1, mockLookBack2, tsc0, tsc1);
    }

    @Test
    public void scrapeCount_andThen_many() {
        when(mockLookBack1.filter(any())).thenReturn(Stream.of(tsc0));
        when(mockLookBack2.filter(any())).thenReturn(Stream.of(tsc1));

        List<TimeSeriesCollection> filtered = ExpressionLookBack.fromScrapeCount(2).andThen(Stream.of(mockLookBack1, mockLookBack2)).filter(tsc()).collect(Collectors.toList());

        verify(mockLookBack1, times(1)).filter(any());
        verify(mockLookBack2, times(1)).filter(any());
        assertThat(filtered,
                containsInAnyOrder(
                        Stream.concat(
                                tsc.subList(0, 2).stream(),
                                Stream.of(tsc0, tsc1)
                        )
                        .map(Matchers::sameInstance).collect(Collectors.toList())));
        verifyNoMoreInteractions(mockLookBack1, mockLookBack2, tsc0, tsc1);
    }

    @Test
    public void interval_exact_andThen_many() {
        Duration delta = Duration.standardMinutes(5);
        when(mockLookBack1.filter(any())).thenReturn(Stream.of(tsc0));
        when(mockLookBack2.filter(any())).thenReturn(Stream.of(tsc1));

        List<TimeSeriesCollection> filtered = ExpressionLookBack.fromInterval(delta).andThen(Stream.of(mockLookBack1, mockLookBack2)).filter(tsc()).collect(Collectors.toList());

        verify(mockLookBack1, times(1)).filter(any());
        verify(mockLookBack2, times(1)).filter(any());
        assertThat(filtered,
                containsInAnyOrder(
                        Stream.concat(
                                tsc.subList(0, 6).stream(),
                                Stream.of(tsc0, tsc1)
                        )
                        .map(Matchers::sameInstance).collect(Collectors.toList())));
        verifyNoMoreInteractions(mockLookBack1, mockLookBack2, tsc0, tsc1);
    }

    @Test
    public void interval_inexact_andThen_many() {
        Duration delta = Duration.standardMinutes(3).plus(Duration.standardSeconds(30));
        when(mockLookBack1.filter(any())).thenReturn(Stream.of(tsc0));
        when(mockLookBack2.filter(any())).thenReturn(Stream.of(tsc1));

        List<TimeSeriesCollection> filtered = ExpressionLookBack.fromInterval(delta).andThen(Stream.of(mockLookBack1, mockLookBack2)).filter(tsc()).collect(Collectors.toList());

        verify(mockLookBack1, times(1)).filter(any());
        verify(mockLookBack2, times(1)).filter(any());
        assertThat(filtered,
                containsInAnyOrder(
                        Stream.concat(
                                tsc.subList(0, 5).stream(),
                                Stream.of(tsc0, tsc1)
                        )
                        .map(Matchers::sameInstance).collect(Collectors.toList())));
        verifyNoMoreInteractions(mockLookBack1, mockLookBack2, tsc0, tsc1);
    }
}
