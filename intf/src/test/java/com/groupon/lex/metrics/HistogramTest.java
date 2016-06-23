/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.groupon.lex.metrics;

import java.util.Collection;
import static java.util.Collections.singletonMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

/**
 *
 * @author ariane
 */
public class HistogramTest {
    private static Histogram.RangeWithCount map_(Map.Entry<Histogram.Range, Double> entry) {
        return new Histogram.RangeWithCount(entry.getKey(), entry.getValue());
    }

    @Test
    public void empty_histogram() {
        final Histogram h = new Histogram();
        final Histogram i = new Histogram();

        assertEquals(0, h.stream().count());
        assertEquals(0, h.getEventCount(), 0.000001);
        assertTrue(h.equals(i));
        assertEquals(h.hashCode(), i.hashCode());
        assertTrue(h.isEmpty());
    }

    @Test
    public void constructor() {
        final Map<Histogram.Range, Double> map = new HashMap<Histogram.Range, Double>() {{
            put(new Histogram.Range(0,  1), 1d);
            put(new Histogram.Range(1,  2), 1d);
            put(new Histogram.Range(2,  6), 4d);
            put(new Histogram.Range(6,  7), 1d);
            put(new Histogram.Range(7, 10), 3d);
        }};
        final Histogram h = new Histogram(map.entrySet().stream().map(HistogramTest::map_));

        assertThat(h.stream().collect(Collectors.toList()),
                contains(new Histogram.RangeWithCount(new Histogram.Range(0, 10), 10)));
        assertEquals(10, h.getEventCount(), 0.000001);
        assertFalse(h.isEmpty());
    }

    @Test
    public void drop_zeroes() {
        final Map<Histogram.Range, Double> expect = new HashMap<Histogram.Range, Double>() {{
            put(new Histogram.Range(0,  1), 1d);
            put(new Histogram.Range(1,  2), 1d);
            put(new Histogram.Range(2,  6), 4d);
            put(new Histogram.Range(6,  7), 1d);
            put(new Histogram.Range(7, 10), 3d);
        }};
        final Stream<Histogram.RangeWithCount> init =
                Stream.of(
                        expect,
                        singletonMap(new Histogram.Range(-100,  -10),  0d),
                        singletonMap(new Histogram.Range(1000, 1100),  7d),
                        singletonMap(new Histogram.Range(1000, 1100), -7d)
                )
                .map(Map::entrySet)
                .flatMap(Collection::stream)
                .map(HistogramTest::map_);
        final Histogram h = new Histogram(init);

        assertThat(h.stream().collect(Collectors.toList()),
                contains(new Histogram.RangeWithCount(new Histogram.Range(0, 10), 10)));
    }

    @Test(expected = IllegalArgumentException.class)
    public void multiple_sign() {
        new Histogram(new HashMap<Histogram.Range, Double>() {{
            put(new Histogram.Range(0, 1),  1d);
            put(new Histogram.Range(1, 2), -2d);
            put(new Histogram.Range(2, 3),  4d);
        }}.entrySet().stream().map(HistogramTest::map_));
    }

    @Test
    public void add_scalar() {
        final Histogram h = new Histogram(new HashMap<Histogram.Range, Double>() {{
            put(new Histogram.Range(0, 1), 1d);
            put(new Histogram.Range(1, 2), 2d);
            put(new Histogram.Range(2, 4), 4d);
        }}.entrySet().stream().map(HistogramTest::map_));

        assertEquals(
                new Histogram(new HashMap<Histogram.Range, Double>() {{
                    put(new Histogram.Range(0, 1), 11d);
                    put(new Histogram.Range(1, 2), 12d);
                    put(new Histogram.Range(2, 4), 24d);
                }}.entrySet().stream().map(HistogramTest::map_)),
                Histogram.add(h, 10));
    }

    @Test
    public void subtract_scalar() {
        final Histogram h = new Histogram(new HashMap<Histogram.Range, Double>() {{
            put(new Histogram.Range(0, 1), 1d);
            put(new Histogram.Range(1, 2), 2d);
            put(new Histogram.Range(2, 4), 4d);
        }}.entrySet().stream().map(HistogramTest::map_));

        assertEquals(
                new Histogram(new HashMap<Histogram.Range, Double>() {{
                    put(new Histogram.Range(0, 1), 1d - 0.5d);
                    put(new Histogram.Range(1, 2), 2d - 0.5d);
                    put(new Histogram.Range(2, 4), 4d - 1.0d);
                }}.entrySet().stream().map(HistogramTest::map_)),
                Histogram.subtract(h, 0.5));
    }

    @Test
    public void multiply_scalar() {
        final Histogram h = new Histogram(new HashMap<Histogram.Range, Double>() {{
            put(new Histogram.Range(0, 1), 1d);
            put(new Histogram.Range(1, 2), 2d);
            put(new Histogram.Range(2, 4), 4d);
        }}.entrySet().stream().map(HistogramTest::map_));

        assertEquals(
                new Histogram(new HashMap<Histogram.Range, Double>() {{
                    put(new Histogram.Range(0, 1), 10d);
                    put(new Histogram.Range(1, 2), 20d);
                    put(new Histogram.Range(2, 4), 40d);
                }}.entrySet().stream().map(HistogramTest::map_)),
                Histogram.multiply(h, 10));
    }

    @Test
    public void divide_scalar() {
        final Histogram h = new Histogram(new HashMap<Histogram.Range, Double>() {{
            put(new Histogram.Range(0, 1), 10d);
            put(new Histogram.Range(1, 2), 20d);
            put(new Histogram.Range(2, 4), 40d);
        }}.entrySet().stream().map(HistogramTest::map_));

        assertEquals(
                new Histogram(new HashMap<Histogram.Range, Double>() {{
                    put(new Histogram.Range(0, 1), 1d);
                    put(new Histogram.Range(1, 2), 2d);
                    put(new Histogram.Range(2, 4), 4d);
                }}.entrySet().stream().map(HistogramTest::map_)),
                Histogram.divide(h, 10));
    }

    @Test
    public void percentile() {
        final Histogram h = new Histogram(new HashMap<Histogram.Range, Double>() {{
            put(new Histogram.Range(0,  1), 1d);
            put(new Histogram.Range(1,  2), 1d);
            put(new Histogram.Range(2,  6), 4d);
            put(new Histogram.Range(6,  7), 1d);
            put(new Histogram.Range(7, 10), 3d);
        }}.entrySet().stream().map(HistogramTest::map_));

        assertEquals( 0,   h.percentile(  0), 0.000001);
        assertEquals(10,   h.percentile(100), 0.000001);
        assertEquals( 5,   h.percentile( 50), 0.000001);
        assertEquals( 9.9, h.percentile( 99), 0.000001);
        assertEquals( 6.9, h.percentile( 69), 0.000001);
    }

    @Test
    public void percentile_for_negative_counts() {
        final Histogram h = new Histogram(new HashMap<Histogram.Range, Double>() {{
            put(new Histogram.Range(0,  1), -1d);
            put(new Histogram.Range(1,  2), -1d);
            put(new Histogram.Range(2,  6), -4d);
            put(new Histogram.Range(6,  7), -1d);
            put(new Histogram.Range(7, 10), -3d);
        }}.entrySet().stream().map(HistogramTest::map_));

        assertEquals(-10,   h.getEventCount(), 0.000001);
        assertEquals(  0,   h.percentile(  0), 0.000001);
        assertEquals( 10,   h.percentile(100), 0.000001);
        assertEquals(  5,   h.percentile( 50), 0.000001);
        assertEquals(  9.9, h.percentile( 99), 0.000001);
        assertEquals(  6.9, h.percentile( 69), 0.000001);
    }

    @Test
    public void get() {
        final Histogram h = new Histogram(new HashMap<Histogram.Range, Double>() {{
            put(new Histogram.Range(0,  1), 1d);
            put(new Histogram.Range(1,  2), 1d);
            put(new Histogram.Range(2,  6), 4d);
            put(new Histogram.Range(6,  7), 1d);
            put(new Histogram.Range(7, 10), 3d);
        }}.entrySet().stream().map(HistogramTest::map_));

        assertEquals( 0,   h.get(-0.0), 0.000001);
        assertEquals( 0,   h.get( 0  ), 0.000001);
        assertEquals(10,   h.get(10  ), 0.000001);
        assertEquals( 5,   h.get( 5  ), 0.000001);
        assertEquals( 9.9, h.get( 9.9), 0.000001);
        assertEquals( 6.9, h.get( 6.9), 0.000001);
    }

    @Test
    public void get_for_negative_counts() {
        final Histogram h = new Histogram(new HashMap<Histogram.Range, Double>() {{
            put(new Histogram.Range(0,  1), -1d);
            put(new Histogram.Range(1,  2), -1d);
            put(new Histogram.Range(2,  6), -4d);
            put(new Histogram.Range(6,  7), -1d);
            put(new Histogram.Range(7, 10), -3d);
        }}.entrySet().stream().map(HistogramTest::map_));

        assertEquals( 0,   h.get(  0  ), 0.000001);
        assertEquals( 0,   h.get( -0.0), 0.000001);
        assertEquals(10,   h.get(-10  ), 0.000001);
        assertEquals( 5,   h.get( -5  ), 0.000001);
        assertEquals( 9.9, h.get( -9.9), 0.000001);
        assertEquals( 6.9, h.get( -6.9), 0.000001);
    }

    @Test
    public void add() {
        Histogram h = new Histogram(new HashMap<Histogram.Range, Double>() {{
            put(new Histogram.Range(0,  8), 8d);
        }}.entrySet().stream().map(HistogramTest::map_));
        Histogram i = new Histogram(new HashMap<Histogram.Range, Double>() {{
            put(new Histogram.Range(4, 10), 6d);
        }}.entrySet().stream().map(HistogramTest::map_));

        Histogram expect = new Histogram(new HashMap<Histogram.Range, Double>() {{
            put(new Histogram.Range(0,  4), 4d);
            put(new Histogram.Range(4,  8), 8d);
            put(new Histogram.Range(8, 10), 2d);
        }}.entrySet().stream().map(HistogramTest::map_));

        assertEquals(expect, Histogram.add(h, i));
    }

    @Test
    public void subtract() {
        Histogram h = new Histogram(new HashMap<Histogram.Range, Double>() {{
            put(new Histogram.Range(0,  1),  2d);
            put(new Histogram.Range(1,  8), 14d);
            put(new Histogram.Range(8, 10),  4d);
        }}.entrySet().stream().map(HistogramTest::map_));
        Histogram i = new Histogram(new HashMap<Histogram.Range, Double>() {{
            put(new Histogram.Range(0, 10), 10d);
        }}.entrySet().stream().map(HistogramTest::map_));

        Histogram expect = new Histogram(new HashMap<Histogram.Range, Double>() {{
            put(new Histogram.Range(0,  1), 1d);
            put(new Histogram.Range(1,  8), 7d);
            put(new Histogram.Range(8, 10), 2d);
        }}.entrySet().stream().map(HistogramTest::map_));

        assertEquals(expect, Histogram.subtract(h, i));
    }

    @Test
    public void subtract_self_is_empty() {
        final Histogram h = new Histogram(new HashMap<Histogram.Range, Double>() {{
            put(new Histogram.Range(0,  1), 1d);
            put(new Histogram.Range(1,  2), 1d);
            put(new Histogram.Range(2,  6), 4d);
            put(new Histogram.Range(6,  7), 1d);
            put(new Histogram.Range(7, 10), 3d);
        }}.entrySet().stream().map(HistogramTest::map_));

        final Histogram expect = new Histogram();

        assertEquals(expect, Histogram.subtract(h, h));
    }

    @Test
    public void gaps() {
        final Map<Histogram.Range, Double> expect = new HashMap<Histogram.Range, Double>() {{
            put(new Histogram.Range(0,  1), 1d);
            put(new Histogram.Range(7, 10), 3d);
        }};

        assertThat(new Histogram(expect.entrySet().stream().map(HistogramTest::map_)).stream().collect(Collectors.toList()),
                contains(new Histogram.RangeWithCount(new Histogram.Range(0, 1), 1), new Histogram.RangeWithCount(new Histogram.Range(7, 10), 3)));
    }

    @Test
    public void gap_percentile() {
        // Percentile must prioritize floor values over ceil values, which matters if there is a gap in the range.
        final Map<Histogram.Range, Double> expect = new HashMap<Histogram.Range, Double>() {{
            put(new Histogram.Range(0,  1), 1d);
            put(new Histogram.Range(9, 10), 1d);
        }};

        assertEquals(9.0, new Histogram(expect.entrySet().stream().map(HistogramTest::map_)).percentile(50), 0.000001);
    }

    @Test
    public void singleton_ranges_percentile() {
        // Percentile must prioritize floor values over ceil values, which matters if there is a gap in the range.
        final Map<Histogram.Range, Double> expect = new HashMap<Histogram.Range, Double>() {{
            put(new Histogram.Range(0, 0), 1d);
            put(new Histogram.Range(9, 9), 1d);
        }};
        Histogram h = new Histogram(expect.entrySet().stream().map(HistogramTest::map_));

        assertEquals(0, h.percentile(  0), 0.000001);
        assertEquals(0, h.percentile( 25), 0.000001);
        assertEquals(0, h.percentile( 49), 0.000001);
        assertEquals(9, h.percentile( 50), 0.000001);
        assertEquals(9, h.percentile( 75), 0.000001);
        assertEquals(9, h.percentile( 99), 0.000001);
        assertEquals(9, h.percentile(100), 0.000001);
    }

    @Test
    public void string_representation() {
        final Map<Histogram.Range, Double> expect = new HashMap<Histogram.Range, Double>() {{
            put(new Histogram.Range(0,  1), 1d);
            put(new Histogram.Range(7, 10), 3d);
        }};

        assertEquals("[ 0.0..1.0=1.0, 7.0..10.0=3.0 ]", new Histogram(expect.entrySet().stream().map(HistogramTest::map_)).toString());
    }

    @Test(expected = IllegalArgumentException.class)
    public void no_inverted_ranges() {
        new Histogram.Range(2, 1);
    }

    @Test
    public void equality() {
        final Histogram h = new Histogram(new HashMap<Histogram.Range, Double>() {{
            put(new Histogram.Range(0,  1), 1d);
            put(new Histogram.Range(7, 10), 3d);
        }}.entrySet().stream().map(HistogramTest::map_));
        final Histogram i = new Histogram(new HashMap<Histogram.Range, Double>() {{
            put(new Histogram.Range(0,  1), 1d);
            put(new Histogram.Range(7,  8), 1d);
            put(new Histogram.Range(8,  9), 1d);
            put(new Histogram.Range(9, 10), 1d);
        }}.entrySet().stream().map(HistogramTest::map_));

        assertEquals(h.hashCode(), i.hashCode());
        assertEquals(h.stream().collect(Collectors.toList()), i.stream().collect(Collectors.toList()));
        assertTrue(h.equals(i));
        assertEquals(0, h.compareTo(i));
    }

    @Test
    public void min_max() {
        final Map<Histogram.Range, Double> map = new HashMap<Histogram.Range, Double>() {{
            put(new Histogram.Range(0,  1), 1d);
            put(new Histogram.Range(7, 10), 3d);
        }};
        Histogram h = new Histogram(map.entrySet().stream().map(HistogramTest::map_));

        assertEquals(Optional.of( 0d), h.min());
        assertEquals(Optional.of(10d), h.max());
    }

    @Test
    public void median_avg() {
        final Map<Histogram.Range, Double> map = new HashMap<Histogram.Range, Double>() {{
            put(new Histogram.Range(0,  1), 1d);
            put(new Histogram.Range(1,  2), 1d);
            put(new Histogram.Range(2,  3), 1d);
            put(new Histogram.Range(3,  4), 1d);
            put(new Histogram.Range(8, 10), 3d);
        }};
        Histogram h = new Histogram(map.entrySet().stream().map(HistogramTest::map_));

        assertEquals(Optional.of(3.5d), h.median());
        assertEquals(Optional.of(5d), h.avg());
    }
}
