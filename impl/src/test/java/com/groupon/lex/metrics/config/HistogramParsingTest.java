package com.groupon.lex.metrics.config;

import com.groupon.lex.metrics.Histogram;
import java.util.ArrayList;
import java.util.List;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author ariane
 */
public class HistogramParsingTest {
    private List<Histogram.RangeWithCount> map;

    @Before
    public void setup() {
        map = new ArrayList<>();
        map.add(new Histogram.RangeWithCount(new Histogram.Range(0, 2),  2d));
        map.add(new Histogram.RangeWithCount(new Histogram.Range(2, 4), 20d));
        map.add(new Histogram.RangeWithCount(new Histogram.Range(3, 5),  2d));
        map.add(new Histogram.RangeWithCount(new Histogram.Range(3, 5),  2d));  // Duplicates must be preserved.
    }

    @Test
    public void parse_empty() throws Exception {
        Histogram h = new ParserSupport("[]").histogram();

        assertEquals(new Histogram(), h);
        assertEquals("Histogram.toString is parseable", h, new ParserSupport(h.toString()).histogram());
    }

    @Test
    public void single_entry() throws Exception {
        Histogram h = new ParserSupport("[0.0..2=2.0]").histogram();

        assertEquals(new Histogram(map.stream().limit(1)), h);
        assertEquals("Histogram.toString is parseable", h, new ParserSupport(h.toString()).histogram());
    }

    @Test
    public void two_entries() throws Exception {
        Histogram h = new ParserSupport("[0.0..2=2.0,2.0..4.0=20]").histogram();

        assertEquals(new Histogram(map.stream().limit(2)), h);
        assertEquals("Histogram.toString is parseable", h, new ParserSupport(h.toString()).histogram());
    }

    @Test
    public void three_entries() throws Exception {
        Histogram h = new ParserSupport("[0.0..2=2.0,2.0..4.0=20, 3..5=2]").histogram();

        assertEquals(new Histogram(map.stream().limit(3)), h);
        assertEquals("Histogram.toString is parseable", h, new ParserSupport(h.toString()).histogram());
    }

    @Test
    public void four_entries() throws Exception {
        Histogram h = new ParserSupport("[0.0..2=2.0,2.0..4.0=20, 3..5=2, 3..5=2]").histogram();

        assertEquals(new Histogram(map.stream().limit(4)), h);
        assertEquals("Histogram.toString is parseable", h, new ParserSupport(h.toString()).histogram());
    }
}
