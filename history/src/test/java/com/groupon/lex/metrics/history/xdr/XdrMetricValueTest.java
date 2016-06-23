/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.groupon.lex.metrics.history.xdr;

import com.groupon.lex.metrics.history.v0.xdr.ToXdr;
import com.groupon.lex.metrics.history.v0.xdr.FromXdr;
import com.groupon.lex.metrics.Histogram;
import com.groupon.lex.metrics.MetricValue;
import java.util.stream.Stream;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

/**
 *
 * @author ariane
 */
public class XdrMetricValueTest {
    private MetricValue write_and_read_back(MetricValue mv) {
        return FromXdr.metricValue(ToXdr.metricValue(mv));
    }

    @Test
    public void empty_mv() {
        assertEquals(MetricValue.EMPTY, write_and_read_back(MetricValue.EMPTY));
    }

    @Test
    public void int_mv() {
        assertEquals(MetricValue.fromIntValue(17), write_and_read_back(MetricValue.fromIntValue(17)));
    }

    @Test
    public void flt_mv() {
        assertEquals(MetricValue.fromDblValue(17), write_and_read_back(MetricValue.fromDblValue(17)));
    }

    @Test
    public void str_mv() {
        assertEquals(MetricValue.fromStrValue("17"), write_and_read_back(MetricValue.fromStrValue("17")));
    }

    @Test
    public void hist_mv() {
        Histogram h = new Histogram(Stream.of(new Histogram.RangeWithCount(new Histogram.Range(0, 10), 10), new Histogram.RangeWithCount(new Histogram.Range(50, 60), 20)));

        assertEquals(MetricValue.fromHistValue(h), write_and_read_back(MetricValue.fromHistValue(h)));
    }
}
