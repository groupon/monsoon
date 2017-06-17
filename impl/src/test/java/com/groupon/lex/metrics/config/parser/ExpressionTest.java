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

import com.groupon.lex.metrics.Histogram;
import com.groupon.lex.metrics.MetricName;
import org.junit.Ignore;
import org.junit.Test;

/**
 *
 * @author ariane
 */
public class ExpressionTest extends AbstractExpressionTest {
    @Test
    public void plus() throws Exception {
        validateExpression("test a + test b = test expected",
                newDatapoint("a", 0, 2, -1, 1),
                newDatapoint("b", 1, 3, -2, -2),
                newDatapoint("expected", 1, 5, -3, -1));
    }

    @Test
    public void precedence_plus_mul() throws Exception {
        validateExpression("test a * test b + test c * test d = test expected",
                newDatapoint("a", 4),
                newDatapoint("b", 4),
                newDatapoint("c", 5),
                newDatapoint("d", 5),
                newDatapoint("expected", 41));
    }

    @Test
    public void precedence_mul_brackets() throws Exception {
        validateExpression("test a * (test b + test c) = test expected",
                newDatapoint("a", 4),
                newDatapoint("b", 1),
                newDatapoint("c", 1),
                newDatapoint("expected", 8));
    }

    @Test
    public void unary_bang_dash() throws Exception {
        validateExpression("!-test a = test expected",
                newDatapoint("a", 7, 0),
                newDatapoint("expected", false, true));
    }

    @Test
    public void unary_dash_bang() throws Exception {
        validateExpression("-!test a = test expected",
                newDatapoint("a", 7, 1, 0),
                newDatapoint("expected", 0, 0, -1));
    }

    @Test
    public void unary_dash_dash() throws Exception {
        validateExpression("--test a = test expected",
                newDatapoint("a", 7, 1, 0, -1, -7),
                newDatapoint("expected", 7, 1, 0, -1, -7));
    }

    @Test
    public void dashes() throws Exception {
        /*
         * a - ---b == a - -b == a + b
         */
        validateExpression("test a----test b = test expected",
                newDatapoint("a", 7, 7, 7, 7, 7, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, -1, -1, -1, -1, -1, -7, -7, -7, -7, -7),
                newDatapoint("b", 7, 1, 0, -1, -7, 7, 1, 0, -1, -7, 7, 1, 0, -1, -7, 7, 1, 0, -1, -7, 7, 1, 0, -1, -7),
                newDatapoint("expected", 14, 8, 7, 6, 0, 8, 2, 1, 0, -6, 7, 1, 0, -1, -7, 6, 0, -1, -2, -8, 0, -6, -7, -8, -14));
    }

    @Test
    @Ignore  // Didn't implement the ternary operator...
    public void ternary_operator() throws Exception {
        validateExpression("(test a ? test b : test c = test d) = test expected",
                newDatapoint("a", true, true, false, false),
                newDatapoint("b", 17, 19, 23, 29),
                newDatapoint("c", 5, 5, 5, 5),
                newDatapoint("d", 5, 3, 5, 3),
                newDatapoint("expected", 17, 19, 1, 1));
    }

    @Test
    public void equality() throws Exception {
        validateExpression("(test a = test b) = test expected",
                newDatapoint("a", true, true, false, false),
                newDatapoint("b", true, false, true, false),
                newDatapoint("expected", true, false, false, true));
    }

    @Test
    public void sum() throws Exception {
        validateExpression("sum(* a, " + GROUP.configString() + " b + 6, -13) = test expected",
                newDatapoint("a", 0, 1, 2, null, 3, 5),
                newDatapoint("b", 1, 1, 1, 1, 1, 1),
                newDatapoint("expected", 1 - 7, 2 - 7, 3 - 7, 1 - 7, 4 - 7, 6 - 7));
    }

    @Test
    public void sum_nil() throws Exception {
        validateExpression("sum(* a, " + GROUP.configString() + " b) = test expected",
                newDatapoint("a", (Double) null),
                newDatapoint("b", (Double) null),
                newDatapoint("expected", 0));
    }

    @Test
    public void regex_match() throws Exception {
        validateExpression("(test a =~ \"^[a-z]+_[^_]+$\") = test expected",
                newDatapoint("a", "foobar", "foobar_bla", "foo_foo_bar", "88_88"),
                newDatapoint("expected", false, true, false, false));
    }

    @Test
    public void regex_mismatch() throws Exception {
        validateExpression("(test a !~ \"^[a-z]+_[^_]+$\") = test expected",
                newDatapoint("a", "foobar", "foobar_bla", "foo_foo_bar", "88_88"),
                newDatapoint("expected", true, false, true, true));
    }

    @Test
    public void regex_match_using_double_slash() throws Exception {
        validateExpression("(test a =~ //^[a-z]+_[^_]+$//) = test expected",
                newDatapoint("a", "foobar", "foobar_bla", "foo_foo_bar", "88_88"),
                newDatapoint("expected", false, true, false, false));
    }

    @Test
    public void regex_mismatch_using_double_slash() throws Exception {
        validateExpression("(test a !~ //^[a-z]+_[^_]+$//) = test expected",
                newDatapoint("a", "foobar", "foobar_bla", "foo_foo_bar", "88_88"),
                newDatapoint("expected", true, false, true, true));
    }

    @Test
    public void double_slash_escapes_dot() throws Exception {
        validateExpression("(test a =~ //^\\.//) = test expected",
                newDatapoint("a", "foobar", ".foobar_bla", "foo_foo_bar", "88_88", ".dot", "\\."),
                newDatapoint("expected", false, true, false, false, true, false));
    }

    @Test
    public void double_slash_escapes_correctly() throws Exception {
        validateExpression("(test a =~ //^[\\\\.]//) = test expected",
                newDatapoint("a", "\\foobar", ".foobar_bla", "foo_foo_bar", "88_88", ".dot", "\\."),
                newDatapoint("expected", true, true, false, false, true, true));
    }

    @Test
    public void double_slash_handles_dot() throws Exception {
        validateExpression("(test a =~ //^.$//) = test expected",
                newDatapoint("a", "a", ".foobar_bla", " ", "\\", ".dot", "."),
                newDatapoint("expected", true, false, true, true, false, true));
    }

    @Test
    public void percentile_agg_exact() throws Exception {
        validateExpression("percentile_agg(50, TEST *) = test ex.pected",
                newDatapoint("a", 4, 7, -2, 0),
                newDatapoint("b", 1, 9, -1, null),
                newDatapoint("c", 1, 11, -3, null),
                newDatapoint(MetricName.valueOf("ex", "pected"), 1d, 9d, -2d, 0d));
    }

    @Test
    public void percentile_agg_fraction() throws Exception {
        validateExpression("percentile_agg(50, TEST *) = test ex.pected",
                newDatapoint("a", 4, 7, -2, 0),
                newDatapoint("b", 1, 9, -1, 1),
                newDatapoint("c", 1, 11, -3, 2),
                newDatapoint("d", 1, 13, -3, null),
                newDatapoint(MetricName.valueOf("ex", "pected"), 1d, 10d, -2.5d, 1d));
    }

    @Test
    public void percentile_agg_p99() throws Exception {
        validateExpression("percentile_agg(99, TEST *) = test ex.pected",
                newDatapoint("00", 0),
                newDatapoint("01", 1),
                newDatapoint("02", 2),
                newDatapoint("03", 3),
                newDatapoint("04", 4),
                newDatapoint("05", 5),
                newDatapoint("06", 6),
                newDatapoint("07", 7),
                newDatapoint("08", 8),
                newDatapoint("09", 9),
                newDatapoint("10", 10),
                newDatapoint(MetricName.valueOf("ex", "pected"), 9.9d));
    }

    @Test
    public void max() throws Exception {
        validateExpression("max(TEST *) = test ex.pected",
                newDatapoint("a", 4, 7, -2, 0),
                newDatapoint("b", 1, 9, -1, null),
                newDatapoint("c", 1, 11, -3, null),
                newDatapoint(MetricName.valueOf("ex", "pected"), 4, 11, -1, 0));
    }

    @Test
    public void min() throws Exception {
        validateExpression("min(TEST *) = test ex.pected",
                newDatapoint("a", 4, 7, -2, 0),
                newDatapoint("b", 1, 9, -1, null),
                newDatapoint("c", 1, 11, -3, null),
                newDatapoint(MetricName.valueOf("ex", "pected"), 1, 7, -3, 0));
    }

    @Test
    public void avg() throws Exception {
        validateExpression("avg(TEST *) = test ex.pected",
                newDatapoint("a", 4, 7, -2, 0),
                newDatapoint("b", 1, 9, -1, null),
                newDatapoint("c", 1, 11, -3, null),
                newDatapoint(MetricName.valueOf("ex", "pected"), 2d, 9d, -2d, 0));
    }

    @Test
    public void histogram_add() throws Exception {
        validateExpression("test a + test b = test expected",
                newDatapoint("a", new Histogram(new Histogram.RangeWithCount(0, 10, 10))),
                newDatapoint("b", new Histogram(new Histogram.RangeWithCount(0, 1, 10))),
                newDatapoint("expected", new Histogram(new Histogram.RangeWithCount(0, 1, 11), new Histogram.RangeWithCount(1, 10, 9))));
    }

    @Test
    public void histogram_subtract() throws Exception {
        validateExpression("test a - test b = test expected",
                newDatapoint("a", new Histogram(new Histogram.RangeWithCount(0, 1, 10))),
                newDatapoint("b", new Histogram(new Histogram.RangeWithCount(0, 1, 1))),
                newDatapoint("expected", new Histogram(new Histogram.RangeWithCount(0, 1, 9))));
    }

    @Test
    public void histogram_multiply_scalar() throws Exception {
        validateExpression("test a * test b = test expected",
                newDatapoint("a", new Histogram(new Histogram.RangeWithCount(0, 10, 10))),
                newDatapoint("b", 2),
                newDatapoint("expected", new Histogram(new Histogram.RangeWithCount(0, 10, 20))));
    }

    @Test
    public void histogram_divide_scalar() throws Exception {
        validateExpression("test a / test b = test expected",
                newDatapoint("a", new Histogram(new Histogram.RangeWithCount(0, 2, 10))),
                newDatapoint("b", 2),
                newDatapoint("expected", new Histogram(new Histogram.RangeWithCount(0, 2, 5))));
    }

    @Test
    public void histogram_add_literal() throws Exception {
        validateExpression("test a + [ 0..10=20, 0..10=4, 0.0..10=1 ] = test expected",
                newDatapoint("a", new Histogram(new Histogram.RangeWithCount(0, 2, 10))),
                newDatapoint("expected", new Histogram(new Histogram.RangeWithCount(0, 2, 15), new Histogram.RangeWithCount(2, 10, 20))));
    }
}
