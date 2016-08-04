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

import com.groupon.lex.metrics.GroupName;
import com.groupon.lex.metrics.MetricValue;
import com.groupon.lex.metrics.SimpleGroupPath;
import com.groupon.lex.metrics.Tags;
import static com.groupon.lex.metrics.timeseries.AlertState.FIRING;
import static com.groupon.lex.metrics.timeseries.AlertState.OK;
import static com.groupon.lex.metrics.timeseries.AlertState.UNKNOWN;
import java.util.Collection;
import static java.util.Collections.singletonMap;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.Test;

/**
 *
 * @author ariane
 */
public class FunctionTest extends AbstractAlertTest {
    private static final GroupName TESTGROUP = GroupName.valueOf("com", "groupon", "metrics", "test");
    private static final GroupName testgroup_idx_(int idx) {
        return GroupName.valueOf(Stream.concat(TESTGROUP.getPath().getPath().stream(), Stream.of(String.valueOf(idx), "indexed"))
                .toArray(String[]::new));
    }

    @Test
    public void rate() throws Exception {
        /*
         * We're comparing for rate between upper and lower bounds, since doubles are really hard to compare for equality.
         */
        try (AbstractAlertTest.AlertValidator impl = replay("alert test if "
                + "rate(" + TESTGROUP.configString() + " a) < " + TESTGROUP.configString() + " lowerbound ||"
                + "rate(" + TESTGROUP.configString() + " a) > " + TESTGROUP.configString() + " upperbound;", 60,
                newDatapoint(TESTGROUP, "a",            0,          1,          2, null,   3,          5),
                newDatapoint(TESTGROUP, "lowerbound", -1D, 0.99*1/60D, 0.99*1/60D,  -1D, -1D, 0.99*2/60D),
                newDatapoint(TESTGROUP, "upperbound", -1D, 1.01*1/60D, 1.01*1/60D,  -1D, -1D, 1.01*2/60D))) {
            impl.validate(newDatapoint(GroupName.valueOf("test"),
                    UNKNOWN, OK, OK, UNKNOWN, UNKNOWN, OK));
        }
    }

    @Test
    public void rate_allows_negative() throws Exception {
        try (AbstractAlertTest.AlertValidator impl = replay(
                "define " + TESTGROUP.configString() + " rate_of_a = rate(" + TESTGROUP.configString() + " a);"
                + "alert test if "
                + "rate(" + TESTGROUP.configString() + " a) < 0;", 60,
                newDatapoint(TESTGROUP, "a", 0, 1000, 100))) {
            impl.validate(newDatapoint(GroupName.valueOf("test"),
                    UNKNOWN, OK, FIRING));
        }
    }

    @Test
    public void sum_direct() throws Exception {
        final String QUERY = "alert test if "
                + "sum("
                + testgroup_idx_(0).configString() + " a, "
                + testgroup_idx_(1).configString() + " a, "
                + testgroup_idx_(2).configString() + " a) != " + TESTGROUP.configString() + " expected;";
        try (AbstractAlertTest.AlertValidator impl = replay(QUERY, 60,
                newDatapoint(testgroup_idx_(0), "a", 0, 0, 12, 1, null),
                newDatapoint(testgroup_idx_(1), "a", 0, 1, 13, 1,    0),
                newDatapoint(testgroup_idx_(2), "a", 0, 2, 17, 1,    0),
                newDatapoint(TESTGROUP, "expected",  0, 3, 42, 1,    0))) {
            impl.validate(newDatapoint(GroupName.valueOf("test"),
                    OK, OK, OK, FIRING, OK));
        }
    }

    @Test
    public void sum_matched() throws Exception {
        try (AbstractAlertTest.AlertValidator impl = replay("alert test if "
                + "sum(" + TESTGROUP.configString() + ".*.* a) != " + TESTGROUP.configString() + " expected;", 60,
                newDatapoint(testgroup_idx_(0), "a", 0, 0, 12, 1, null),
                newDatapoint(testgroup_idx_(1), "a", 0, 1, 13, 1,    0),
                newDatapoint(testgroup_idx_(2), "a", 0, 2, 17, 1,    0),
                newDatapoint(TESTGROUP, "expected",  0, 3, 42, 1,    0))) {
            impl.validate(newDatapoint(GroupName.valueOf("test"),
                    OK, OK, OK, FIRING, OK));
        }
    }

    @Test
    public void sum_metric_matched() throws Exception {
        try (AbstractAlertTest.AlertValidator impl = replay("alert test if "
                + "sum(" + testgroup_idx_(0).configString() + " *) != " + TESTGROUP.configString() + " expected;", 60,
                newDatapoint(testgroup_idx_(0), "a", 0, 0, 12, 1, null),
                newDatapoint(testgroup_idx_(0), "b", 0, 1, 13, 1,    0),
                newDatapoint(testgroup_idx_(0), "c", 0, 2, 17, 1,    0),
                newDatapoint(TESTGROUP, "expected",  0, 3, 42, 1,    0))) {
            impl.validate(newDatapoint(GroupName.valueOf("test"),
                    OK, OK, OK, FIRING, OK));
        }
    }

    @Test
    public void sum_matched_using_double_slash() throws Exception {
        try (AbstractAlertTest.AlertValidator impl = replay("alert test if "
                + "sum(" + TESTGROUP.configString() + ".//^0|2$//.//dexed$// a) != " + TESTGROUP.configString() + " expected;", 60,
                newDatapoint(testgroup_idx_(0), "a", 0, 0, 12, 1, null),
                newDatapoint(testgroup_idx_(1), "a", 0, 1, 13, 1,    0),
                newDatapoint(testgroup_idx_(2), "a", 0, 2, 17, 1,    0),
                newDatapoint(TESTGROUP, "expected",  0, 2, 29, 1,    0))) {
            impl.validate(newDatapoint(GroupName.valueOf("test"),
                    OK, OK, OK, FIRING, OK));
        }
    }

    @Test
    public void sum_matched_with_floats() throws Exception {
        try (AbstractAlertTest.AlertValidator impl = replay("alert test if "
                + "sum(" + TESTGROUP.configString() + ".*.* a) != " + TESTGROUP.configString() + " expected;", 60,
                newDatapoint(testgroup_idx_(0), "a", 0.0,   0, 12.0,   1, null),
                newDatapoint(testgroup_idx_(1), "a", 0.0, 1.0, 13.0, 1.0,  0.0),
                newDatapoint(testgroup_idx_(2), "a",   0, 2.0, 17.0,   1,    0),
                newDatapoint(TESTGROUP, "expected",  0.0, 3.0, 42.0, 1.0,  0.0))) {
            impl.validate(newDatapoint(GroupName.valueOf("test"),
                    OK, OK, OK, FIRING, OK));
        }
    }

    @Test
    public void sum_tagged() throws Exception {
        GroupName g_a = GroupName.valueOf(TESTGROUP.getPath(),
                Stream.of(singletonMap("host", MetricValue.fromStrValue("localhost")), singletonMap("id", MetricValue.fromStrValue("a"))).map(Map::entrySet).flatMap(Collection::stream));
        GroupName g_b = GroupName.valueOf(TESTGROUP.getPath(),
                Stream.of(singletonMap("host", MetricValue.fromStrValue("localhost")), singletonMap("id", MetricValue.fromStrValue("b"))).map(Map::entrySet).flatMap(Collection::stream));

        try (AbstractAlertTest.AlertValidator impl = replay("alert host if "
                + "sum(" + TESTGROUP.configString() + " a) by (host) != 14;", 60,
                newDatapoint(g_a, "a", 0.0, 1.0, 2.0, 3.0, 4.0, 5.0),
                newDatapoint(g_b, "a",  14,  13,  12,   6,   4,   2))) {
            impl.validate(newDatapoint(GroupName.valueOf(SimpleGroupPath.valueOf("host"), singletonMap("host", MetricValue.fromStrValue("localhost"))),
                    OK, OK, OK, FIRING, FIRING, FIRING));
        }
    }

    @Test
    public void sum_grouping_tagged() throws Exception {
        GroupName l_a = GroupName.valueOf(TESTGROUP.getPath(),
                Stream.of(singletonMap("host", MetricValue.fromStrValue("localhost")), singletonMap("x", MetricValue.fromIntValue(1)), singletonMap("id", MetricValue.fromStrValue("a"))).map(Map::entrySet).flatMap(Collection::stream));
        GroupName l_b = GroupName.valueOf(TESTGROUP.getPath(),
                Stream.of(singletonMap("host", MetricValue.fromStrValue("localhost")), singletonMap("x", MetricValue.fromIntValue(1)), singletonMap("id", MetricValue.fromStrValue("b"))).map(Map::entrySet).flatMap(Collection::stream));
        GroupName r_a = GroupName.valueOf(TESTGROUP.getPath(),
                Stream.of(singletonMap("host", MetricValue.fromStrValue("otherhost")), singletonMap("x", MetricValue.fromIntValue(1)), singletonMap("id", MetricValue.fromStrValue("a"))).map(Map::entrySet).flatMap(Collection::stream));
        GroupName r_b = GroupName.valueOf(TESTGROUP.getPath(),
                Stream.of(singletonMap("host", MetricValue.fromStrValue("otherhost")), singletonMap("x", MetricValue.fromIntValue(1)), singletonMap("id", MetricValue.fromStrValue("b"))).map(Map::entrySet).flatMap(Collection::stream));

        try (AbstractAlertTest.AlertValidator impl = replay("alert host if "
                + "sum(" + TESTGROUP.configString() + " a) by (host) != 14;", 60,
                newDatapoint(l_a, "a", 0.0, 1.0, 2.0, 3.0, 4.0, 5.0),
                newDatapoint(l_b, "a",  14,  13,  12,   6,   4,   2),
                newDatapoint(r_a, "a", 0.0, 1.0, 2.0, 3.0, 4.0, 5.0),
                newDatapoint(r_b, "a",  14,  13,  12,   6,   4,   2))) {
            impl.validate(
                    newDatapoint(GroupName.valueOf(SimpleGroupPath.valueOf("host"), singletonMap("host", MetricValue.fromStrValue("localhost"))),
                    OK, OK, OK, FIRING, FIRING, FIRING),
                    newDatapoint(GroupName.valueOf(SimpleGroupPath.valueOf("host"), singletonMap("host", MetricValue.fromStrValue("otherhost"))),
                    OK, OK, OK, FIRING, FIRING, FIRING));
        }
    }

    @Test
    public void sum_grouping_tagged_keep_common() throws Exception {
        GroupName l_a = GroupName.valueOf(TESTGROUP.getPath(),
                Stream.of(singletonMap("host", MetricValue.fromStrValue("localhost")), singletonMap("x", MetricValue.fromIntValue(1)), singletonMap("id", MetricValue.fromStrValue("a"))).map(Map::entrySet).flatMap(Collection::stream));
        GroupName l_b = GroupName.valueOf(TESTGROUP.getPath(),
                Stream.of(singletonMap("host", MetricValue.fromStrValue("localhost")), singletonMap("x", MetricValue.fromIntValue(1)), singletonMap("id", MetricValue.fromStrValue("b"))).map(Map::entrySet).flatMap(Collection::stream));
        GroupName r_a = GroupName.valueOf(TESTGROUP.getPath(),
                Stream.of(singletonMap("host", MetricValue.fromStrValue("otherhost")), singletonMap("x", MetricValue.fromIntValue(1)), singletonMap("id", MetricValue.fromStrValue("a"))).map(Map::entrySet).flatMap(Collection::stream));
        GroupName r_b = GroupName.valueOf(TESTGROUP.getPath(),
                Stream.of(singletonMap("host", MetricValue.fromStrValue("otherhost")), singletonMap("x", MetricValue.fromIntValue(1)), singletonMap("id", MetricValue.fromStrValue("b"))).map(Map::entrySet).flatMap(Collection::stream));

        try (AbstractAlertTest.AlertValidator impl = replay("alert host if "
                + "sum(" + TESTGROUP.configString() + " a) by (host) keep_common != 14;", 60,
                newDatapoint(l_a, "a", 0.0, 1.0, 2.0, 3.0, 4.0, 5.0),
                newDatapoint(l_b, "a",  14,  13,  12,   6,   4,   2),
                newDatapoint(r_a, "a", 0.0, 1.0, 2.0, 3.0, 4.0, 5.0),
                newDatapoint(r_b, "a",  14,  13,  12,   6,   4,   2))) {
            impl.validate(
                    newDatapoint(GroupName.valueOf(SimpleGroupPath.valueOf("host"), Stream.of(singletonMap("host", MetricValue.fromStrValue("localhost")), singletonMap("x", MetricValue.fromIntValue(1))).map(Map::entrySet).flatMap(Collection::stream)),
                    OK, OK, OK, FIRING, FIRING, FIRING),
                    newDatapoint(GroupName.valueOf(SimpleGroupPath.valueOf("host"), Stream.of(singletonMap("host", MetricValue.fromStrValue("otherhost")), singletonMap("x", MetricValue.fromIntValue(1))).map(Map::entrySet).flatMap(Collection::stream)),
                    OK, OK, OK, FIRING, FIRING, FIRING));
        }
    }

    @Test
    public void count_direct() throws Exception {
        final String QUERY = "alert test if "
                + "count("
                + testgroup_idx_(0).configString() + " a, "
                + testgroup_idx_(1).configString() + " a, "
                + testgroup_idx_(2).configString() + " a) != " + TESTGROUP.configString() + " expected;";
        try (AbstractAlertTest.AlertValidator impl = replay(QUERY, 60,
                newDatapoint(testgroup_idx_(0), "a", 0, 0, 12, 1, null, null),
                newDatapoint(testgroup_idx_(1), "a", 0, 1, 13, 1,    0, null),
                newDatapoint(testgroup_idx_(2), "a", 0, 2, 17, 1,    0, null),
                newDatapoint(TESTGROUP, "expected",  3, 3,  3, 3,    2,    0))) {
            impl.validate(newDatapoint(GroupName.valueOf("test"),
                    OK, OK, OK, OK, OK, OK));
        }
    }

    @Test
    public void count_matched() throws Exception {
        try (AbstractAlertTest.AlertValidator impl = replay("alert test if "
                + "count(" + TESTGROUP.configString() + ".*.* a) != " + TESTGROUP.configString() + " expected;", 60,
                newDatapoint(testgroup_idx_(0), "a", 0, 0, 12, 1, null, null),
                newDatapoint(testgroup_idx_(1), "a", 0, 1, 13, 1,    0, null),
                newDatapoint(testgroup_idx_(2), "a", 0, 2, 17, 1,    0, null),
                newDatapoint(TESTGROUP, "expected",  3, 3,  3, 3,    2,    0))) {
            impl.validate(newDatapoint(GroupName.valueOf("test"),
                    OK, OK, OK, OK, OK, OK));
        }
    }

    @Test
    public void tag_value_using_group() throws Exception {
        final String S = "localhost";
        final Tags tags = Tags.valueOf(singletonMap("host", MetricValue.fromStrValue(S)));
        final GroupName testgroup = GroupName.valueOf(TESTGROUP.getPath(), tags);

        try (AbstractAlertTest.AlertValidator impl = replay("alert test if "
                + "tag(" + TESTGROUP.configString() + ", 'host') != " + TESTGROUP.configString() + " expected;", 60,
                newDatapoint(testgroup, "a",         0, 0, 12, 1, null, null),
                newDatapoint(testgroup, "expected",  S, S,  S, S,    S,    S))) {
            impl.validate(newDatapoint(GroupName.valueOf(SimpleGroupPath.valueOf("test"), tags),
                    OK, OK, OK, OK, OK, OK));
        }
    }

    @Test
    public void tag_value_using_expr() throws Exception {
        final String S = "localhost";
        final Tags tags = Tags.valueOf(singletonMap("host", MetricValue.fromStrValue(S)));
        final GroupName testgroup = GroupName.valueOf(TESTGROUP.getPath(), tags);

        try (AbstractAlertTest.AlertValidator impl = replay("alert test if "
                + "tag(" + TESTGROUP.configString() + " a + 1, 'host') != " + TESTGROUP.configString() + " expected;", 60,
                newDatapoint(testgroup, "a",         0, 0, 12, 1, null, null),
                newDatapoint(testgroup, "expected",  S, S,  S, S,    S,    S))) {
            impl.validate(newDatapoint(GroupName.valueOf(SimpleGroupPath.valueOf("test"), tags),
                    OK, OK, OK, OK, UNKNOWN, UNKNOWN));
        }
    }

    @Test
    public void regexp_expr() throws Exception {
        try (AbstractAlertTest.AlertValidator impl = replay("alert test if "
                + "regexp(" + TESTGROUP.configString() + " a, \"(o+)\", \"$1\") != " + TESTGROUP.configString() + " expected;", 60,
                newDatapoint(TESTGROUP, "a",        "foo", "oooooooo!", "abracadabra", "lao", "oo o", "o oo", null),
                newDatapoint(TESTGROUP, "expected",  "oo", "oooooooo",  "",              "o", "oo",   "o",    ""))) {
            impl.validate(newDatapoint(GroupName.valueOf("test"),
                    OK, OK, UNKNOWN, OK, OK, OK, UNKNOWN));
        }
    }

    @Test
    public void regexp_expr_using_double_slash() throws Exception {
        try (AbstractAlertTest.AlertValidator impl = replay("alert test if "
                + "regexp(" + TESTGROUP.configString() + " a, //(o+)//, \"$1\") != " + TESTGROUP.configString() + " expected;", 60,
                newDatapoint(TESTGROUP, "a",        "foo", "oooooooo!", "abracadabra", "lao", "oo o", "o oo", null),
                newDatapoint(TESTGROUP, "expected",  "oo", "oooooooo",  "",              "o", "oo",   "o",    ""))) {
            impl.validate(newDatapoint(GroupName.valueOf("test"),
                    OK, OK, UNKNOWN, OK, OK, OK, UNKNOWN));
        }
    }

    @Test
    public void regexp_expr_with_begin_match() throws Exception {
        try (AbstractAlertTest.AlertValidator impl = replay("alert test if "
                + "regexp(" + TESTGROUP.configString() + " a, \"^(o+)\", \"$1\") != " + TESTGROUP.configString() + " expected;", 60,
                newDatapoint(TESTGROUP, "a",        "foo", "oooooooo!", "abracadabra", "lao", "oo o", "o oo", null),
                newDatapoint(TESTGROUP, "expected",  "oo", "oooooooo",  "",              "o", "oo",   "o",    ""))) {
            impl.validate(newDatapoint(GroupName.valueOf("test"),
                    UNKNOWN, OK, UNKNOWN, UNKNOWN, OK, OK, UNKNOWN));
        }
    }

    @Test
    public void regexp_expr_with_begin_match_using_double_slash() throws Exception {
        try (AbstractAlertTest.AlertValidator impl = replay("alert test if "
                + "regexp(" + TESTGROUP.configString() + " a, //^(o+)//, \"$1\") != " + TESTGROUP.configString() + " expected;", 60,
                newDatapoint(TESTGROUP, "a",        "foo", "oooooooo!", "abracadabra", "lao", "oo o", "o oo", null),
                newDatapoint(TESTGROUP, "expected",  "oo", "oooooooo",  "",              "o", "oo",   "o",    ""))) {
            impl.validate(newDatapoint(GroupName.valueOf("test"),
                    UNKNOWN, OK, UNKNOWN, UNKNOWN, OK, OK, UNKNOWN));
        }
    }

    @Test
    public void regexp_expr_with_end_match() throws Exception {
        try (AbstractAlertTest.AlertValidator impl = replay("alert test if "
                + "regexp(" + TESTGROUP.configString() + " a, \"(o+)$\", \"$1\") != " + TESTGROUP.configString() + " expected;", 60,
                newDatapoint(TESTGROUP, "a",        "foo", "oooooooo!", "abracadabra", "lao", "oo o", "o oo", null),
                newDatapoint(TESTGROUP, "expected",  "oo", "oooooooo",  "",              "o",    "o",   "oo",   ""))) {
            impl.validate(newDatapoint(GroupName.valueOf("test"),
                    OK, UNKNOWN, UNKNOWN, OK, OK, OK, UNKNOWN));
        }
    }

    @Test
    public void regexp_expr_with_end_match_using_double_slash() throws Exception {
        try (AbstractAlertTest.AlertValidator impl = replay("alert test if "
                + "regexp(" + TESTGROUP.configString() + " a, //(o+)$//, \"$1\") != " + TESTGROUP.configString() + " expected;", 60,
                newDatapoint(TESTGROUP, "a",        "foo", "oooooooo!", "abracadabra", "lao", "oo o", "o oo", null),
                newDatapoint(TESTGROUP, "expected",  "oo", "oooooooo",  "",              "o",    "o",   "oo",   ""))) {
            impl.validate(newDatapoint(GroupName.valueOf("test"),
                    OK, UNKNOWN, UNKNOWN, OK, OK, OK, UNKNOWN));
        }
    }

    @Test
    public void regexp_expr_with_begin_and_end_match() throws Exception {
        try (AbstractAlertTest.AlertValidator impl = replay("alert test if "
                + "regexp(" + TESTGROUP.configString() + " a, \"^(o+)$\", \"$1\") != " + TESTGROUP.configString() + " expected;", 60,
                newDatapoint(TESTGROUP, "a",        "foo", "oooooooo!", "abracadabra", "lao", "oo o", "o oo", null, "ooo"),
                newDatapoint(TESTGROUP, "expected",  "oo", "oooooooo",  "",              "o",    "o",   "oo",   "", "ooo"))) {
            impl.validate(newDatapoint(GroupName.valueOf("test"),
                    UNKNOWN, UNKNOWN, UNKNOWN, UNKNOWN, UNKNOWN, UNKNOWN, UNKNOWN, OK));
        }
    }

    @Test
    public void str_expr() throws Exception {
        try (AbstractAlertTest.AlertValidator impl = replay("alert test if "
                + "str(" + TESTGROUP.configString() + " a, \"-\", " + TESTGROUP.configString() + " b) != " + TESTGROUP.configString() + " expected;", 60,
                newDatapoint(TESTGROUP, "a",            1,     2,     3,  null,     5),
                newDatapoint(TESTGROUP, "b",          "a",   "b",   "c",   "d",  null),
                newDatapoint(TESTGROUP, "expected", "1-a", "2-b", "3-c", "4-d", "5-e"))) {
            impl.validate(newDatapoint(GroupName.valueOf("test"),
                    OK, OK, OK, UNKNOWN, UNKNOWN));
        }
    }

    @Test
    public void name_expr() throws Exception {
        try (AbstractAlertTest.AlertValidator impl = replay("alert test if "
                + "name(" + TESTGROUP.configString() + ") != " + TESTGROUP.configString() + " expected;", 60,
                newDatapoint(TESTGROUP, "expected", String.join(".", TESTGROUP.getPath().getPath())))) {
            impl.validate(newDatapoint(GroupName.valueOf("test"),
                    OK));
        }
    }

    @Test
    public void name_indirect() throws Exception {
        final GroupName groupName0 = GroupName.valueOf("com", "groupon", "metrics", "17");  // G[3] = 17
        final GroupName groupName1 = GroupName.valueOf("com", "groupon", "metrics", "true");  // G[3] = true
        final GroupName groupName2 = GroupName.valueOf("com", "groupon", "metrics", "false");  // G[3] = false
        final GroupName groupName3 = GroupName.valueOf("com", "groupon", "metrics", "3e4");  // G[3] = 3e4
        final GroupName groupName4 = GroupName.valueOf("com", "groupon", "metrics", "just a string");  // G[3] = "just a string"

        try (AbstractAlertTest.AlertValidator impl = replay(
                  "match com.groupon.metrics.* as G {\n"
                + "    alert ${G[2:]}\n"
                + "    if name(${G[2:]}) != G expected;\n"
                + "\n"
                + "    alert shortname.${G[3]}\n"
                + "    if name(${G[3]}) != G shortname;\n"
                + "}\n", 60,
                newDatapoint(groupName0, "expected", String.join(".", groupName0.getPath().getPath().subList(2, 4))),
                newDatapoint(groupName1, "expected", String.join(".", groupName1.getPath().getPath().subList(2, 4))),
                newDatapoint(groupName2, "expected", String.join(".", groupName2.getPath().getPath().subList(2, 4))),
                newDatapoint(groupName3, "expected", String.join(".", groupName3.getPath().getPath().subList(2, 4))),
                newDatapoint(groupName4, "expected", String.join(".", groupName4.getPath().getPath().subList(2, 4))),
                newDatapoint(groupName0, "shortname", 17),
                newDatapoint(groupName1, "shortname", true),
                newDatapoint(groupName2, "shortname", false),
                newDatapoint(groupName3, "shortname", 3e4),
                newDatapoint(groupName4, "shortname", "just a string")
        )) {
            impl.validate(
                    newDatapoint(GroupName.valueOf("metrics",   "17"),            OK),
                    newDatapoint(GroupName.valueOf("metrics",   "true"),          OK),
                    newDatapoint(GroupName.valueOf("metrics",   "false"),         OK),
                    newDatapoint(GroupName.valueOf("metrics",   "3e4"),           OK),
                    newDatapoint(GroupName.valueOf("metrics",   "just a string"), OK),
                    newDatapoint(GroupName.valueOf("shortname", "17"),            OK),
                    newDatapoint(GroupName.valueOf("shortname", "true"),          OK),
                    newDatapoint(GroupName.valueOf("shortname", "false"),         OK),
                    newDatapoint(GroupName.valueOf("shortname", "3e4"),           OK),
                    newDatapoint(GroupName.valueOf("shortname", "just a string"), OK));
        }
    }
}
