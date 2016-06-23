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
import com.groupon.lex.metrics.MetricName;
import com.groupon.lex.metrics.MetricValue;
import com.groupon.lex.metrics.SimpleGroupPath;
import static com.groupon.lex.metrics.timeseries.AlertState.FIRING;
import static com.groupon.lex.metrics.timeseries.AlertState.OK;
import static com.groupon.lex.metrics.timeseries.AlertState.TRIGGERING;
import static com.groupon.lex.metrics.timeseries.AlertState.UNKNOWN;
import static java.util.Collections.singletonMap;
import java.util.HashMap;
import org.junit.Test;

/**
 *
 * @author ariane
 */
public class RuleTest extends AbstractAlertTest {
    private static final GroupName TESTGROUP = new GroupName("com", "groupon", "metrics", "test");

    @Test
    public void setTag_multiple_tags() throws Exception {
        final HashMap<String, MetricValue> expected_tags = new HashMap<String, MetricValue>() {{
            put("bool", MetricValue.TRUE);
            put("something", MetricValue.fromIntValue(7));
        }};

        try (AbstractAlertTest.AlertValidator impl = replay(
                  "tag " + TESTGROUP.configString() + " { bool = true, something = 7 }\n"
                + "alert test if "
                + "!tag(" + TESTGROUP.configString() + ", bool);", 60,
                newDatapoint(TESTGROUP, "a", 0, 1))) {
            impl.validate(newDatapoint(new GroupName(new SimpleGroupPath("test"), expected_tags),
                    OK, OK));
        }
    }

    @Test
    public void setTag_using_scalar() throws Exception {
        try (AbstractAlertTest.AlertValidator impl = replay(
                  "tag " + TESTGROUP.configString() + " as bool = true;\n"
                + "alert test if "
                + "!tag(" + TESTGROUP.configString() + ", bool);", 60,
                newDatapoint(TESTGROUP, "a", 0, 1))) {
            impl.validate(newDatapoint(new GroupName(new SimpleGroupPath("test"), singletonMap("bool", MetricValue.TRUE)),
                    OK, OK));
        }
    }

    @Test
    public void setTag_using_vector() throws Exception {
        try (AbstractAlertTest.AlertValidator impl = replay(
                  "tag " + TESTGROUP.configString() + " as bool = " + TESTGROUP.configString() + " a;\n"
                + "alert test if "
                + "!tag(" + TESTGROUP.configString() + ", bool);", 60,
                newDatapoint(TESTGROUP, "a", true, false, null))) {
            impl.validate(
                    newDatapoint(new GroupName(new SimpleGroupPath("test"), singletonMap("bool", MetricValue.TRUE)),
                        OK,      UNKNOWN, UNKNOWN),
                    newDatapoint(new GroupName(new SimpleGroupPath("test"), singletonMap("bool", MetricValue.FALSE)),
                        UNKNOWN, FIRING,  UNKNOWN));
        }
    }

    @Test
    public void setTag_to_same_value() throws Exception {
        try (AbstractAlertTest.AlertValidator impl = replay(
                  "tag " + TESTGROUP.configString() + " as bool = true;\n"
                + "alert test if "
                + "!tag(" + TESTGROUP.configString() + ", bool);", 60,
                newDatapoint(new GroupName(TESTGROUP.getPath(), singletonMap("bool", MetricValue.TRUE)), "a", true))) {
            impl.validate(
                    newDatapoint(new GroupName(new SimpleGroupPath("test"), singletonMap("bool", MetricValue.TRUE)),
                        OK));
        }
    }

    @Test
    public void setTag_overwriting() throws Exception {
        try (AbstractAlertTest.AlertValidator impl = replay(
                  "tag " + TESTGROUP.configString() + " as bool = true;\n"
                + "alert test if "
                + "!tag(" + TESTGROUP.configString() + ", bool);", 60,
                newDatapoint(new GroupName(TESTGROUP.getPath(), singletonMap("bool", MetricValue.FALSE)), "a", true))) {
            impl.validate(
                    newDatapoint(new GroupName(new SimpleGroupPath("test"), singletonMap("bool", MetricValue.TRUE)),
                        OK),
                    newDatapoint(new GroupName(new SimpleGroupPath("test"), singletonMap("bool", MetricValue.FALSE)),
                        UNKNOWN));
        }
    }

    @Test
    public void setTag_using_handler() throws Exception {
        try (AbstractAlertTest.AlertValidator impl = replay(
                  "alias " + TESTGROUP.configString() + " as Handler;\n"
                + "tag Handler as bool = true;\n"
                + "alert test if "
                + "!tag(" + TESTGROUP.configString() + ", bool);", 60,
                newDatapoint(new GroupName(TESTGROUP.getPath(), singletonMap("bool", MetricValue.FALSE)), "a", true))) {
            impl.validate(
                    newDatapoint(new GroupName(new SimpleGroupPath("test"), singletonMap("bool", MetricValue.TRUE)),
                        OK),
                    newDatapoint(new GroupName(new SimpleGroupPath("test"), singletonMap("bool", MetricValue.FALSE)),
                        UNKNOWN));
        }
    }

    @Test
    public void match() throws Exception {
        try (AbstractAlertTest.AlertValidator impl = replay(
                  "match com.** as M {\n"
                + "alert test if "
                + "M a;\n"
                + "}", 60,
                newDatapoint(TESTGROUP, "a", true, false, null))) {
            impl.validate(
                    newDatapoint(new GroupName("test"),
                        FIRING, OK, UNKNOWN));
        }
    }

    @Test
    public void match_metric_but_use_group() throws Exception {
        try (AbstractAlertTest.AlertValidator impl = replay(
                  "match ** a as M, A {\n"
                + "alert test if "
                + "M a;\n"
                + "}", 60,
                newDatapoint(TESTGROUP, "a", true, false, null))) {
            impl.validate(
                    newDatapoint(new GroupName("test"),
                        FIRING, OK, UNKNOWN));
        }
    }

    @Test
    public void match_metric() throws Exception {
        try (AbstractAlertTest.AlertValidator impl = replay(
                  "match ** a as M, A {\n"
                + "alert test if "
                + "A;\n"
                + "}", 60,
                newDatapoint(TESTGROUP, "a", true, false, null))) {
            impl.validate(
                    newDatapoint(new GroupName("test"),
                        FIRING, OK, UNKNOWN));
        }
    }

    @Test
    public void match_using_where() throws Exception {
        try (AbstractAlertTest.AlertValidator impl = replay(
                  "match com.** as M\n"
                + "where M a {\n"
                + "alert test if "
                + "M a;\n"
                + "}", 60,
                newDatapoint(TESTGROUP, "a", true, false, null))) {
            impl.validate(
                    newDatapoint(new GroupName("test"),
                        FIRING, UNKNOWN, UNKNOWN));
        }
    }

    @Test
    public void match_alias_follows_across_renames() throws Exception {
        try (AbstractAlertTest.AlertValidator impl = replay(
                  "match com.** as M\n"
                + "where M a {\n"
                + "tag M as tagname = \"tagvalue\";\n"
                + "alert test if "
                + "M a;\n"
                + "}", 60,
                newDatapoint(TESTGROUP, "a", true, false, null))) {
            impl.validate(
                    newDatapoint(new GroupName(new SimpleGroupPath("test"), singletonMap("tagname", MetricValue.fromStrValue("tagvalue"))),
                        FIRING, UNKNOWN, UNKNOWN));
        }
    }

    @Test
    public void binary_operation_by_tag_matcher() throws Exception {
        try (AbstractAlertTest.AlertValidator impl = replay(
                  "alert test.'plus'  if      " + TESTGROUP.configString() + " x + by(id) " + TESTGROUP.configString() + " y != " + TESTGROUP.configString() + " 'plus';\n"
                + "alert test.'minus' if      " + TESTGROUP.configString() + " x - by(id) " + TESTGROUP.configString() + " y != " + TESTGROUP.configString() + " 'minus';\n"
                + "alert test.'mul'   if      " + TESTGROUP.configString() + " x * by(id) " + TESTGROUP.configString() + " y != " + TESTGROUP.configString() + " 'mul';\n"
                + "alert test.'div'   if 10 * " + TESTGROUP.configString() + " x / by(id) " + TESTGROUP.configString() + " y != " + TESTGROUP.configString() + " 'div';\n"
                + "alert test.'mod'   if      " + TESTGROUP.configString() + " x % by(id) " + TESTGROUP.configString() + " y != " + TESTGROUP.configString() + " 'mod';\n", 60,
                newDatapoint(new GroupName(TESTGROUP.getPath(), singletonMap("id", MetricValue.fromIntValue(0))), "x",                1  ,  2  ,  3  ,  4  ,  5  ),
                newDatapoint(new GroupName(TESTGROUP.getPath(), singletonMap("id", MetricValue.fromIntValue(0))), "y",                2  ,  3  ,  4  ,  5  ,  6  ),
                newDatapoint(new GroupName(TESTGROUP.getPath(), singletonMap("id", MetricValue.fromIntValue(0))), "plus",             3  ,  5  ,  7  ,  9  , 11  ),
                newDatapoint(new GroupName(TESTGROUP.getPath(), singletonMap("id", MetricValue.fromIntValue(0))), "minus",           -1  , -1  , -1  , -1  , -1  ),
                newDatapoint(new GroupName(TESTGROUP.getPath(), singletonMap("id", MetricValue.fromIntValue(0))), "mul",              2  ,  6  , 12  , 20  , 30  ),
                newDatapoint(new GroupName(TESTGROUP.getPath(), singletonMap("id", MetricValue.fromIntValue(0))), "div",              5  ,  6  ,  7  ,  8  ,  8  ),
                newDatapoint(new GroupName(TESTGROUP.getPath(), singletonMap("id", MetricValue.fromIntValue(0))), "mod",              1  ,  2  ,  3  ,  4  ,  5  ))) {
            impl.validate(
                newDatapoint(new GroupName(new SimpleGroupPath("test", "plus" ), singletonMap("id", MetricValue.fromIntValue(0))),   OK  , OK  , OK  , OK  , OK  ),
                newDatapoint(new GroupName(new SimpleGroupPath("test", "minus"), singletonMap("id", MetricValue.fromIntValue(0))),   OK  , OK  , OK  , OK  , OK  ),
                newDatapoint(new GroupName(new SimpleGroupPath("test", "mul"  ), singletonMap("id", MetricValue.fromIntValue(0))),   OK  , OK  , OK  , OK  , OK  ),
                newDatapoint(new GroupName(new SimpleGroupPath("test", "div"  ), singletonMap("id", MetricValue.fromIntValue(0))),   OK  , OK  , OK  , OK  , OK  ),
                newDatapoint(new GroupName(new SimpleGroupPath("test", "mod"  ), singletonMap("id", MetricValue.fromIntValue(0))),   OK  , OK  , OK  , OK  , OK  ));
        }
    }

    @Test
    public void match_from_conveyor() throws Exception {
        try (AbstractAlertTest.AlertValidator impl = replay(
                  "match conveyor.master.avi gauge.//^.*\\.health_score\\.health_score$// as G, M {\n"
                + "    alert ${M[1:]}\n"
                + "    if M < 75\n"
                + "    for 5000m\n"
                + "    message \"foobar\";\n"
                + "}", 60,
                newDatapoint(new GroupName("conveyor", "master", "avi"), new MetricName("gauge", "pi.nginx-pool-80.health_score.health_score"), 100, 90, 80, 70, 60, 50, 40, 30, 20, 10, 0))) {
            impl.validate(
                    newDatapoint(new GroupName("pi.nginx-pool-80.health_score.health_score"),
                        OK, OK, OK, TRIGGERING, TRIGGERING, TRIGGERING, TRIGGERING, TRIGGERING, TRIGGERING, TRIGGERING, TRIGGERING));
        }
    }
}
