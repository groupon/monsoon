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
import com.groupon.lex.metrics.SimpleGroupPath;
import static com.groupon.lex.metrics.timeseries.AlertState.*;
import java.util.ArrayList;
import org.junit.Test;

/**
 *
 * @author ariane
 */
public class AlertTest extends AbstractAlertTest {
    private static final GroupName TESTGROUP = new GroupName("com", "groupon", "metrics", "test");

    @Test
    public void always_firing_alert() throws Exception {
        try (AlertValidator impl = replay("alert com.groupon.metrics.test if true;", 60)) {
            impl.validate(
                newDatapoint(TESTGROUP, FIRING, FIRING, FIRING, FIRING)
            );
        }
    }

    @Test
    public void trigger_alert() throws Exception {
        try (AlertValidator impl = replay("alert com.groupon.metrics.test if com.groupon.metrics.test b for 1m;", 60,
                newDatapoint(TESTGROUP, "b", false,  true,       true,   true))) {
            impl.validate(
                newDatapoint(TESTGROUP,      OK,     TRIGGERING, FIRING, FIRING));
        }
    }

    @Test
    public void recovering_alert() throws Exception {
        try (AlertValidator impl = replay("alert com.groupon.metrics.test if com.groupon.metrics.test b for 1m;", 60,
                newDatapoint(TESTGROUP, "b", false,  true,       true,   false))) {
            impl.validate(
                newDatapoint(TESTGROUP,      OK,     TRIGGERING, FIRING, OK));
        }
    }

    @Test
    public void alert_identifyer() throws Exception {
        /*
         * This test doesn't test if the alert performs correctly, but if the 'foobar' identifier is correctly accepted as an alert name.
         */
        try (AlertValidator impl = replay("alert foobar if true;", 60)) {
            impl.validate(
                newDatapoint(new GroupName("foobar"), FIRING));
        }
    }

    @Test
    public void alert_expr_identifier() throws Exception {
        /*
         * This test doesn't test if the alert performs correctly, but if the 'foobar' group is accepted as a group name.
         */
        try (AlertValidator impl = replay("alert test if foobar x;", 60,
                newDatapoint(new GroupName("foobar"), "x", true, null, false))) {
            impl.validate(
                newDatapoint(new GroupName("test"), FIRING, UNKNOWN, OK));
        }
    }

    @Test
    public void alert_match() throws Exception {
        try (AlertValidator impl = replay("match com.**.test as t { alert ${t} if t x; }", 60,
                newDatapoint(TESTGROUP,                    "x", false,  true),
                newDatapoint(new GroupName("com", "test"), "x", true,   false))) {
            impl.validate(
                newDatapoint(new GroupName("com", "test"),      FIRING, OK),
                newDatapoint(TESTGROUP,                         OK,     FIRING));
        }
    }

    @Test
    public void alert_match_subgroup() throws Exception {
        final GroupName EXPECTED_TESTGROUP = new GroupName(new SimpleGroupPath(new ArrayList<String>() {{
                    add("foo");
                    addAll(TESTGROUP.getPath().getPath());
                    add("bar");
                }}));

        try (AlertValidator impl = replay("match com.**.test as t { alert foo.${t}.bar if t x; }", 60,
                newDatapoint(TESTGROUP,                    "x", false,  true),
                newDatapoint(new GroupName("com", "test"), "x", true,   false))) {
            impl.validate(
                newDatapoint(new GroupName("foo", "com", "test", "bar"), FIRING, OK),
                newDatapoint(EXPECTED_TESTGROUP,                         OK,     FIRING));
        }
    }

    @Test
    public void alert_with_comment() throws Exception {
        try (AlertValidator impl = replay("alert too_many_restarts\n" +
                "if java.lang.Runtime Uptime <= 15 * 60 * 1000  # 15 minutes\n" +
                "for 20m;\n",
                60,
                newDatapoint(new GroupName("java", "lang", "Runtime"), "Uptime",
                         1 * 60 * 1000,  2 * 60 * 1000,  3 * 60 * 1000,  4 * 60 * 1000,  5 * 60 * 1000,
                         6 * 60 * 1000,  7 * 60 * 1000,  8 * 60 * 1000,  9 * 60 * 1000, 10 * 60 * 1000,
                        11 * 60 * 1000, 12 * 60 * 1000, 13 * 60 * 1000, 14 * 60 * 1000, 15 * 60 * 1000,
                        16 * 60 * 1000, 17 * 60 * 1000, 18 * 60 * 1000, 19 * 60 * 1000, 20 * 60 * 1000,
                        21 * 60 * 1000, 22 * 60 * 1000, 23 * 60 * 1000, 24 * 60 * 1000, 25 * 60 * 1000))) {
            impl.validate(newDatapoint(new GroupName("too_many_restarts"),
                    TRIGGERING, TRIGGERING, TRIGGERING, TRIGGERING, TRIGGERING,
                    TRIGGERING, TRIGGERING, TRIGGERING, TRIGGERING, TRIGGERING,
                    TRIGGERING, TRIGGERING, TRIGGERING, TRIGGERING, TRIGGERING,
                    OK,         OK,         OK,         OK,         OK,
                    OK,         OK,         OK,         OK,         OK));
        }
    }

    @Test
    public void alert_with_comment_and_restart() throws Exception {
        try (AlertValidator impl = replay("alert too_many_restarts\n" +
                "if java.lang.Runtime Uptime <= 15 * 60 * 1000  # 15 minutes\n" +
                "for 20m;\n",
                60,
                newDatapoint(new GroupName("java", "lang", "Runtime"), "Uptime",
                         1 * 60 * 1000,  2 * 60 * 1000,  3 * 60 * 1000,  4 * 60 * 1000,  5 * 60 * 1000,
                         6 * 60 * 1000,  7 * 60 * 1000,  8 * 60 * 1000,  9 * 60 * 1000, 10 * 60 * 1000,
                        11 * 60 * 1000, 12 * 60 * 1000, 13 * 60 * 1000, 14 * 60 * 1000, 15 * 60 * 1000,
                         1 * 60 * 1000,  2 * 60 * 1000,  3 * 60 * 1000,  4 * 60 * 1000,  5 * 60 * 1000,
                         6 * 60 * 1000,  7 * 60 * 1000,  8 * 60 * 1000,  9 * 60 * 1000, 10 * 60 * 1000))) {
            impl.validate(newDatapoint(new GroupName("too_many_restarts"),
                    TRIGGERING, TRIGGERING, TRIGGERING, TRIGGERING, TRIGGERING,
                    TRIGGERING, TRIGGERING, TRIGGERING, TRIGGERING, TRIGGERING,
                    TRIGGERING, TRIGGERING, TRIGGERING, TRIGGERING, TRIGGERING,
                    TRIGGERING, TRIGGERING, TRIGGERING, TRIGGERING, TRIGGERING,
                    FIRING,     FIRING,     FIRING,     FIRING,     FIRING));
        }
    }

    @Test
    public void alert_with_unknown_inbetween() throws Exception {
        try (AlertValidator impl = replay("alert test if test x for 3m;", 60,
                newDatapoint(new GroupName("test"), "x", true, true, true, true, null, null, true))) {
            impl.validate(newDatapoint(new GroupName("test"),
                    TRIGGERING, TRIGGERING, TRIGGERING, FIRING, UNKNOWN, UNKNOWN, TRIGGERING));
        }
    }

    @Test
    public void alert_with_unknown_before() throws Exception {
        try (AlertValidator impl = replay("alert test if test x for 3m;", 60,
                newDatapoint(new GroupName("test"), "x", null, null, null, true))) {
            impl.validate(newDatapoint(new GroupName("test"),
                    UNKNOWN, UNKNOWN, UNKNOWN, TRIGGERING));
        }
    }

    @Test
    public void alert_attribute_test() throws Exception {
        try (AlertValidator impl = replay("alert com.groupon.metrics.test if true attributes { foo = \"bar\", prime = 13 }", 60)) {
            impl.validate(
                newDatapoint(TESTGROUP, FIRING, FIRING, FIRING, FIRING)
            );
        }
    }
}
