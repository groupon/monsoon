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
package com.groupon.lex.metrics.timeseries;

import com.groupon.lex.metrics.GroupName;
import static java.util.Collections.EMPTY_MAP;
import java.util.Optional;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

/**
 *
 * @author ariane
 */
public class AlertTest {
    private static final GroupName alert_name = GroupName.valueOf("com", "groupon");
    private static final DateTime t0 = new DateTime(2015, 10, 21, 10, 10, 10, DateTimeZone.UTC);
    private static final DateTime t_past = new DateTime(2015, 10, 21, 10, 7, 37, DateTimeZone.UTC);
    private static final DateTime t_past_min5 = t_past.minus(Duration.standardMinutes(5));

    @Test
    public void constructor_ok() {
        Alert alert = new Alert(t0, alert_name, () -> "", Optional.of(false), Duration.ZERO, "test", EMPTY_MAP);

        assertEquals(t0, alert.getCur());
        assertEquals(t0, alert.getStart());
        assertEquals(alert_name, alert.getName());
        assertEquals(AlertState.OK, alert.getAlertState());
        assertEquals(false, alert.isFiring());
        assertEquals(Optional.of(false), alert.isTriggered());
    }

    @Test
    public void constructor_triggered() {
        Alert alert = new Alert(t0, alert_name, () -> "", Optional.of(true), Duration.standardMinutes(1), "test", EMPTY_MAP);

        assertEquals(t0, alert.getCur());
        assertEquals(t0, alert.getStart());
        assertEquals(alert_name, alert.getName());
        assertEquals(AlertState.TRIGGERING, alert.getAlertState());
        assertEquals(false, alert.isFiring());
        assertEquals(Optional.of(true), alert.isTriggered());
    }

    @Test
    public void constructor_unknown() {
        Alert alert = new Alert(t0, alert_name, () -> "", Optional.empty(), Duration.standardMinutes(1), "test", EMPTY_MAP);

        assertEquals(t0, alert.getCur());
        assertEquals(t0, alert.getStart());
        assertEquals(alert_name, alert.getName());
        assertEquals(AlertState.UNKNOWN, alert.getAlertState());
        assertEquals(false, alert.isFiring());
        assertEquals(Optional.empty(), alert.isTriggered());
    }

    @Test
    public void constructor_hairtrigger() {
        Alert alert = new Alert(t0, alert_name, () -> "", Optional.of(true), Duration.ZERO, "test", EMPTY_MAP);

        assertEquals(t0, alert.getCur());
        assertEquals(t0, alert.getStart());
        assertEquals(alert_name, alert.getName());
        assertEquals(AlertState.FIRING, alert.getAlertState());
        assertEquals(true, alert.isFiring());
        assertEquals(Optional.of(true), alert.isTriggered());
    }

    @Test
    public void extend_ok() {
        Alert past_alert = new Alert(t_past, alert_name, () -> "", Optional.of(false), Duration.standardMinutes(1), "test", EMPTY_MAP);
        Alert alert = past_alert.extend(new Alert(t0, alert_name, () -> "", Optional.of(false), Duration.standardMinutes(1), "test", EMPTY_MAP));

        assertEquals(Optional.of(false), alert.isTriggered());
        assertEquals(false, alert.isFiring());
        assertEquals("Return timestamp of first detected ok", t_past, alert.getStart());
        assertEquals("Return timestamp of last update", t0, alert.getCur());
    }

    @Test
    public void extend_triggered() {
        Alert past_alert = new Alert(t_past, alert_name, () -> "", Optional.of(true), Duration.standardDays(1), "test", EMPTY_MAP);
        Alert alert = past_alert.extend(new Alert(t0, alert_name, () -> "", Optional.of(true), Duration.standardDays(1), "test", EMPTY_MAP));

        assertEquals(Optional.of(true), alert.isTriggered());
        assertEquals(false, alert.isFiring());
        assertEquals("Return timestamp of first detected trigger", t_past, alert.getStart());
        assertEquals("Return timestamp of last update", t0, alert.getCur());
    }

    @Test
    public void extend_firing() {
        Alert past_alert = new Alert(t_past_min5, alert_name, () -> "", Optional.of(true), Duration.standardMinutes(1), "test", EMPTY_MAP)
                .extend(new Alert(t0, alert_name, () -> "", Optional.of(true), Duration.standardMinutes(1), "test", EMPTY_MAP));
        Alert alert = past_alert.extend(new Alert(t0, alert_name, () -> "", Optional.of(true), Duration.standardMinutes(1), "test", EMPTY_MAP));

        assertEquals(Optional.of(true), alert.isTriggered());
        assertEquals(true, alert.isFiring());
        assertEquals("Return timestamp of first detected trigger", t_past_min5, alert.getStart());
        assertEquals("Return timestamp of last update", t0, alert.getCur());
    }

    @Test
    public void extend_transition_triggered() {
        Alert past_alert = new Alert(t_past, alert_name, () -> "", Optional.of(false), Duration.standardMinutes(1), "test", EMPTY_MAP);
        Alert alert = past_alert.extend(new Alert(t0, alert_name, () -> "", Optional.of(true), Duration.standardMinutes(1), "test", EMPTY_MAP));

        assertEquals(Optional.of(true), alert.isTriggered());
        assertEquals(false, alert.isFiring());
        assertEquals("Return timestamp of first detected trigger", t0, alert.getStart());
        assertEquals("Return timestamp of last update", t0, alert.getCur());
    }

    @Test
    public void extend_transition_firing() {
        Alert past_alert = new Alert(t_past, alert_name, () -> "", Optional.of(true), Duration.standardMinutes(1), "test", EMPTY_MAP);
        Alert alert = past_alert.extend(new Alert(t0, alert_name, () -> "", Optional.of(true), Duration.standardMinutes(1), "test", EMPTY_MAP));

        assertEquals(Optional.of(true), alert.isTriggered());
        assertEquals(true, alert.isFiring());
        assertEquals("Return timestamp of first detected trigger", t_past, alert.getStart());
        assertEquals("Return timestamp of last update", t0, alert.getCur());
    }

    /**
     * Alerts are considered the same, if they have the same name.
     */
    @Test
    public void name_equality() {
        Alert alert1 = new Alert(t_past, alert_name, () -> "foo", Optional.of(true), Duration.standardDays(1), "test", EMPTY_MAP);
        Alert alert2 = new Alert(t0, alert_name, () -> "bar", Optional.of(false), Duration.standardDays(2), "test", EMPTY_MAP);

        assertEquals(alert1, alert2);
    }

    @Test
    public void predicate_string() {
        Alert alert = new Alert(t0, alert_name, () -> "bar", Optional.of(false), Duration.standardDays(2), "test", EMPTY_MAP);

        assertEquals("bar", alert.getRule());
    }
}
