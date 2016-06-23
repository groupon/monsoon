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
package com.groupon.lex.metrics;

import org.joda.time.Duration;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

/**
 *
 * @author ariane
 */
public class ConfigSupportDurationTest {
    @Test
    public void zero_seconds() {
        String s = ConfigSupport.durationConfigString(Duration.ZERO).toString();

        assertEquals("0s", s);
    }

    @Test
    public void one_second() {
        String s = ConfigSupport.durationConfigString(Duration.standardSeconds(1)).toString();

        assertEquals("1s", s);
    }

    @Test
    public void one_minute() {
        String s = ConfigSupport.durationConfigString(Duration.standardMinutes(1)).toString();

        assertEquals("1m", s);
    }

    @Test
    public void one_hour() {
        String s = ConfigSupport.durationConfigString(Duration.standardHours(1)).toString();

        assertEquals("1h", s);
    }

    @Test
    public void one_day() {
        String s = ConfigSupport.durationConfigString(Duration.standardDays(1)).toString();

        assertEquals("1d", s);
    }

    @Test
    public void one_of_each() {
        String s = ConfigSupport.durationConfigString(
                Duration.standardSeconds(1)
                        .plus(Duration.standardMinutes(2))
                        .plus(Duration.standardHours(3))
                        .plus(Duration.standardDays(4))
                ).toString();

        assertEquals("4d 3h 2m 1s", s);
    }

    @Test
    public void ignore_msec() {
        String s0 = ConfigSupport.durationConfigString(Duration.millis(100)).toString();
        String s1 = ConfigSupport.durationConfigString(Duration.millis(1100)).toString();

        assertEquals("0s", s0);
        assertEquals("1s", s1);
    }
}
