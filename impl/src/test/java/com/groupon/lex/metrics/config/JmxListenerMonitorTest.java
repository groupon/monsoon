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
package com.groupon.lex.metrics.config;

import com.groupon.lex.metrics.resolver.NameBoundResolver;
import static java.util.Collections.EMPTY_SET;
import static java.util.Collections.singleton;
import java.util.HashSet;
import java.util.Set;
import javax.management.ObjectName;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class JmxListenerMonitorTest {
    @Mock
    private NameBoundResolver nbr;

    private JmxListenerMonitor mon_noNames, mon_oneName, mon_twoNames;

    @Before
    public void setup() throws Exception {
        final Set<ObjectName> no_names = EMPTY_SET;
        final Set<ObjectName> one_name = singleton(new ObjectName("java.lang.*:*"));
        final Set<ObjectName> two_names = new HashSet<ObjectName>() {{
            add(new ObjectName("java.lang.*:*"));
            add(new ObjectName("com.groupon.*:*"));
        }};

        mon_noNames = new JmxListenerMonitor(no_names, nbr);
        mon_oneName = new JmxListenerMonitor(one_name, nbr);
        mon_twoNames = new JmxListenerMonitor(two_names, nbr);
    }

    @Test
    public void configStrings_noNames_noNBR() {
        when(nbr.isEmpty()).thenReturn(true);

        assertEquals("collect jmx_listener;\n",
                mon_noNames.configString().toString());

        verify(nbr, times(1)).isEmpty();
        verifyNoMoreInteractions(nbr);
    }

    @Test
    public void configStrings_oneName_noNBR() {
        when(nbr.isEmpty()).thenReturn(true);

        assertEquals("collect jmx_listener \"java.lang.*:*\";\n",
                mon_oneName.configString().toString());

        verify(nbr, times(1)).isEmpty();
        verifyNoMoreInteractions(nbr);
    }

    @Test
    public void configStrings_twoNames_noNBR() {
        when(nbr.isEmpty()).thenReturn(true);

        assertEquals("collect jmx_listener \"com.groupon.*:*\", \"java.lang.*:*\";\n",
                mon_twoNames.configString().toString());

        verify(nbr, times(1)).isEmpty();
        verifyNoMoreInteractions(nbr);
    }

    @Test
    public void configStrings_noNames_withNBR() {
        when(nbr.isEmpty()).thenReturn(false);
        when(nbr.configString()).thenReturn("{FOOBAR}");

        assertEquals("collect jmx_listener {FOOBAR}\n",
                mon_noNames.configString().toString());

        verify(nbr, times(1)).isEmpty();
        verify(nbr, times(1)).configString();
        verifyNoMoreInteractions(nbr);
    }

    @Test
    public void configStrings_oneName_withNBR() {
        when(nbr.isEmpty()).thenReturn(false);
        when(nbr.configString()).thenReturn("{FOOBAR}");

        assertEquals("collect jmx_listener \"java.lang.*:*\" {FOOBAR}\n",
                mon_oneName.configString().toString());

        verify(nbr, times(1)).isEmpty();
        verify(nbr, times(1)).configString();
        verifyNoMoreInteractions(nbr);
    }

    @Test
    public void configStrings_twoNames_withNBR() {
        when(nbr.isEmpty()).thenReturn(false);
        when(nbr.configString()).thenReturn("{FOOBAR}");

        assertEquals("collect jmx_listener \"com.groupon.*:*\", \"java.lang.*:*\" {FOOBAR}\n",
                mon_twoNames.configString().toString());

        verify(nbr, times(1)).isEmpty();
        verify(nbr, times(1)).configString();
        verifyNoMoreInteractions(nbr);
    }
}
