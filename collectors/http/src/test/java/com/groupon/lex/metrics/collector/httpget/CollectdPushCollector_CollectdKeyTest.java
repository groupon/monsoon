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
package com.groupon.lex.metrics.collector.httpget;

import com.groupon.lex.metrics.lib.Any2;
import java.util.HashMap;
import java.util.Map;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

/**
 *
 * @author ariane
 */
public class CollectdPushCollector_CollectdKeyTest {
    @Test
    public void equality() {
        CollectdPushCollector.CollectdKey msg1 = new CollectdPushCollector.CollectdKey("otherhost", "plugin", "instance-0", "type", "type-0");
        CollectdPushCollector.CollectdKey msg2 = new CollectdPushCollector.CollectdKey("otherhost", "plugin", "instance-0", "type", "type-0");

        assertEquals(msg1.hashCode(), msg2.hashCode());
        assertTrue(msg1.equals(msg2));
    }

    @Test
    public void inequality() {
        CollectdPushCollector.CollectdKey msg = new CollectdPushCollector.CollectdKey("otherhost", "plugin", "instance-0", "type", "type-0");
        CollectdPushCollector.CollectdKey msg_host = new CollectdPushCollector.CollectdKey("localhost", "plugin", "instance-0", "type", "type-0");
        CollectdPushCollector.CollectdKey msg_plugin = new CollectdPushCollector.CollectdKey("otherhost", "otherplugin", "instance-0", "type", "type-0");
        CollectdPushCollector.CollectdKey msg_plugin_instance = new CollectdPushCollector.CollectdKey("otherhost", "plugin", "other-instance-0", "type", "type-0");
        CollectdPushCollector.CollectdKey msg_type = new CollectdPushCollector.CollectdKey("otherhost", "plugin", "instance-0", "other-type", "type-0");
        CollectdPushCollector.CollectdKey msg_type_instance = new CollectdPushCollector.CollectdKey("otherhost", "plugin", "instance-0", "type", "other-type-0");
        CollectdPushCollector.CollectdKey msg_tags = new CollectdPushCollector.CollectdKey("otherhost", "plugin", "instance-0[a=a]", "type", "type-0");

        assertFalse(msg.equals(msg_host));
        assertFalse(msg.equals(msg_plugin));
        assertFalse(msg.equals(msg_plugin_instance));
        assertFalse(msg.equals(msg_type));
        assertFalse(msg.equals(msg_type_instance));
        assertFalse(msg.equals(msg_tags));
        assertFalse(msg.equals(new Object()));
        assertFalse(msg.equals(null));
    }

    @Test
    public void tagExtraction() {
        final Map<String, Any2<String, Number>> expected = new HashMap<String, Any2<String, Number>>() {{
            put("human_status", Any2.left("ok"));
            put("plugin_data", Any2.left("_c0"));
            put("name", Any2.left("LSI 3108 MegaRAID PCI-E 00:04:00:00 ver: 24.7.0-0026"));
            put("id", Any2.left("FW-AE85RGMAARBWA"));
        }};
        CollectdPushCollector.CollectdKey msg = new CollectdPushCollector.CollectdKey(
                "localhost",
                "raid",
                "cards[human_status=ok,plugin_data=_c0,name=LSI 3108 MegaRAID PCI-E 00:04:00:00 ver: 24.7.0-0026,id=FW-AE85RGMAARBWA]",
                "gauge",
                "status");

        assertEquals("localhost", msg.host);
        assertEquals("raid", msg.plugin);
        assertEquals("cards", msg.plugin_instance);
        assertEquals("gauge", msg.type);
        assertEquals("status", msg.type_instance);
        assertEquals(expected, msg.tags);
    }

    @Test
    public void quotedStringTagExtraction() {
        final Map<String, Any2<String, Number>> expected = new HashMap<String, Any2<String, Number>>() {{
            put("a", Any2.left("7"));
            put("7", Any2.left("a"));
        }};
        CollectdPushCollector.CollectdKey msg = new CollectdPushCollector.CollectdKey(
                "localhost",
                "plugin",
                "plugin_instance[\"a\"=\"7\",\"7\"=\"a\"]",
                "type",
                "type_instance");

        assertEquals(expected, msg.tags);
    }

    @Test
    public void numericTagExtraction() {
        final Map<String, Any2<String, Number>> expected = new HashMap<String, Any2<String, Number>>() {{
            put("a", Any2.right(7l));
            put("b", Any2.right(3.14d));
            put("c", Any2.right(7e1d));
            put("d", Any2.right(-8e-4d));
            put("e", Any2.right(-7l));
        }};
        CollectdPushCollector.CollectdKey msg = new CollectdPushCollector.CollectdKey(
                "localhost",
                "plugin",
                "plugin_instance[a=7,b=3.14,c=7e1,d=-8e-4,e=-7]",
                "type",
                "type_instance");

        assertEquals(expected, msg.tags);
    }

    @Test(expected=Exception.class)
    public void numericKeysAreQuotedOrFail() {
        CollectdPushCollector.CollectdKey msg = new CollectdPushCollector.CollectdKey(
                "localhost",
                "plugin",
                "plugin_instance[7=a]",
                "type",
                "type_instance");
    }

    @Test
    public void tagCommaIsNotKey() {
        final Map<String, Any2<String, Number>> expected = new HashMap<String, Any2<String, Number>>() {{
            put("a", Any2.left("b,c"));
            put("d", Any2.right(4l));
        }};
        CollectdPushCollector.CollectdKey msg = new CollectdPushCollector.CollectdKey(
                "localhost",
                "plugin",
                "plugin_instance[a=b,c,d=4]",
                "type",
                "type_instance");

        assertEquals(expected, msg.tags);
    }
}
