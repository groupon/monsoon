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

        assertFalse(msg.equals(msg_host));
        assertFalse(msg.equals(msg_plugin));
        assertFalse(msg.equals(msg_plugin_instance));
        assertFalse(msg.equals(msg_type));
        assertFalse(msg.equals(msg_type_instance));
        assertFalse(msg.equals(new Object()));
        assertFalse(msg.equals(null));
    }
}
