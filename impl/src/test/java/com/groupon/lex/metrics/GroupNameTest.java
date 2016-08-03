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

import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasEntry;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

/**
 *
 * @author ariane
 */
public class GroupNameTest {
    @Test
    public void array_constructor() {
        GroupName group = GroupName.valueOf("foo", "bar");

        assertThat(group.getPath().getPath(), contains("foo", "bar"));
        assertTrue(group.getTags().isEmpty());
    }

    @Test
    public void list_constructor() {
        GroupName group = GroupName.valueOf(SimpleGroupPath.valueOf("foo", "bar"));

        assertThat(group.getPath().getPath(), contains("foo", "bar"));
        assertTrue(group.getTags().isEmpty());
    }

    @Test
    public void tag_constructor() {
        GroupName group = GroupName.valueOf(SimpleGroupPath.valueOf("group"), Tags.valueOf(singletonMap("x", MetricValue.TRUE)));

        assertThat(group.getPath().getPath(), contains("group"));
        assertThat(group.getTags().asMap(), hasEntry("x", MetricValue.TRUE));
    }

    @Test
    public void tagmap_constructor() {
        GroupName group = GroupName.valueOf(SimpleGroupPath.valueOf("group"), singletonMap("x", MetricValue.TRUE));

        assertThat(group.getPath().getPath(), contains("group"));
        assertThat(group.getTags().asMap(), hasEntry("x", MetricValue.TRUE));
    }

    @Test
    public void config_string() {
        GroupName group = GroupName.valueOf(SimpleGroupPath.valueOf("foo", "bar", "escape-dash"), singletonMap("x", MetricValue.TRUE));
        GroupName untagged = GroupName.valueOf("escape.dots", "escape'quotes'");

        assertEquals("foo.bar.'escape-dash'{x=true}", group.configString().toString());
        assertEquals("'escape.dots'.'escape\\'quotes\\''", untagged.configString().toString());
    }

    @Test
    public void to_string() {
        GroupName group = GroupName.valueOf(SimpleGroupPath.valueOf("foo", "bar", "escape-dash"), singletonMap("x", MetricValue.TRUE));

        assertEquals("foo.bar.'escape-dash'{x=true}", group.toString());
    }

    @Test
    public void tagstream_constructor() {
        GroupName group = GroupName.valueOf(SimpleGroupPath.valueOf("group"), singletonMap("x", MetricValue.TRUE).entrySet().stream());

        assertThat(group.getPath().getPath(), contains("group"));
        assertThat(group.getTags().asMap(), hasEntry("x", MetricValue.TRUE));
    }

    @Test
    public void equality() {
        assertEquals(GroupName.valueOf("foo").hashCode(), GroupName.valueOf(SimpleGroupPath.valueOf("foo")).hashCode());
        assertEquals(GroupName.valueOf("foo"), GroupName.valueOf(SimpleGroupPath.valueOf("foo")));

        assertEquals(GroupName.valueOf(SimpleGroupPath.valueOf("foo"), singletonMap("x", MetricValue.fromStrValue("y"))).hashCode(),
                GroupName.valueOf(SimpleGroupPath.valueOf("foo"), singletonMap("x", MetricValue.fromStrValue("y"))).hashCode());
        assertEquals(GroupName.valueOf(SimpleGroupPath.valueOf("foo"), singletonMap("x", MetricValue.fromStrValue("y"))),
                GroupName.valueOf(SimpleGroupPath.valueOf("foo"), singletonMap("x", MetricValue.fromStrValue("y"))));
    }

    @Test
    public void inequality() {
        GroupName group = GroupName.valueOf(SimpleGroupPath.valueOf("group"), singletonMap("x", MetricValue.TRUE).entrySet().stream());
        GroupName group_with_path_change = GroupName.valueOf(SimpleGroupPath.valueOf("other"), singletonMap("x", MetricValue.TRUE).entrySet().stream());
        GroupName group_with_tags_change = GroupName.valueOf(SimpleGroupPath.valueOf("group"), singletonMap("y", MetricValue.TRUE).entrySet().stream());

        assertFalse(group.equals(null));
        assertFalse(group.equals(new Object()));
        assertFalse(group.equals(group_with_path_change));
        assertFalse(group.equals(group_with_tags_change));
    }

    @Test
    public void compare() {
        GroupName a = GroupName.valueOf("a");
        GroupName clone_of_a = GroupName.valueOf("a");
        GroupName a_b = GroupName.valueOf("a", "b");
        GroupName abba = GroupName.valueOf("abba");
        GroupName a_tagged_true = GroupName.valueOf(SimpleGroupPath.valueOf("a"), singletonMap("x", MetricValue.TRUE));
        GroupName a_tagged_false = GroupName.valueOf(SimpleGroupPath.valueOf("a"), singletonMap("x", MetricValue.FALSE));

        assertEquals(0, a.compareTo(clone_of_a));
        assertTrue(a.compareTo(a_b) < 0);
        assertTrue(a.compareTo(abba) < 0);
        assertTrue(a.compareTo(a_tagged_true) < 0);
        assertTrue(a.compareTo(a_tagged_false) < 0);
        assertEquals(a_tagged_false.getTags().compareTo(a_tagged_true.getTags()),
                a_tagged_false.compareTo(a_tagged_true));
        assertEquals(a_tagged_true.getTags().compareTo(a_tagged_false.getTags()),
                a_tagged_true.compareTo(a_tagged_false));
    }
}
