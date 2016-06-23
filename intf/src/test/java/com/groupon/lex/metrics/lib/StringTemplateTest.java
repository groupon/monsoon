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
package com.groupon.lex.metrics.lib;

import com.groupon.lex.metrics.lib.StringTemplate;
import com.groupon.lex.metrics.lib.StringTemplate.LiteralElement;
import com.groupon.lex.metrics.lib.StringTemplate.SubstituteElement;
import com.groupon.lex.metrics.lib.Any2;
import com.groupon.lex.metrics.lib.StringTemplate.SubstituteNameElement;
import java.util.Arrays;
import java.util.Collection;
import static java.util.Collections.EMPTY_LIST;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import org.junit.Test;

/**
 *
 * @author ariane
 */
public class StringTemplateTest {
    @Test
    public void empty() {
        final StringTemplate tmpl = new StringTemplate(EMPTY_LIST);

        assertEquals(0, tmpl.indexSize());
        assertEquals("", tmpl.apply(emptyMap()));
    }

    @Test
    public void literal() {
        final StringTemplate tmpl = StringTemplate.fromString("chocoladevla");;

        assertEquals(0, tmpl.indexSize());
        assertEquals("chocoladevla", tmpl.apply(emptyMap()));
    }

    @Test
    public void templated_using_value() {
        final StringTemplate tmpl = StringTemplate.fromString("$0");

        assertEquals(1, tmpl.indexSize());
        assertEquals("foobar", tmpl.apply(singletonMap(Any2.<String, Integer>right(0), "foobar")));
    }

    @Test
    public void templated_using_name() {
        final StringTemplate tmpl = StringTemplate.fromString("${arg}");

        assertEquals(0, tmpl.indexSize());
        assertEquals("foobar", tmpl.apply(singletonMap(Any2.<String, Integer>left("arg"), "foobar")));
    }

    @Test
    public void long_string() {
        final StringTemplate tmpl = new StringTemplate(Arrays.asList(
                new SubstituteElement(0),
                new LiteralElement("="),
                new SubstituteElement(2),
                new SubstituteElement(1),
                new LiteralElement("^"),
                new SubstituteElement(3)));

        assertEquals(4, tmpl.indexSize());
        assertEquals("E=mc^2", tmpl.apply(
                Stream.of(
                                singletonMap(Any2.<String, Integer>right(0), "E"),
                                singletonMap(Any2.<String, Integer>right(1), "c"),
                                singletonMap(Any2.<String, Integer>right(2), "m"),
                                singletonMap(Any2.<String, Integer>right(3), "2")
                        )
                        .map(Map::entrySet)
                        .flatMap(Collection::stream)
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))));
    }

    @Test
    public void mixing_indices_and_names() {
        final StringTemplate tmpl = StringTemplate.fromString("${0}=${2}${1}^${two}");

        assertEquals(3, tmpl.indexSize());
        assertThat(tmpl.getArguments(),
                containsInAnyOrder(Any2.left("two"), Any2.right(0), Any2.right(1), Any2.right(2)));
        assertEquals("E=mc^2", tmpl.apply(
                Stream.of(
                                singletonMap(Any2.<String, Integer>right(0), "E"),
                                singletonMap(Any2.<String, Integer>right(1), "c"),
                                singletonMap(Any2.<String, Integer>right(2), "m"),
                                singletonMap(Any2.<String, Integer>left("two"), "2")
                        )
                        .map(Map::entrySet)
                        .flatMap(Collection::stream)
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))));
    }

    @Test
    public void validToString() {
        final StringTemplate tmpl = new StringTemplate(Arrays.asList(
                new SubstituteElement(0),
                new LiteralElement("="),
                new SubstituteElement(2),
                new SubstituteElement(1),
                new LiteralElement("^"),
                new SubstituteNameElement("two")));

        assertEquals("${0}=${2}${1}^${two}", tmpl.toString());
    }

    @Test
    public void config_string() {
        final StringTemplate tmpl = new StringTemplate(Arrays.asList(
                new SubstituteElement(0),
                new LiteralElement("="),
                new SubstituteElement(2),
                new SubstituteElement(1),
                new LiteralElement("^\t"),
                new SubstituteNameElement("two")));

        assertEquals("\"${0}=${2}${1}^\\t${two}\"", tmpl.configString().toString());
    }

    @Test
    public void fromString() {
        final StringTemplate tmpl = StringTemplate.fromString("$1-$0");

        assertEquals("${1}-${0}", tmpl.toString());
        assertEquals("b-a", tmpl.apply(
                Stream.of(
                                singletonMap(Any2.<String, Integer>right(0), "a"),
                                singletonMap(Any2.<String, Integer>right(1), "b")
                        )
                        .map(Map::entrySet)
                        .flatMap(Collection::stream)
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))));
    }

    @Test
    public void fromStringWithDollars() {
        final StringTemplate tmpl = StringTemplate.fromString("$$$0");

        assertEquals("$$${0}", tmpl.toString());
        assertEquals("$foobar", tmpl.apply(singletonMap(Any2.right(0), "foobar")));
    }

    @Test(expected = IllegalArgumentException.class)
    public void bad_string() {
        StringTemplate.fromString("${ 0}");
    }
}
