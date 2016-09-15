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
package com.groupon.lex.metrics.timeseries.expression;

import com.groupon.lex.metrics.timeseries.Alert;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollectionPair;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollectionPairInstance;
import java.util.Optional;
import java.util.function.Consumer;
import static org.hamcrest.Matchers.hasKey;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

/**
 *
 * @author ariane
 */
public class MutableContextTest {
    private final TimeSeriesCollectionPair ts_data = new TimeSeriesCollectionPairInstance(DateTime.now(DateTimeZone.UTC));
    private final Consumer<Alert> alert_manager = (Alert alert) -> {};
    private final String DATA = "The quick brown fox...";
    private final String ALIAS = "alias";

    @Test
    public void constructor() {
        MutableContext ctx = new MutableContext(ts_data, alert_manager);

        assertSame(alert_manager, ctx.getAlertManager());
        assertSame(ts_data, ctx.getTSData());
        assertTrue(ctx.getAllIdentifiers().isEmpty());
        assertEquals(Optional.empty(), ctx.getAliasFromIdentifier(String.class, "s"));
    }

    @Test
    public void construct_from_copy() {
        Context parent = new SimpleContext(ts_data, alert_manager);
        MutableContext ctx = new MutableContext(parent);

        assertSame(alert_manager, ctx.getAlertManager());
        assertSame(ts_data, ctx.getTSData());
        assertTrue(ctx.getAllIdentifiers().isEmpty());
        assertEquals(Optional.empty(), ctx.getAliasFromIdentifier(String.class, "s"));
    }

    @Test
    public void construct_from_copy_with_tsdata() {
        Context parent = new SimpleContext(ts_data, alert_manager);
        TimeSeriesCollectionPair new_tsdata = new TimeSeriesCollectionPairInstance(DateTime.now(DateTimeZone.UTC));
        MutableContext ctx = new MutableContext(new_tsdata, parent);

        assertSame(alert_manager, ctx.getAlertManager());
        assertSame(new_tsdata, ctx.getTSData());
        assertTrue(ctx.getAllIdentifiers().isEmpty());
        assertEquals(Optional.empty(), ctx.getAliasFromIdentifier(String.class, "s"));
    }

    @Test
    public void put_1() {
        MutableContext ctx = new MutableContext(ts_data, alert_manager);
        ctx.put("identifier", String.class, DATA, ALIAS);

        assertThat(ctx.getAllIdentifiers(), hasKey("identifier"));

        // Check that identifier is resolveable to the same type.
        assertSame(DATA, ctx.getFromIdentifier(String.class, "identifier").get());
        assertSame(DATA, ctx.getFromIdentifier(CharSequence.class, "identifier").get());
        assertEquals(Optional.empty(), ctx.getFromIdentifier(Float.class, "identifier"));

        // Check that alias is resolveable to the original value.
        assertSame(ALIAS, ctx.getAliasFromIdentifier(String.class, "identifier").get());
        assertSame(ALIAS, ctx.getAliasFromIdentifier(CharSequence.class, "identifier").get());
        assertEquals(Optional.empty(), ctx.getAliasFromIdentifier(Float.class, "identifier"));
    }

    @Test
    public void put_using_opt_value_present() {
        MutableContext ctx = new MutableContext(ts_data, alert_manager);
        ctx.put("identifier", String.class, Optional.of(DATA), ALIAS);

        assertThat(ctx.getAllIdentifiers(), hasKey("identifier"));

        // Check that identifier is resolveable to the same type.
        assertSame(DATA, ctx.getFromIdentifier(String.class, "identifier").get());
        assertSame(DATA, ctx.getFromIdentifier(CharSequence.class, "identifier").get());
        assertEquals(Optional.empty(), ctx.getFromIdentifier(Float.class, "identifier"));

        // Check that alias is resolveable to the original value.
        assertSame(ALIAS, ctx.getAliasFromIdentifier(String.class, "identifier").get());
        assertSame(ALIAS, ctx.getAliasFromIdentifier(CharSequence.class, "identifier").get());
        assertEquals(Optional.empty(), ctx.getAliasFromIdentifier(Float.class, "identifier"));
    }

    @Test
    public void put_using_opt_value_absent() {
        MutableContext ctx = new MutableContext(ts_data, alert_manager);
        ctx.put("identifier", String.class, Optional.empty(), ALIAS);

        assertThat(ctx.getAllIdentifiers(), hasKey("identifier"));

        // Check that identifier is resolveable to the same type.
        assertEquals(Optional.empty(), ctx.getFromIdentifier(String.class, "identifier"));
        assertEquals(Optional.empty(), ctx.getFromIdentifier(CharSequence.class, "identifier"));
        assertEquals(Optional.empty(), ctx.getFromIdentifier(Float.class, "identifier"));

        // Check that alias is resolveable to the original value.
        assertSame(ALIAS, ctx.getAliasFromIdentifier(String.class, "identifier").get());
        assertSame(ALIAS, ctx.getAliasFromIdentifier(CharSequence.class, "identifier").get());
        assertEquals(Optional.empty(), ctx.getAliasFromIdentifier(Float.class, "identifier"));
    }

    @Test
    public void put_using_producer() {
        MutableContext ctx = new MutableContext(ts_data, alert_manager);
        ctx.putSupplied("identifier", String.class, (c) -> Optional.of(DATA), () -> ALIAS);

        // Check that identifier is resolveable to the same type.
        assertSame(DATA, ctx.getFromIdentifier(String.class, "identifier").get());
        assertSame(DATA, ctx.getFromIdentifier(CharSequence.class, "identifier").get());
        assertEquals(Optional.empty(), ctx.getFromIdentifier(Float.class, "identifier"));

        // Check that alias is resolveable to the original value.
        assertSame(ALIAS, ctx.getAliasFromIdentifier(String.class, "identifier").get());
        assertSame(ALIAS, ctx.getAliasFromIdentifier(CharSequence.class, "identifier").get());
        assertEquals(Optional.empty(), ctx.getAliasFromIdentifier(Float.class, "identifier"));
    }

    @Test
    public void remove() {
        MutableContext ctx = new MutableContext(ts_data, alert_manager);
        ctx.put("identifier", String.class, Optional.empty(), ALIAS);
        assertThat(ctx.getAllIdentifiers(), hasKey("identifier"));

        ctx.remove("identifier");

        // Check that identifier is resolveable to the same type.
        assertEquals(Optional.empty(), ctx.getFromIdentifier(String.class, "identifier"));
        assertEquals(Optional.empty(), ctx.getFromIdentifier(CharSequence.class, "identifier"));
        assertEquals(Optional.empty(), ctx.getFromIdentifier(Float.class, "identifier"));

        // Check that alias is resolveable to the original value.
        assertEquals(Optional.empty(), ctx.getAliasFromIdentifier(String.class, "identifier"));
        assertEquals(Optional.empty(), ctx.getAliasFromIdentifier(CharSequence.class, "identifier"));
        assertEquals(Optional.empty(), ctx.getAliasFromIdentifier(Float.class, "identifier"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void put_when_lying_about_type() {
        Class clazz = Float.class;
        MutableContext ctx = new MutableContext(ts_data, alert_manager);

        ctx.put("identifier", clazz, DATA, ALIAS);
    }

    @Test(expected = IllegalArgumentException.class)
    public void put_when_lying_about_type_using_optional() {
        Class<String> clazz = (Class<String>)(Class)Float.class;  // Tricking the type system, using type erase and reintroduction to mismap the type.
        MutableContext ctx = new MutableContext(ts_data, alert_manager);

        ctx.put("identifier", clazz, Optional.of(DATA), ALIAS);
    }

    @Test(expected = IllegalArgumentException.class)
    public void put_using_producer_when_lying_about_type() {
        Class clazz = Float.class;
        MutableContext ctx = new MutableContext(ts_data, alert_manager);

        ctx.putSupplied("identifier", clazz, (c) -> Optional.of(DATA), () -> ALIAS);
        ctx.getFromIdentifier(clazz, "identifier");
    }
}
