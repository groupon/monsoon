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
package com.groupon.lex.metrics.transformers;

import com.groupon.lex.metrics.SimpleGroupPath;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollectionPairInstance;
import com.groupon.lex.metrics.timeseries.expression.SimpleContext;
import java.util.Optional;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import org.junit.Test;

/**
 *
 * @author ariane
 */
public class NameResolverCombineTest {
    private final NameResolver x = new LiteralNameResolver("x");
    private final NameResolver y = new LiteralNameResolver("y");
    private final NameResolver resolver = NameResolver.combine(x, y);

    @Test
    public void to_string() {
        assertEquals("x.y", resolver.toString());
    }

    @Test
    public void equality() {
        assertEquals(new NameResolver.CombinedNameResolver(x, y), resolver);
        assertEquals(new NameResolver.CombinedNameResolver(x, y).hashCode(), resolver.hashCode());
    }

    @Test
    public void inequality() {
        assertNotEquals(new NameResolver.CombinedNameResolver(y, x), resolver);
        assertNotEquals(new NameResolver.CombinedNameResolver(x, x), resolver);
        assertFalse(resolver.equals(null));
        assertFalse(resolver.equals(new Object()));
    }

    @Test
    public void resolve() {
        Optional<SimpleGroupPath> path = resolver.apply(new SimpleContext(new TimeSeriesCollectionPairInstance(), (alert) -> {})).map(p -> SimpleGroupPath.valueOf(p.getPath()));

        assertEquals(Optional.of(SimpleGroupPath.valueOf("x", "y")), path);
    }
}
