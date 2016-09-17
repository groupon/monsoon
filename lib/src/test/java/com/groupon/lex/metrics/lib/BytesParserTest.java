/*
 * Copyright (c) 2016, Ariane van der Steldt
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

import static org.junit.Assert.assertEquals;
import org.junit.Test;

public class BytesParserTest {
    @Test
    public void parseTest() {
        assertEquals(0L, BytesParser.parse("0"));
        assertEquals(17L, BytesParser.parse("17"));

        assertEquals(4L << 10, BytesParser.parse("4k"));
        assertEquals(4L << 10, BytesParser.parse("4K"));

        assertEquals(16L << 20, BytesParser.parse("16m"));
        assertEquals(16L << 20, BytesParser.parse("16M"));

        assertEquals(5L << 30, BytesParser.parse("5g"));
        assertEquals(5L << 30, BytesParser.parse("5G"));

        assertEquals(764L << 40, BytesParser.parse("764t"));
        assertEquals(764L << 40, BytesParser.parse("764T"));

        assertEquals(2048L << 50, BytesParser.parse("2048p"));
        assertEquals(2048L << 50, BytesParser.parse("2048P"));

        assertEquals(3L << 60, BytesParser.parse("3e"));
        assertEquals(3L << 60, BytesParser.parse("3E"));
    }

    @Test
    public void renderTest() {
        assertEquals("0", BytesParser.render(0));
        assertEquals("17", BytesParser.render(17));
        assertEquals("1025", BytesParser.render(1025));

        assertEquals("4k", BytesParser.render(4L << 10));

        assertEquals("16M", BytesParser.render(16L << 20));

        assertEquals("389G", BytesParser.render(389L << 30));

        assertEquals("2020T", BytesParser.render(2020L << 40));

        assertEquals("5P", BytesParser.render(5L << 50));

        assertEquals("6E", BytesParser.render(6L << 60));
    }
}
