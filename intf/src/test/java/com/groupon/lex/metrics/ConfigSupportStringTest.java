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

import static org.junit.Assert.assertEquals;
import org.junit.Test;

/**
 *
 * @author ariane
 */
public class ConfigSupportStringTest {
    @Test
    public void simple_string() {
        String s = ConfigSupport.quotedString("this wont need any escapes 999").toString();

        assertEquals("\"this wont need any escapes 999\"", s);
    }

    @Test
    public void quote_empty() {
        String s = ConfigSupport.quotedString("").toString();

        assertEquals("\"\"", s);
    }

    @Test
    public void escape_quotes() {
        String s = ConfigSupport.quotedString("\"'").toString();

        assertEquals("\"\\\"'\"", s);
    }

    @Test
    public void escape_utf16() {
        String s = ConfigSupport.quotedString("\u1700").toString();

        assertEquals("\"\\u1700\"", s);
    }

    @Test
    public void escape_utf16_correct_leading_zeroes() {
        String s = ConfigSupport.quotedString("\u0700").toString();

        assertEquals("\"\\u0700\"", s);
    }

    @Test
    public void special_escapes() {
        String s = ConfigSupport.quotedString("\r\n\t").toString();

        assertEquals("\"\\r\\n\\t\"", s);
    }

    @Test
    public void escape_utf32() {
        final String INPUT = String.copyValueOf(Character.toChars(0x10fff));
        String s = ConfigSupport.quotedString(INPUT).toString();

        assertEquals("\"\\U00010fff\"", s);
    }

    @Test
    public void keywords_are_not_special() {
        String s = ConfigSupport.quotedString("as").toString();

        assertEquals("\"as\"", s);
    }

    @Test
    public void escape_nul_char() {
        String s = ConfigSupport.quotedString("\0").toString();

        assertEquals("\"\\0\"", s);
    }

    @Test
    public void escape_backslash() {
        String s = ConfigSupport.quotedString("\\").toString();

        assertEquals("\"\\\\\"", s);
    }
}
