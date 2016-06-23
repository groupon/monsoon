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
package com.groupon.lex.metrics.config.parser;

import com.groupon.lex.metrics.config.Configuration;
import java.io.StringReader;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author ariane
 */
public class ConfigurationCommentStatementTest {
    private String EXPECTED;

    @Before
    public void setup() throws Exception {
        EXPECTED = Configuration.readFromFile(null, new StringReader("alert foobar if 1 > 0 for 10m;")).toString();
    }

    @Test
    public void comment1() throws Exception {
        final String DOC_WITH_COMMENT = "alert foobar if 1 >   # comment starts here\n" +
                "  0 for 10m;";
        Configuration cfg_with_comment = Configuration.readFromFile(null, new StringReader(DOC_WITH_COMMENT));

        assertEquals(EXPECTED, cfg_with_comment.toString());
    }

    @Test
    public void comment2() throws Exception {
        final String DOC_WITH_COMMENT = "alert foobar if 1 > 0  # comment starts here\n" +
                "  for 10m;";
        Configuration cfg_with_comment = Configuration.readFromFile(null, new StringReader(DOC_WITH_COMMENT));

        assertEquals(EXPECTED, cfg_with_comment.toString());
    }

    @Test
    public void comment3() throws Exception {
        final String DOC_WITH_COMMENT = "alert too_many_restarts\n" +
                "if java.lang.Runtime Uptime <= 15 * 60 * 1000  # 15 minutes\n" +
                "for 20m;";
        final String DOC_WITHOUT_COMMENT = "alert too_many_restarts\n" +
                "if java.lang.Runtime Uptime <= 15 * 60 * 1000\n" +
                "for 20m;";

        Configuration cfg_with_comment = Configuration.readFromFile(null, new StringReader(DOC_WITH_COMMENT));
        Configuration cfg_without_comment = Configuration.readFromFile(null, new StringReader(DOC_WITHOUT_COMMENT));

        assertEquals(cfg_without_comment.toString(), cfg_with_comment.toString());
    }
}
