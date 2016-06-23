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

import static com.groupon.lex.metrics.ConfigSupport.quotedString;
import com.groupon.lex.metrics.MetricValue;
import static com.groupon.lex.metrics.config.ImportStatement.ALERTS;
import static com.groupon.lex.metrics.config.ImportStatement.ALL;
import static com.groupon.lex.metrics.config.ImportStatement.CONSTANTS;
import static com.groupon.lex.metrics.config.ImportStatement.MONITORS;
import static com.groupon.lex.metrics.config.ImportStatement.RULES;
import com.groupon.lex.metrics.expression.LiteralGroupExpression;
import com.groupon.lex.metrics.transformers.LiteralNameResolver;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author ariane
 */
public class ImportStatementTest {
    private File file;
    private final String contents =
            new ResolvedConstantStatement(
                    new LiteralGroupExpression(new LiteralNameResolver("foo")),
                    new LiteralNameResolver("metric"),
                    MetricValue.TRUE)
            .configString()
            .toString();

    @Before
    public void setup() throws IOException {
        file = File.createTempFile("ImportStatementTest-", ".monsoon");
        file.deleteOnExit();
        try (FileWriter out = new FileWriter(file)) {
            out.append(contents);
        }
    }

    @Test
    public void importing_all() throws Exception {
        ImportStatement importStatement = new ImportStatement(file.getParentFile(), file.getName());

        assertEquals("import all from " + quotedString(file.getName()) + ";\n", importStatement.configString().toString());
        assertEquals(file, importStatement.getConfigFile());
        Configuration config = importStatement.read();
        assertThat(config.configString().toString(), containsString(contents));
    }

    @Test
    public void importing_collectors() throws Exception {
        ImportStatement importStatement = new ImportStatement(file.getParentFile(), file.getName(), MONITORS);

        assertEquals("import collectors from " + quotedString(file.getName()) + ";\n", importStatement.configString().toString());
        assertEquals(file, importStatement.getConfigFile());
        Configuration config = importStatement.read();
        assertThat(config.configString().toString(), not(containsString(contents)));
    }

    @Test
    public void importing_all_except_collectors() throws Exception {
        ImportStatement importStatement = new ImportStatement(file.getParentFile(), file.getName(), CONSTANTS | RULES | ALERTS);

        assertEquals("import constants, rules, alerts from " + quotedString(file.getName()) + ";\n", importStatement.configString().toString());
        assertEquals(file, importStatement.getConfigFile());
        Configuration config = importStatement.read();
        assertThat(config.configString().toString(), containsString(contents));
    }

    @Test(expected = IllegalArgumentException.class)
    public void constructor_with_invalid_selector() throws Exception {
        new ImportStatement(file.getParentFile(), file.getName(), ~ALL);
    }

    @Test
    public void equality() {
        ImportStatement s1 = new ImportStatement(file.getParentFile(), file.getName(), MONITORS);
        ImportStatement s2 = new ImportStatement(file.getParentFile(), file.getName(), MONITORS);

        assertEquals(s1.hashCode(), s2.hashCode());
        assertEquals(s1, s2);
    }

    @Test
    public void inequality() {
        ImportStatement s = new ImportStatement(file.getParentFile(), file.getName(), MONITORS);
        ImportStatement s_selector = new ImportStatement(file.getParentFile(), file.getName(), RULES);
        ImportStatement s_fname = new ImportStatement(file.getParentFile(), file.getName() + "-different", MONITORS);
        ImportStatement s_dir = new ImportStatement(new File(file.getParentFile(), "othername"), file.getName(), MONITORS);

        assertFalse(s.equals(null));
        assertFalse(s.equals(new Object()));
        assertFalse(s.equals(s_selector));
        assertFalse(s.equals(s_fname));
        assertFalse(s.equals(s_dir));
    }

    @Test(expected = IOException.class)
    public void nonexistant_file() throws Exception {
        new ImportStatement(new File(file, "othername"), file.getName(), ALL).read();
    }
}
