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

import static com.groupon.lex.metrics.ConfigSupport.maybeQuoteIdentifier;
import static com.groupon.lex.metrics.ConfigSupport.quotedString;
import com.groupon.lex.metrics.MetricValue;
import com.groupon.lex.metrics.PushMetricRegistryInstance;
import com.groupon.lex.metrics.expression.LiteralGroupExpression;
import com.groupon.lex.metrics.transformers.LiteralNameResolver;
import java.io.File;
import java.io.FileWriter;
import java.io.StringReader;
import java.util.Arrays;
import static java.util.Collections.singleton;
import java.util.Set;
import java.util.TreeSet;
import javax.management.ObjectName;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

/**
 *
 * @author ariane
 */
public class ConfigurationTest {
    @Test
    public void emptyConfig() throws Exception {
        Configuration cfg = Configuration.readFromFile(null, new StringReader(""));

        assertTrue("no imports", cfg.getImports().isEmpty());
        assertTrue("no monitors", cfg.getMonitors().isEmpty());
        assertTrue("no rules", cfg.getRules().isEmpty());
        assertTrue(cfg.isResolved());
        assertFalse(cfg.needsResolve());
    }

    @Test
    public void commentOnlyConfig() throws Exception {
        Configuration cfg = Configuration.readFromFile(null, new StringReader("# Nothing new under the sun\n"));

        assertTrue("no imports", cfg.getImports().isEmpty());
        assertTrue("no monitors", cfg.getMonitors().isEmpty());
        assertTrue("no rules", cfg.getRules().isEmpty());
        assertTrue(cfg.isResolved());
        assertFalse(cfg.needsResolve());
    }

    @Test
    public void commentOnlyConfigNoNewline() throws Exception {
        Configuration cfg = Configuration.readFromFile(null, new StringReader("# Nothing new under the sun"));

        assertTrue("no imports", cfg.getImports().isEmpty());
        assertTrue("no monitors", cfg.getMonitors().isEmpty());
        assertTrue("no rules", cfg.getRules().isEmpty());
        assertTrue(cfg.isResolved());
        assertFalse(cfg.needsResolve());
    }

    @Test
    public void importStatement() throws Exception {
        Configuration cfg = Configuration.readFromFile(File.listRoots()[0], new StringReader("import all\n  from \"file.name\";"));

        assertFalse("import present", cfg.getImports().isEmpty());
        assertTrue("no monitors", cfg.getMonitors().isEmpty());
        assertTrue("no rules", cfg.getRules().isEmpty());
        assertFalse(cfg.isResolved());
        assertTrue(cfg.needsResolve());

        ImportStatement cfg_import = cfg.getImports().iterator().next();
        assertEquals("import all from \"file.name\";\n", cfg_import.configString().toString());
    }

    @Test
    public void listenerStatement() throws Exception {
        Configuration cfg = Configuration.readFromFile(null, new StringReader("collect jmx_listener \"java.lang:*\";"));

        assertTrue("no imports", cfg.getImports().isEmpty());
        assertFalse("monitors present", cfg.getMonitors().isEmpty());
        assertTrue("no rules", cfg.getRules().isEmpty());
        assertTrue(cfg.isResolved());
        assertFalse(cfg.needsResolve());

        MonitorStatement mon = cfg.getMonitors().iterator().next();
        assertTrue("monitor is a jmx listener", mon instanceof JmxListenerMonitor);
        JmxListenerMonitor mon_impl = (JmxListenerMonitor)mon;
        assertEquals("monitor listens for \"java.lang:*\"", singleton(new ObjectName("java.lang:*")), mon_impl.getIncludes());
    }

    @Test
    public void multilineListenerStatement() throws Exception {
        final Set<ObjectName> expected = new TreeSet<ObjectName>(Arrays.asList(new ObjectName("java.lang:*"), new ObjectName("metrics:*")));
        Configuration cfg = Configuration.readFromFile(null, new StringReader("collect jmx_listener \"java.lang:*\", \"metrics:*\";"));

        assertTrue("no imports", cfg.getImports().isEmpty());
        assertFalse("monitors present", cfg.getMonitors().isEmpty());
        assertTrue("no rules", cfg.getRules().isEmpty());
        assertTrue(cfg.isResolved());
        assertFalse(cfg.needsResolve());

        MonitorStatement mon = cfg.getMonitors().iterator().next();
        assertTrue("monitor is a jmx listener", mon instanceof JmxListenerMonitor);
        JmxListenerMonitor mon_impl = (JmxListenerMonitor)mon;
        assertEquals("monitor listens for \"java.lang:*\"", expected, mon_impl.getIncludes());
    }

    @Test
    public void defaultConfigSerialization() throws Exception {
        Configuration cfg = Configuration.readFromFile(null, new StringReader(Configuration.DEFAULT.configString().toString()));

        assertEquals("Configuration can parse itself", Configuration.DEFAULT, cfg);
    }

    @Test(expected = ConfigurationException.class)
    public void parseError() throws Exception {
        Configuration cfg = Configuration.readFromFile(null, new StringReader("import;\n"));
    }

    @Test
    public void urlCollectorParsing() throws Exception {
        Configuration cfg = Configuration.readFromFile(null, new StringReader("collect url \"http://$0\" as root {\n" +
                "0 = [ \"www.google.com\", \"www.groupon.com\" ]," +
                "1 = [ \"index.html\", \"error.html\" ]}"));
        Configuration reparsed = Configuration.readFromFile(null, new StringReader(cfg.configString().toString()));

        assertEquals(cfg.configString().toString(), reparsed.configString().toString());
    }

    @Test(expected = IllegalStateException.class)
    public void unresolvedConfigurationWontCreate() throws Exception {
        Configuration cfg = Configuration.readFromFile(File.listRoots()[0], new StringReader("import all\n  from \"file.name\";"));

        assertTrue(cfg.needsResolve());
        cfg.create((pattern, handler) -> {});
    }

    @Test
    public void resolveConfiguration() throws Exception {
        // Contents to store in loaded file.
        final String contents =
                new ResolvedConstantStatement(
                        new LiteralGroupExpression(new LiteralNameResolver("foo")),
                        new LiteralNameResolver("metric"),
                        MetricValue.TRUE)
                .configString()
                .toString();

        // Create the actual file.
        File file = File.createTempFile("ConfigurationTest-", ".monsoon");
        file.deleteOnExit();
        try (FileWriter out = new FileWriter(file)) {
            out.append(contents);
        }

        // Read config refering to the file we just created.
        Configuration cfg = Configuration.readFromFile(file.getParentFile(),
                new StringReader("import all from " + quotedString(file.getName()) + ";\n"
                        + "collect url \"http://localhost/\" as 'test';"));
        assertTrue(cfg.needsResolve());
        // Create must not fail.
        try (PushMetricRegistryInstance instance = cfg.resolve().create((pattern, handler) -> {})) {
            /* SKIP */
        }
    }


    @Test
    public void resolveConfigurationUsingReducer() throws Exception {
        // Contents to store in loaded file.
        final String contents =
                new ResolvedConstantStatement(
                        new LiteralGroupExpression(new LiteralNameResolver("foo")),
                        new LiteralNameResolver("metric"),
                        MetricValue.TRUE)
                .configString()
                .toString();

        // Create a file.
        File file = File.createTempFile("ConfigurationTest-", ".monsoon");
        file.deleteOnExit();
        try (FileWriter out = new FileWriter(file)) {
            out.append(contents);
        }

        // Read config refering to the file we just created.
        Configuration cfg = Configuration.readFromFile(file.getParentFile(),
                new StringReader("import all from " + quotedString(file.getName()) + ";\n"
                        + "import all from " + quotedString(file.getName()) + ";\n"
                        + "import all from " + quotedString(file.getName()) + ";\n"
                        + "import all from " + quotedString(file.getName()) + ";\n"
                        + "import all from " + quotedString(file.getName()) + ";\n"
                        + "import all from " + quotedString(file.getName()) + ";\n"
                        + "collect url \"http://localhost/\" as 'test';"));
        assertTrue(cfg.needsResolve());
        // Create must not fail.
        try (PushMetricRegistryInstance instance = cfg.resolve().create((pattern, handler) -> {})) {
            /* SKIP */
        }
    }

    @Test
    public void readFromFile() throws Exception {
        File file = File.createTempFile("ConfigurationTest-", ".monsoon");
        file.deleteOnExit();
        try (FileWriter out = new FileWriter(file)) {
            out.append("collect url \"http://$0\" as root {\n" +
                    "0 = [ \"www.google.com\", \"www.groupon.com\" ]," +
                    "1 = [ \"index.html\", \"error.html\" ]}");
        }
        Configuration cfg = Configuration.readFromFile(file);
        Configuration reparsed = Configuration.readFromFile(null, new StringReader(cfg.configString().toString()));

        assertEquals(cfg.configString().toString(), reparsed.configString().toString());
    }

    @Test
    public void testToStringWithImports() throws Exception {
        final String file = "filename.monsoon";

        // Read config refering to the file we just created.
        Configuration cfg = Configuration.readFromFile(null,
                new StringReader("import all from " + quotedString(file) + ";\n"
                        + "import all from " + quotedString(file) + ";\n"
                        + "import all from " + quotedString(file) + ";\n"
                        + "import all from " + quotedString(file) + ";\n"
                        + "import all from " + quotedString(file) + ";\n"
                        + "import all from " + quotedString(file) + ";\n"
                        + "collect url \"http://localhost/\" as 'test';"));

        assertThat(cfg.toString(), containsString("import all from " + quotedString(file) + ";"));
        assertThat(cfg.toString(), containsString("collect url \"http://localhost/\" as " + maybeQuoteIdentifier("test") + ";"));
    }

    @Test
    public void equality() throws Exception {
        final String file = "filename.monsoon";

        Configuration cfg1 = Configuration.readFromFile(null,
                new StringReader("import all from " + quotedString(file) + ";\n"));
        Configuration cfg2 = Configuration.readFromFile(null,
                new StringReader("import all from " + quotedString(file) + ";\n"));

        assertEquals(cfg1.toString(), cfg2.toString());
        assertTrue(cfg1.equals(cfg2));
        assertEquals(cfg1.hashCode(), cfg2.hashCode());
    }

    @Test
    public void inequality() throws Exception {
        final String file1 = "filename1.monsoon";
        final String file2 = "filename2.monsoon";

        Configuration cfg1 = Configuration.readFromFile(null,
                new StringReader("import all from " + quotedString(file1) + ";\n"));
        Configuration cfg2 = Configuration.readFromFile(null,
                new StringReader("import all from " + quotedString(file2) + ";\n"));

        assertNotEquals(cfg1.toString(), cfg2.toString());
        assertFalse(cfg1.equals(cfg2));
        assertFalse(cfg1.equals(new Object()));
        assertFalse(cfg1.equals(null));
    }
}
