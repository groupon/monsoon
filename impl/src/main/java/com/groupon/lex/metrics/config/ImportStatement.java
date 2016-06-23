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

import com.groupon.lex.metrics.ConfigSupport;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collection;
import static java.util.Collections.emptyList;
import java.util.Objects;

/**
 *
 * @author ariane
 */
public class ImportStatement implements ConfigStatement {
    public final static int CONSTANTS = 0x1;
    public final static int MONITORS = 0x2;
    public final static int RULES = 0x4;
    public final static int ALERTS = 0x10;
    public final static int ALL = CONSTANTS | MONITORS | RULES | ALERTS;

    private final String fname_;
    private final int selector_;
    private final File cfg_file_;

    public ImportStatement(File dir, String fname) {
        this(dir, fname, ALL);
    }

    public ImportStatement(File dir, String fname, int selector) {
        if ((selector & ALL) != selector)
            throw new IllegalArgumentException("unrecognized selector 0x" + Integer.toHexString(selector));
        fname_ = fname;
        selector_ = selector;
        cfg_file_ = new File(dir, fname_);
    }

    public Configuration read() throws IOException, ConfigurationException {
        final Configuration config_file;
        try (final Reader input = new FileReader(cfg_file_)) {
            config_file = Configuration.readFromFile(cfg_file_.getParentFile(), input).resolve();
        }

        if (selector_ == ALL) return config_file;
        return new Configuration(emptyList(),
                ((selector_ & MONITORS) == MONITORS ? config_file.getMonitors() : emptyList()),
                ((selector_ & RULES) == RULES ? config_file.getRules() : emptyList()));
    }

    public File getConfigFile() { return cfg_file_; }

    private String getSelectorKeywords() {
        if (selector_ == ALL)
            return "all";

        final String SEP = ", ";

        Collection<String> kw = new ArrayList<String>();
        if ((selector_ & MONITORS) == MONITORS) kw.add("collectors");
        if ((selector_ & CONSTANTS) == CONSTANTS) kw.add("constants");
        if ((selector_ & RULES) == RULES) kw.add("rules");
        if ((selector_ & ALERTS) == ALERTS) kw.add("alerts");

        return String.join(SEP, kw);
    }

    @Override
    public StringBuilder configString() {
        return new StringBuilder()
                .append("import ")
                .append(getSelectorKeywords())
                .append(" from ")
                .append(ConfigSupport.quotedString(fname_))
                .append(";\n");
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 67 * hash + Objects.hashCode(this.fname_);
        hash = 67 * hash + Integer.hashCode(this.selector_);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final ImportStatement other = (ImportStatement) obj;
        if (!Objects.equals(this.fname_, other.fname_)) {
            return false;
        }
        if (this.selector_ != other.selector_) {
            return false;
        }
        if (!Objects.equals(this.cfg_file_, other.cfg_file_)) {
            return false;
        }
        return true;
    }
}
