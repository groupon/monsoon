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

import com.groupon.lex.metrics.config.Configuration;
import com.groupon.lex.metrics.config.ConfigurationException;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 *
 * @author ariane
 */
public class Report {
    private final File file_;
    private Optional<Exception> except_ = Optional.empty();
    private Optional<List<String>> parse_errors_ = Optional.empty();
    private Optional<Configuration> config_ = Optional.empty();

    public Report(File file, boolean recursive) {
        file_ = file;

        try {
            Configuration cfg = Configuration.readFromFile(file);
            if (recursive) cfg = cfg.resolve();
            config_ = Optional.of(cfg);
        } catch (IOException ex) {
            except_ = Optional.of(ex);
        } catch (ConfigurationException ex) {
            parse_errors_ = Optional.of(ex.getParseErrors());
            if (ex.getCause() != null) {
                if (ex.getCause() instanceof Exception)
                    except_ = Optional.ofNullable((Exception)ex.getCause());
                else
                    throw new RuntimeException(ex);
            }
        }
    }

    public Optional<Exception> getExcept() { return except_; }
    public Optional<List<String>> getParseErrors() { return parse_errors_; }
    public boolean hasErrors() { return except_.isPresent() || parse_errors_.isPresent(); }

    public String toString() {
        return String.format("Parsed %s\n", file_)
                + parse_errors_.map(errlist -> errlist.stream().map(err -> String.format("Parse error: %s\n", err)).collect(Collectors.joining())).orElse("")
                + except_.map(Exception::getMessage).map(s -> String.format("Encountered exception: %s\n", s)).orElse("")
                + (parse_errors_.isPresent() || except_.isPresent() ? "" : "No errors detected.\n");
    }

    public Optional<String> configString() {
        return config_.map(Configuration::configString).map(Object::toString);
    }
}
