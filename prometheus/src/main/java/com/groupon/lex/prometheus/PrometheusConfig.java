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
package com.groupon.lex.prometheus;

import com.groupon.lex.metrics.config.Configuration;
import com.groupon.lex.metrics.config.ConfigurationException;
import java.io.File;
import java.io.IOException;

/**
 *
 * @author nolofsson
 * This Class is used to set the command line options.
 * if no command line options are given then it uses defaults.
 */
public class PrometheusConfig {
    private short port = 9001;
    private String path = "/metrics";

    public void setPort(short p) { port = p; }
    
    public short getPort (){ return port; };
    
    public String getPath() { return path; };
    
    public void setPath(String p) { path = p; } 
    private File config_file_;
    

    public synchronized String getConfigFile() { return config_file_.toString(); }

    public void setConfigFile(String config_file) throws IOException {
        if (config_file == null) {
            config_file_ = null;
            return;
        }

        setConfigFile(new File(config_file));
    }
    public void setConfigFile(File file) throws IOException {
        if (!file.isAbsolute()) throw new IOException("file name must be absolute: " + file.toString());
        if (!file.exists()) throw new IOException("file not found: " + file.toString());
        if (!file.isFile()) throw new IOException("expected a proper file: " + file.toString());
        config_file_ = file.getCanonicalFile();
    }
    public Configuration getConfiguration() throws IOException, ConfigurationException {
        if (config_file_ == null) return Configuration.DEFAULT;
        return Configuration.readFromFile(config_file_).resolve();
    }
    
    @Override
    public String toString() {           
        return new StringBuilder()
            .append("(")
            .append("prometheus_path=").append(getPort()).append(",")
            .append("prometheus_path=").append(getPath())
            .append("config=").append(getConfigFile())
            .append(")")
            .toString();           
    }
}
