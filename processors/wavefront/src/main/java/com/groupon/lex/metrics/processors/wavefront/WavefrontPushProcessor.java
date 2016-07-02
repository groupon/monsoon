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
package com.groupon.lex.metrics.processors.wavefront;

import com.groupon.lex.metrics.GroupName;
import com.groupon.lex.metrics.PushProcessor;
import com.groupon.lex.metrics.timeseries.Alert;
import com.groupon.lex.metrics.timeseries.TimeSeriesCollection;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.nio.charset.Charset;
import java.util.Map;
import static java.util.Objects.requireNonNull;
import java.util.logging.Level;
import java.util.logging.Logger;

public class WavefrontPushProcessor implements PushProcessor {
    private static final Logger LOG = Logger.getLogger(WavefrontPushProcessor.class.getName());
    public static final Charset CHARSET = Charset.forName("UTF-8");
    public static final int DEFAULT_PORT = 2878;
    private final int port_;
    private final String host_;

    public WavefrontPushProcessor() {
        this("localhost", DEFAULT_PORT);
    }

    public WavefrontPushProcessor(String host, int port) {
        host_ = requireNonNull(host);
        port_ = port;
        if (port <= 0 || port > 65535)
            throw new IllegalArgumentException("invalid TCP port");
    }

    public String getHost() { return host_; }
    public int getPort() { return port_; }

    @Override
    public void accept(TimeSeriesCollection tsdata, Map<GroupName, Alert> alerts) {
        /*
         * Write a line for each metric out to wavefront.
         * Since the documentation for wavefront doesn't claim to reply, we don't bother reading the reply either.
         */
        try (Socket socket = new Socket(host_, port_)) {
            try (OutputStreamWriter out = new OutputStreamWriter(socket.getOutputStream(), CHARSET)) {
                tsdata.getTSValues().stream()
                        .flatMap(WavefrontStrings::wavefrontLine)
                        .forEach(s -> {
                            try {
                                out.write(s);
                                out.write('\n');
                            } catch (IOException ex) {
                                LOG.log(Level.SEVERE, "error while writing to wavefront socket", ex);
                                throw new RuntimeException("error while writing metrics to wavefront", ex);
                            }
                        });
            }
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, "IO error with wavefront socket", ex);
            throw new RuntimeException(ex);
        }
    }
}
