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

import com.groupon.lex.metrics.MetricRegistryInstance;
import com.groupon.lex.metrics.api.ApiServer;
import com.groupon.lex.metrics.config.Configuration;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ContextHandler;
/**
 *
 * @author nils
 */
public class PrometheusServer {
    private static final Logger LOG = Logger.getLogger(PrometheusServer.class.getName());
    private final static String _PACKAGE_NAME = PrometheusServer.class.getPackage().getName();
    private static MetricRegistryInstance registry_;

    private final static Map<String, BiConsumer<PrometheusConfig, String>> createPrometheusConfig_ = new HashMap<String, BiConsumer<PrometheusConfig, String>>() {{
            put("config=", (PrometheusConfig, config) -> {
            try {
                PrometheusConfig.setConfigFile(config);
            } catch (IOException ex) {
                LOG.log(Level.SEVERE, "error setting config file", ex);
                throw new RuntimeException("error setting config file", ex);
            }
        });
        put("prometheus_path=", PrometheusConfig::setPath);
        put("prometheus_port=", (PrometheusConfig, port) -> {
            try {
                PrometheusConfig.setPort(Short.valueOf(port));
            } catch (NumberFormatException ex) {
                throw new RuntimeException("error processing Prometheus port " + port, ex);
            }
        });
    }};

    private static PrometheusConfig createPrometheusConfig(String[] args) {
        PrometheusConfig cfg = new PrometheusConfig();
        for (String arg : args) {
            createPrometheusConfig_.entrySet().stream()
                    .filter((option) -> (arg.startsWith(option.getKey())))
                    .forEach((option) -> {
                        option.getValue().accept(cfg, arg.substring(option.getKey().length()));
                    });
        }
        LOG.log(Level.INFO, "Prometheus config: {0}", cfg);
        return cfg;
    }

    public static void main(String[] args) throws Exception {
        final ApiServer api = new ApiServer(new InetSocketAddress(9998));

        PrometheusConfig cfg = createPrometheusConfig(args);
        Configuration _cfg = cfg.getConfiguration();
        registry_ = _cfg.create(api);

        api.start();
        Runtime.getRuntime().addShutdownHook(new Thread(api::close));

        Server server = new Server(cfg.getPort());
        ContextHandler context = new ContextHandler();
        context.setClassLoader(Thread.currentThread().getContextClassLoader());
        context.setContextPath(cfg.getPath());
        context.setHandler(new DisplayMetrics(registry_));
        server.setHandler(context);
        server.start();
        server.join();
    }
}
