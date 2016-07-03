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
import com.groupon.lex.metrics.PullProcessorPipeline;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

/**
 *
 * @author nolofsson
 */
public class DisplayMetrics extends AbstractHandler {
    private final PullProcessorPipeline registry_;
    public DisplayMetrics(PullProcessorPipeline registry ){
        registry_ = registry;
    }

    /**
     *
     * @param target
     * @param baseRequest
     * @param request
     * @param response
     * @throws IOException
     * @throws ServletException
     */
    public void handle(String target,
                       Request baseRequest,
                       HttpServletRequest request,
                       HttpServletResponse response)
        throws IOException, ServletException {

        Stream<PrometheusMetric> metrics;
        try {
            metrics = PrometheusMetrics.filteredMetrics(registry_);
        } catch (Exception ex) {
            Logger.getLogger(DisplayMetrics.class.getName()).log(Level.SEVERE, null, ex);
            response.sendError(500, ex.toString());
            return;
        }

        response.setContentType("text/plain;charset=utf-8");
        response.setStatus(HttpServletResponse.SC_OK);
        baseRequest.setHandled(true);
        try {
            for (PrometheusMetric m: metrics.collect(Collectors.toList())) {
                response.getWriter().println(m);
            }
        } catch (IOException ex) {
            Logger.getLogger(PrometheusServer.class.getName()).log(Level.SEVERE, null, ex);
            response.sendError(500, ex.toString());
            return;
        }
    }
}
