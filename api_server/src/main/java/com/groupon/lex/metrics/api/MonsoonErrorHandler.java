/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.groupon.lex.metrics.api;

import java.io.IOException;
import java.io.Writer;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.servlet.RequestDispatcher;
import javax.servlet.http.HttpServletRequest;
import org.eclipse.jetty.http.HttpStatus;
import org.eclipse.jetty.server.handler.ErrorHandler;

/**
 *
 * @author ariane
 */
public class MonsoonErrorHandler extends ErrorHandler {
    private static final Logger LOG = Logger.getLogger(MonsoonErrorHandler.class.getName());

    @Override
    protected void writeErrorPage(HttpServletRequest request, Writer writer, int code, String message, boolean showStacks) throws IOException {
        if (message == null)
            message=HttpStatus.getMessage(code);

        // Write a very short body (this makes people pushing with collectd happy).
        writer.write(String.valueOf(code));
        writer.write(" -- ");
        writer.write(message);

        // Log the request error.
        Throwable th = (Throwable)request.getAttribute(RequestDispatcher.ERROR_EXCEPTION);
        if (th != null) {
            final String http_request = request.getMethod() + ' ' + request.getRequestURI() + ' ' + request.getProtocol();
            LOG.log(Level.WARNING, http_request, th);
        }
    }
}
