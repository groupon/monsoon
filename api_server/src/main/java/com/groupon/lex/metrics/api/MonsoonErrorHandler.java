/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.groupon.lex.metrics.api;

import java.io.IOException;
import java.io.Writer;
import javax.servlet.http.HttpServletRequest;
import org.eclipse.jetty.http.HttpStatus;
import org.eclipse.jetty.server.handler.ErrorHandler;

/**
 *
 * @author ariane
 */
public class MonsoonErrorHandler extends ErrorHandler {
    @Override
    protected void writeErrorPage(HttpServletRequest request, Writer writer, int code, String message, boolean showStacks) throws IOException {
        if (message == null)
            message=HttpStatus.getMessage(code);

        writer.write(code);
        writer.write(" -- ");
        writer.write(message);
    }
}
