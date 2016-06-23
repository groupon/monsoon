package com.groupon.lex.metrics.api;

import com.groupon.lex.metrics.httpd.EndpointRegistration;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.server.handler.gzip.GzipHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.resource.Resource;

public class ApiServer implements AutoCloseable, EndpointRegistration {
    private static final Logger LOG = Logger.getLogger(ApiServer.class.getName());
    private final static Charset UTF8 = Charset.forName("UTF-8");
    private final Server server_ = new Server(9998);
    private final ContextHandlerCollection context_ = new ContextHandlerCollection();
    private final ServletContextHandler servlet_handler;

    public ApiServer() {
        final HandlerList chain = new HandlerList();
        {
            final Handler index_html_handler = new AbstractHandler() {
                private final Resource index_html_ = Resource.newClassPathResource("/www/index.html");

                @Override
                public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
                    response.setContentType("text/html");
                    response.setCharacterEncoding("UTF-8");
                    response.setStatus(HttpServletResponse.SC_OK);
                    baseRequest.setHandled(true);
                    index_html_.writeTo(response.getOutputStream(), 0, -1);
                }
            };
            ContextHandler path = new ContextHandler("/fe");
            path.setHandler(index_html_handler);
            chain.addHandler(path);
        }

        {
            ResourceHandler resHandler = new ResourceHandler();
            resHandler.setBaseResource(Resource.newClassPathResource("/www"));

            ContextHandler path = new ContextHandler();
            path.setContextPath("/");
            path.setHandler(resHandler);
            chain.addHandler(path);
        }

        {
            servlet_handler = new ServletContextHandler();
            servlet_handler.setContextPath("/api");
            context_.addHandler(servlet_handler);
            chain.addHandler(context_);
        }

        final GzipHandler gzip_handler = new GzipHandler();
        gzip_handler.setHandler(chain);
        server_.setHandler(gzip_handler);

        server_.addBean(new MonsoonErrorHandler());
    }

    public void start() throws Exception {
        server_.start();
    }

    @Override
    public void close() {
        try {
            server_.stop();
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, "unable to stop API server", ex);
            return;
        }
    }

    @Override
    public void addEndpoint(String pattern, HttpServlet servlet) {
        servlet_handler.addServlet(new ServletHolder(servlet), pattern);
    }
}
