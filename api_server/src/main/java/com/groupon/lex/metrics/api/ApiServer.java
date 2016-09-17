package com.groupon.lex.metrics.api;

import com.groupon.lex.metrics.httpd.EndpointRegistration;
import java.io.IOException;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import static java.util.Collections.singletonList;
import java.util.Comparator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
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
    private final Server server_;
    private final ContextHandlerCollection context_ = new ContextHandlerCollection();
    private final ServletContextHandler servlet_handler;

    public ApiServer(Collection<? extends InetSocketAddress> addresses) {
        server_ = new Server();
        installListeners(server_, addresses);

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

    public ApiServer(InetSocketAddress address) {
        this(singletonList(address));
    }

    public void start() throws Exception {
        LOG.log(Level.INFO, "starting API server");
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
        LOG.log(Level.INFO, "registering API endpoint {0} => {1}", new Object[]{pattern, servlet});
        servlet_handler.addServlet(new ServletHolder(servlet), pattern);
    }

    private static void installListeners(Server server, Collection<? extends InetSocketAddress> addresses) {
        final List<Connector> connectors = new ArrayList<>(addresses.size());

        for (InetSocketAddress address : addresses.stream()
                .sorted(Comparator.comparing(InetSocketAddress::getAddress, new IPv4BeforeIPv6AddressComparator()))
                .collect(Collectors.toList())) {
            final ServerConnector server_connector = new ServerConnector(server);
            server_connector.setReuseAddress(true);
            if (address.getAddress() != null) {
                if (!address.getAddress().isAnyLocalAddress()) {
                    LOG.log(Level.INFO, "Binding API server address: {0}", address.getAddress().getHostAddress());
                    server_connector.setHost(address.getAddress().getHostAddress());
                }
            } else if (address.getHostString() != null) {
                LOG.log(Level.INFO, "Binding API server address name: {0}", address.getHostString());
                server_connector.setHost(address.getHostString());
            }
            LOG.log(Level.INFO, "Binding API server port: {0}", address.getPort());
            server_connector.setPort(address.getPort());
            connectors.add(server_connector);
        }

        server.setConnectors(connectors.toArray(new Connector[connectors.size()]));
    }

    /**
     * Comparator that compares addresses according to ordering:
     * <ol>
     *   <li>all IPv4 addresses</li>
     *   <li>all IPv6 addresses</li>
     *   <li>all remaining addresses</li>
     * </ol>
     */
    private static class IPv4BeforeIPv6AddressComparator implements Comparator<InetAddress> {
        @Override
        public int compare(InetAddress o1, InetAddress o2) {
            boolean o1_isIPv4 = o1 instanceof Inet4Address;
            boolean o2_isIPv4 = o2 instanceof Inet4Address;
            if (o1_isIPv4) {
                if (o2_isIPv4)
                    return 0;
                else
                    return -1;
            } else if (o2_isIPv4) {
                return 1;
            }

            boolean o1_isIPv6 = o1 instanceof Inet6Address;
            boolean o2_isIPv6 = o2 instanceof Inet6Address;
            if (o1_isIPv6) {
                if (o2_isIPv6)
                    return 0;
                else
                    return -1;
            } else if (o2_isIPv6) {
                return 1;
            }

            return 0;
        }
    }
}
