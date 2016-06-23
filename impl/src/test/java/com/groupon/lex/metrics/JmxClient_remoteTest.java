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

import java.io.IOException;
import java.io.Serializable;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.RMISocketFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import javax.management.MBeanServer;
import javax.management.MBeanServerConnection;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXConnectorServerFactory;
import javax.management.remote.JMXServiceURL;
import org.junit.After;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author ariane
 */
public class JmxClient_remoteTest {
    private static final AtomicInteger port_counter = new AtomicInteger(3000 - 1);

    private static final class JmxServer implements AutoCloseable {
        public static class LoopbackSocketFactory extends RMISocketFactory implements Serializable {
            @Override
            public ServerSocket createServerSocket(int port) throws IOException {
                return new ServerSocket(port, 5, InetAddress.getByName("127.0.0.1"));
            }

            @Override
            public Socket createSocket(String host, int port) throws IOException {
                // just call the default client socket factory
                return RMISocketFactory.getDefaultSocketFactory().createSocket(host, port);
            }

            @Override
            public boolean equals(Object other) {
                return other instanceof LoopbackSocketFactory;
            }

            @Override
            public int hashCode() {
                return 0;
            }
        }

        private final int port;
        private JMXConnectorServer cs;
        public final JMXServiceURL url;
        private boolean started = false;
        private final Map<String,Object> env;
        private final MBeanServer mbs;

        public JmxServer() throws RemoteException, IOException {
            port = port_counter.addAndGet(1);

            final Registry registry = LocateRegistry.createRegistry(port);
            mbs = ManagementFactory.getPlatformMBeanServer();
            env = new HashMap<>();

            /*
             * I don't know which to force here, but the default works (why?)
             * If this ever breaks, it'll need some digging into the RMI code base to figure out what the connection provider is, for non-ssl connections.
             */
//            final RMIClientSocketFactory csf = new LoopbackSocketFactory();
//            final RMIServerSocketFactory ssf = new LoopbackSocketFactory();
//            env.put(RMIConnectorServer.RMI_CLIENT_SOCKET_FACTORY_ATTRIBUTE, csf);
//            env.put(RMIConnectorServer.RMI_SERVER_SOCKET_FACTORY_ATTRIBUTE, ssf);

            env.put("jmx.remote.local.only", Boolean.TRUE);
            env.put("jmx.remote.authenticate", Boolean.FALSE);
            env.put("jmx.remote.ssl", Boolean.FALSE);

            url = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://127.0.0.1:" + port + "/jmxrmi");
        }

        public synchronized void stop() throws IOException {
            if (started) {
                started = false;
                cs.stop();
            }
        }

        public synchronized void start() throws IOException {
            if (!started) {
                cs = JMXConnectorServerFactory.newJMXConnectorServer(url, env, mbs);
                cs.start();
                started = true;
            }
        }

        public synchronized boolean isStarted() { return started; }

        @Override
        public void close() throws IOException {
            stop();
        }
    }
    
    private JmxServer jmx_server = null;

    @Before
    public void setup() throws Exception {
        jmx_server = new JmxServer();
    }

    @After
    public void cleanup() throws Exception {
        if (jmx_server != null)
            jmx_server.close();
    }

    @Test
    public void connection() throws Exception {
        jmx_server.start();
        try (JmxClient jmx_client = new JmxClient(jmx_server.url)) {
            MBeanServerConnection connection = jmx_client.getConnection();

            assertNotNull(connection);
        }
    }

    @Test(expected = IOException.class)
    public void connect_no_server() throws Exception {
        assert(!jmx_server.isStarted());
        try (JmxClient jmx_client = new JmxClient(jmx_server.url)) {
            MBeanServerConnection connection = jmx_client.getConnection();  // Must throw IOException

            assertNull(connection);
        }
    }

    @Test
    public void connect_late_server() throws Exception {
        assert(!jmx_server.isStarted());
        try (JmxClient jmx_client = new JmxClient(jmx_server.url)) {
            /* First, start client, while server isn't up yet. */
            Exception first_connect_exception = null;
            try {
                jmx_client.getConnection();
            } catch (Exception ex) {
                first_connect_exception = ex;
            }
            if (first_connect_exception == null) fail("Test requires connection to fail initially.");

            jmx_server.start();
            MBeanServerConnection connection = jmx_client.getConnection();

            assertNotNull(connection);
        }
    }

    @Test(expected = IOException.class)
    public void connect_and_server_goes_away() throws Exception {
        jmx_server.start();
        try (JmxClient jmx_client = new JmxClient(jmx_server.url)) {
            try {
                jmx_client.getConnection();  // Ensure connection exists.
            } catch (Throwable t) {
                /* Ensure we catch any exception from getConnection, just in case. */
                throw new AssertionError("getConnection() may not throw at this point", t);
            }

            jmx_server.stop();  // Server going down...
            jmx_client.getConnection();
        }
    }

    @Test
    public void connect_and_server_restarts() throws Exception {
        jmx_server.start();
        try (JmxClient jmx_client = new JmxClient(jmx_server.url)) {
            jmx_client.getConnection();

            /* Restart JMX server. */
            jmx_server.stop();
            jmx_server.start();

            MBeanServerConnection connection = jmx_client.getConnection();
            assertNotNull(connection);
        }
    }

    @Test
    public void opt_connection() throws Exception {
        jmx_server.start();
        try (JmxClient jmx_client = new JmxClient(jmx_server.url)) {
            Optional<MBeanServerConnection> connection = jmx_client.getOptionalConnection();

            assertNotEquals("Since the server is up, the connection must be present.", Optional.empty(), connection);
        }
    }

    @Test
    public void opt_connect_no_server() throws Exception {
        assert(!jmx_server.isStarted());
        try (JmxClient jmx_client = new JmxClient(jmx_server.url)) {
            Optional<MBeanServerConnection> connection = jmx_client.getOptionalConnection();  // May not throw.

            assertEquals("Since the server is down, the connection must be absent.", Optional.empty(), connection);
        }
    }

    @Test
    public void opt_connect_late_server() throws Exception {
        assert(!jmx_server.isStarted());
        try (JmxClient jmx_client = new JmxClient(jmx_server.url)) {
            /* First, start client, while server isn't up yet. */
            Exception first_connect_exception = null;
            try {
                jmx_client.getConnection();
            } catch (Exception ex) {
                first_connect_exception = ex;
            }
            if (first_connect_exception == null) fail("Test requires connection to fail initially.");

            jmx_server.start();
            Optional<MBeanServerConnection> connection = jmx_client.getOptionalConnection();
            assertEquals("Asked for connection only if one was thought to exist -- it didn't exist, so no connection shall be returned.", Optional.empty(), connection);

            jmx_client.getConnection();  // Force construction of connection.
            connection = jmx_client.getOptionalConnection();
            assertNotEquals("Since the server is up and a new connection was forcibly created, the connection must now be present.", Optional.empty(), connection);
        }
    }

    @Test
    public void opt_connect_and_server_goes_away() throws Exception {
        jmx_server.start();
        try (JmxClient jmx_client = new JmxClient(jmx_server.url)) {
            try {
                jmx_client.getConnection();  // Ensure connection exists.
            } catch (Throwable t) {
                /* Ensure we catch any exception from getConnection, just in case. */
                throw new AssertionError("getConnection() may not throw at this point", t);
            }

            jmx_server.stop();  // Server going down...
            Optional<MBeanServerConnection> connection = jmx_client.getOptionalConnection();

            assertEquals("getOptionalConnection() must detect that the server went away.", Optional.empty(), connection);
        }
    }

    @Test
    public void opt_connect_and_server_restarts() throws Exception {
        jmx_server.start();
        try (JmxClient jmx_client = new JmxClient(jmx_server.url)) {
            MBeanServerConnection old_connection = jmx_client.getConnection();

            /* Restart JMX server. */
            jmx_server.stop();
            jmx_server.start();

            Optional<MBeanServerConnection> connection = jmx_client.getOptionalConnection();
            if (connection.isPresent())
                assertSame("java performed reconnect in the backgroun?  interesting...", old_connection, connection.get());
            else
                assertEquals("server going away is correctly detected, but won't reconnect since it was not asked to", Optional.empty(), connection);
        }
    }

    @Test
    public void connect_callback() throws Exception {
        class InitCount_ {
            public int count = 0;
        }
        InitCount_ init_count = new InitCount_();

        jmx_server.start();
        try (JmxClient jmx_client = new JmxClient(jmx_server.url)) {
            jmx_client.addRecoveryCallback((conn) -> ++init_count.count);
        }

        assertEquals("Initialization callback was called exactly once", 1, init_count.count);
    }

    @Test
    public void connect_callback_no_server() throws Exception {
        class InitCount_ {
            public int count = 0;
        }
        InitCount_ init_count = new InitCount_();

        assert(!jmx_server.isStarted());
        try (JmxClient jmx_client = new JmxClient(jmx_server.url)) {
            jmx_client.addRecoveryCallback((conn) -> ++init_count.count);
        }

        assertEquals("Initialization callback was called exactly never", 0, init_count.count);
    }

    @Test
    public void connect_callback_late_server() throws Exception {
        class InitCount_ {
            public int count = 0;
        }
        InitCount_ init_count = new InitCount_();

        assert(!jmx_server.isStarted());
        try (JmxClient jmx_client = new JmxClient(jmx_server.url)) {
            jmx_client.addRecoveryCallback((conn) -> ++init_count.count);
            assertEquals("Initialization callback was not yet called", 0, init_count.count);

            jmx_server.start();  // Start server.
            jmx_client.getConnection();  // Force connection to come into existence.
        }

        assertEquals("Initialization callback was called exactly once", 1, init_count.count);
    }

    @Test
    public void connect_callback_and_server_restarts() throws Exception {
        class InitCount_ {
            public int count = 0;
        }
        InitCount_ init_count = new InitCount_();

        jmx_server.start();
        try (JmxClient jmx_client = new JmxClient(jmx_server.url)) {
            jmx_client.addRecoveryCallback((conn) -> ++init_count.count);
            MBeanServerConnection old_connection = jmx_client.getConnection();
            assertEquals("1 connection, 1 callback", 1, init_count.count);

            /* Restart JMX server. */
            jmx_server.stop();
            jmx_server.start();

            MBeanServerConnection connection = jmx_client.getConnection(); // Force reconnect.
            if (old_connection != connection)
                assertEquals("Callback was invoked upon reconnect", 2, init_count.count);
        }
    }

    @Test
    public void callback_on_live_connection() throws Exception {
        class InitCount_ {
            public int count = 0;
        }
        InitCount_ init_count = new InitCount_();

        jmx_server.start();
        try (JmxClient jmx_client = new JmxClient(jmx_server.url)) {
            jmx_client.addRecoveryCallback((conn) -> ++init_count.count);
        }
        assertEquals(1, init_count.count);
    }

    @Test
    public void callback_on_dead_connection() throws Exception {
        class InitCount_ {
            public int count = 0;
        }
        InitCount_ init_count = new InitCount_();

        assert(!jmx_server.isStarted());
        try (JmxClient jmx_client = new JmxClient(jmx_server.url)) {
            jmx_client.addRecoveryCallback((conn) -> ++init_count.count);
        }
        assertEquals(0, init_count.count);
    }

    @Test
    public void callback_on_undetected_dead_connection() throws Exception {
        class InitCount_ {
            public int count = 0;
        }
        InitCount_ init_count = new InitCount_();

        jmx_server.start();
        try (JmxClient jmx_client = new JmxClient(jmx_server.url)) {
            jmx_server.stop();  // JmxClient hasn't detected server went away.

            jmx_client.addRecoveryCallback((conn) -> ++init_count.count);
        }
        assertEquals("Callback wasn't called, because connection was down at registration time.", 0, init_count.count);
    }

    @Test
    public void connect_callback_throws() throws Exception {
        jmx_server.start();
        try (JmxClient jmx_client = new JmxClient(jmx_server.url)) {
            jmx_client.addRecoveryCallback((conn) -> { throw new IOException("muhahaha"); });

            assertEquals("Connection only exists if all callbacks were successful", Optional.empty(), jmx_client.getOptionalConnection());
        }
    }

    @Test
    public void connect_callback_late_server_throws() throws Exception {
        try (JmxClient jmx_client = new JmxClient(jmx_server.url)) {
            jmx_client.addRecoveryCallback((conn) -> { throw new IOException("muhahaha"); });

            jmx_server.start();
            try {
                jmx_client.getConnection();  // Force connection to start.
            } catch (IOException ex) {
                /* ignore */
            }
            assertEquals("Connection only exists if all callbacks were successful", Optional.empty(), jmx_client.getOptionalConnection());
        }
    }
}
