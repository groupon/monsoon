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
package com.groupon.lex.metrics.jmx;

import com.groupon.lex.metrics.lib.GCCloseable;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.logging.Logger;
import javax.management.ListenerNotFoundException;
import javax.management.MBeanServerConnection;
import javax.management.NotificationFilter;
import javax.management.NotificationListener;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import javax.security.auth.Subject;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;

/**
 *
 * @author ariane
 */
public class JmxClient implements AutoCloseable {
    public static interface ConnectionDecorator {
        public void apply(MBeanServerConnection conn) throws IOException;
    }

    private static final Logger LOG = Logger.getLogger(JmxClient.class.getName());
    @Getter
    private final Optional<JMXServiceURL> jmxUrl;
    @Getter(AccessLevel.PACKAGE) // For debug purposes only.
    private CompletableFuture<GCCloseable<JMXConnector>> conn_;  // conn_ == null -> connection needs recovery
    private final Collection<ConnectionDecorator> recovery_callbacks_ = new ArrayList<>();

    /**
     * Create a default client, using the default PlatformMBeanServer.
     *
     * This client is effectively connected to the JVM it is part of.
     */
    public JmxClient() {
        jmxUrl = Optional.empty();
        conn_ = CompletableFuture.completedFuture(new GCCloseable<>(new LocalhostJMXConnectorStub()));
    }

    /**
     * Create a new client, connecting to the specified URL.
     *
     * @param connection_url the JMX server URL
     * @throws IOException if the connection cannot be established
     */
    public JmxClient(@NonNull JMXServiceURL connection_url) throws IOException {
        this(connection_url, false);
    }

    /**
     * Create a new client, connecting to the specified URL.
     *
     * @param connection_url the JMX server URL
     * @param lazy if set, connection creation will be deferred
     * @throws IOException if the connection cannot be established
     */
    public JmxClient(@NonNull JMXServiceURL connection_url, boolean lazy) throws IOException {
        jmxUrl = Optional.of(connection_url);
        conn_ = null;
    }

    /**
     * Create a new client, connecting to the specified URL.
     *
     * @param connection_url the JMX server URL, example:
     * "service:jmx:rmi:///jndi/rmi://:9999/jmxrmi"
     * @throws MalformedURLException if the JMX URL is invalid
     * @throws IOException if the connection cannot be established
     */
    public JmxClient(@NonNull String connection_url) throws MalformedURLException, IOException {
        this(new JMXServiceURL(connection_url));
    }

    /**
     * Create a new client, connecting to the specified URL.
     *
     * @param connection_url the JMX server URL, example:
     * "service:jmx:rmi:///jndi/rmi://:9999/jmxrmi"
     * @param lazy if set, connection creation will be deferred
     * @throws MalformedURLException if the JMX URL is invalid
     * @throws IOException if the connection cannot be established
     */
    public JmxClient(@NonNull String connection_url, boolean lazy) throws MalformedURLException, IOException {
        this(new JMXServiceURL(connection_url), lazy);
    }

    /**
     * Returns a connection to the MBeanServer.
     *
     * Will restore connection if it has broken.
     *
     * @param threadpool The threadpool on which to asynchronously create the
     * connection.
     * @return A future for a connection to the MBean server. To cancel the
     * connection creation, cancel the future with the interruption flag set to
     * true.
     */
    public synchronized CompletableFuture<GCCloseable<JMXConnector>> getConnection(Executor threadpool) {
        if (conn_ != null && conn_.isCompletedExceptionally()) conn_ = null;
        if (conn_ != null && conn_.isDone() && !conn_.isCompletedExceptionally()) {
            try {
                conn_.get().get().getMBeanServerConnection().getMBeanCount();
            } catch (Exception ex) {
                conn_ = null;  // Connection gone bad.
            }
        }

        if (conn_ == null) {
            try {
                conn_ = CompletableFuture.completedFuture(new GCCloseable<>(JMXConnectorFactory.newJMXConnector(jmxUrl.get(), null)))
                        .thenApplyAsync(
                                (factory) -> {
                                    try {
                                        factory.get().connect();
                                        return factory;
                                    } catch (IOException ex) {
                                        throw new RuntimeException("unable to connect to " + jmxUrl.get(), ex);
                                    }
                                }, threadpool)
                        .thenApply((factory) -> {
                            try {
                                MBeanServerConnection conn = factory.get().getMBeanServerConnection();
                                for (ConnectionDecorator cb
                                             : recovery_callbacks_)
                                    cb.apply(conn);
                                return factory;
                            } catch (IOException ex) {
                                throw new RuntimeException("running recovery callbacks failed", ex);
                            }
                        });
            } catch (IOException | RuntimeException ex) {
                CompletableFuture<GCCloseable<JMXConnector>> fail = new CompletableFuture<>();
                fail.completeExceptionally(ex);
                return fail;
            }
        }
        return conn_;
    }

    /**
     * Get the connection, but don't bother with the recovery protocol if the
     * connection is lost.
     *
     * @return An optional with a connection, or empty optional indicating there
     * is no connection.
     */
    public synchronized Optional<MBeanServerConnection> getOptionalConnection() {
        if (conn_ != null && conn_.isCompletedExceptionally()) conn_ = null;
        if (conn_ != null && conn_.isDone() && !conn_.isCompletedExceptionally()) {
            try {
                MBeanServerConnection result = conn_.get().get().getMBeanServerConnection();
                result.getMBeanCount();
                return Optional.of(result);
            } catch (Exception ex) {
                conn_ = null;  // Connection gone bad.
            }
        }
        return Optional.empty();
    }

    /**
     * Add a recovery callback.
     *
     * The callback will be invoked after adding it.
     *
     * @param cb the callback that is to be added.
     */
    public synchronized void addRecoveryCallback(ConnectionDecorator cb) {
        if (cb == null) throw new NullPointerException("null callback");
        final MBeanServerConnection conn = getOptionalConnection().orElse(null);

        recovery_callbacks_.add(cb);
        try {
            if (conn != null) cb.apply(conn);
        } catch (IOException ex) {
            conn_ = null;
        }
    }

    /**
     * Remove a previously registered callback.
     *
     * @param cb the callback that is to be removed.
     * @return the callback that was removed, or null is the callback was not
     * found.
     */
    public synchronized ConnectionDecorator removeRecoveryCallback(ConnectionDecorator cb) {
        if (cb == null) throw new NullPointerException("null callback");
        return (recovery_callbacks_.remove(cb) ? cb : null);
    }

    @Override
    public synchronized void close() throws IOException {
        conn_ = null;
    }

    private static class LocalhostJMXConnectorStub implements JMXConnector {
        @Override
        public void connect() throws IOException {
        }

        @Override
        public void connect(Map<String, ?> env) throws IOException {
        }

        @Override
        public MBeanServerConnection getMBeanServerConnection() throws IOException {
            return ManagementFactory.getPlatformMBeanServer();
        }

        @Override
        public MBeanServerConnection getMBeanServerConnection(Subject delegationSubject) throws IOException {
            return getMBeanServerConnection();
        }

        @Override
        public void close() throws IOException {
        }

        @Override
        public void addConnectionNotificationListener(NotificationListener listener, NotificationFilter filter, Object handback) {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public void removeConnectionNotificationListener(NotificationListener listener) throws ListenerNotFoundException {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public void removeConnectionNotificationListener(NotificationListener l, NotificationFilter f, Object handback) throws ListenerNotFoundException {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public String getConnectionId() throws IOException {
            return "localhost";
        }
    }
}
