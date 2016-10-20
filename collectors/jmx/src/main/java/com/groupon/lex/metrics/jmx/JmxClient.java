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

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.management.MBeanServerConnection;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import lombok.Getter;

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
    private Optional<JMXConnector> jmx_factory_;
    private MBeanServerConnection conn_;  // conn_ == null -> connection needs recovery
    private final Collection<ConnectionDecorator> recovery_callbacks_ = new ArrayList<>();

    /**
     * Create a default client, using the default PlatformMBeanServer.
     *
     * This client is effectively connected to the JVM it is part of.
     */
    public JmxClient() {
        jmxUrl = Optional.empty();
        jmx_factory_ = Optional.empty();
        conn_ = ManagementFactory.getPlatformMBeanServer();
    }

    /**
     * Create a new client, connecting to the specified URL.
     *
     * @param connection_url the JMX server URL
     * @param lazyConnect if true, the connection will not be established at construction time, but once the first time the connection is requested.
     * @throws IOException if the connection cannot be established
     */
    public JmxClient(JMXServiceURL connection_url, boolean lazyConnect) throws IOException {
        jmxUrl = Optional.of(connection_url);
        jmx_factory_ = Optional.of(JMXConnectorFactory.newJMXConnector(jmxUrl.get(), null));
        if (lazyConnect) {
            conn_ = null;
        } else {
            try {
                jmx_factory_.get().connect();
            } catch (IOException ex) {
                LOG.log(Level.WARNING, "unable to connect");
                conn_ = null;
            }
            try {
                conn_ = jmx_factory_.get().getMBeanServerConnection();
            } catch (IOException ex) {
                LOG.log(Level.WARNING, "unable to connect");
                try {
                    jmx_factory_.get().close();
                } catch (IOException ex1) {
                    LOG.log(Level.WARNING, "unable to close failed connection attempt", ex1);
                    /* Eat exception. */
                }
                conn_ = null;
            }
        }
    }

    /**
     * Create a new client, connecting to the specified URL.
     *
     * @param connection_url the JMX server URL
     * @throws IOException if the connection cannot be established
     */
    public JmxClient(JMXServiceURL connection_url) throws IOException {
        this(connection_url, false);
    }

    /**
     * Create a new client, connecting to the specified URL.
     *
     * @param connection_url the JMX server URL, example:
     * "service:jmx:rmi:///jndi/rmi://:9999/jmxrmi"
     * @param lazyConnect if true, the connection will not be established at construction time, but once the first time the connection is requested.
     * @throws MalformedURLException if the JMX URL is invalid
     * @throws IOException if the connection cannot be established
     */
    public JmxClient(String connection_url, boolean lazyConnect) throws MalformedURLException, IOException {
        this(new JMXServiceURL(connection_url), lazyConnect);
    }

    /**
     * Create a new client, connecting to the specified URL.
     *
     * @param connection_url the JMX server URL, example:
     * "service:jmx:rmi:///jndi/rmi://:9999/jmxrmi"
     * @throws MalformedURLException if the JMX URL is invalid
     * @throws IOException if the connection cannot be established
     */
    public JmxClient(String connection_url) throws MalformedURLException, IOException {
        this(connection_url, false);
    }

    /**
     * Returns a connection to the MBeanServer.
     *
     * Will restore connection if it has broken.
     *
     * @return A connection to the MBean server.
     * @throws IOException if the connection couldn't be established.
     */
    public synchronized MBeanServerConnection getConnection() throws IOException {
        {
            Optional<MBeanServerConnection> optionalConnection = getOptionalConnection();
            if (optionalConnection.isPresent()) return optionalConnection.get();
        }

        /* Re-create factory, because apparently they cannot re-create a broken connection. */
        if (jmxUrl.isPresent())
            jmx_factory_ = Optional.of(JMXConnectorFactory.newJMXConnector(jmxUrl.get(), null));  // May throw

        /* Attempt to recreate the connection and replay the listeners. */
        JMXConnector factory = jmx_factory_.orElseThrow(() -> new IOException("cannot recover from broken connection to local MBean Server"));
        factory.connect();  // May throw
        try {
            conn_ = factory.getMBeanServerConnection();
            for (ConnectionDecorator cb : recovery_callbacks_)
                cb.apply(conn_);
        } catch (IOException ex) {
            /* Cleanup on initialization failure. */
            conn_ = null;
            factory.close();
            throw ex;
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
    public Optional<MBeanServerConnection> getOptionalConnection() {
        if (conn_ != null) {
            /* Verify the connection is valid. */
            try {
                conn_.getMBeanCount();
            } catch (IOException ex) {
                try {
                    /* Connection is bad/lost. */
                    jmx_factory_
                            .orElseThrow(() -> new IOException("cannot recover from broken connection to local MBean Server"))
                            .close();
                } catch (IOException ex1) {
                    Logger.getLogger(JmxClient.class.getName()).log(Level.SEVERE, "failed to close failing connection", ex1);
                }
                conn_ = null;
            }
        }
        return Optional.ofNullable(conn_);
    }

    /**
     * Add a recovery callback.
     *
     * The callback will be invoked after adding it.
     *
     * @param cb the callback that is to be added.
     */
    public void addRecoveryCallback(ConnectionDecorator cb) {
        if (cb == null) throw new NullPointerException("null callback");
        final MBeanServerConnection conn = getOptionalConnection().orElse(null);

        recovery_callbacks_.add(cb);
        try {
            if (conn != null) cb.apply(conn);
        } catch (IOException ex) {
            Logger.getLogger(getClass().getName()).log(Level.FINE, "connection error while adding callback", ex);
            /* Silently ignore: connection is bad and next time a connection is requested, the callback will be invoked. */

            if (jmx_factory_.isPresent()) { // Cleanup on initialization failure.
                conn_ = null;
                try {
                    jmx_factory_.get().close();
                } catch (IOException ex1) {
                    LOG.log(Level.WARNING, "failed to close failing connection", ex1);
                }
            }
        }
    }

    /**
     * Remove a previously registered callback.
     *
     * @param cb the callback that is to be removed.
     * @return the callback that was removed, or null is the callback was not
     * found.
     */
    public ConnectionDecorator removeRecoveryCallback(ConnectionDecorator cb) {
        if (cb == null) throw new NullPointerException("null callback");
        return (recovery_callbacks_.remove(cb) ? cb : null);
    }

    @Override
    public synchronized void close() throws IOException {
        if (jmx_factory_.isPresent()) jmx_factory_.get().close();
        conn_ = null;
    }

    /**
     * Rejects the current connection. Does nothing if the local JMX is used.
     */
    public synchronized void rejectCurrentConnection() {
        if (jmx_factory_.isPresent()) {
            conn_ = null;
            try {
                jmx_factory_.get().close();
            } catch (IOException ex) {
                LOG.log(Level.WARNING, "failed to close failing connection", ex);
            }
        }
    }
}
