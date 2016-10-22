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

import com.groupon.lex.metrics.GroupGenerator;
import com.groupon.lex.metrics.MetricGroup;
import com.groupon.lex.metrics.jmx.JmxClient.ConnectionDecorator;
import com.groupon.lex.metrics.resolver.NamedResolverMap;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import static java.util.Collections.singleton;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.management.InstanceNotFoundException;
import javax.management.ListenerNotFoundException;
import javax.management.MBeanServerConnection;
import javax.management.MBeanServerDelegate;
import javax.management.MBeanServerNotification;
import javax.management.Notification;
import javax.management.NotificationListener;
import javax.management.ObjectName;
import lombok.Getter;
import lombok.NonNull;

/**
 * A JMX bean listener, that detects new MBeans with a given name pattern and
 * registers them as JMX based metrics.
 *
 * @author ariane
 */
public class MetricListener implements GroupGenerator {
    private static final Logger LOG = Logger.getLogger(MetricListener.class.getName());
    private final Collection<ObjectName> filter_;
    private final Map<ObjectName, MBeanGroup> detected_groups_ = new HashMap<ObjectName, MBeanGroup>();
    private boolean is_enabled_ = false;
    @Getter
    private final JmxClient connection;
    private final ConnectionDecorator decorator_;
    private final NotificationListener listener_;
    @Getter
    private final NamedResolverMap resolvedMap;

    public MetricListener(@NonNull JmxClient conn, @NonNull Collection<ObjectName> filter, @NonNull NamedResolverMap resolvedMap) throws IOException, InstanceNotFoundException {
        if (filter.isEmpty())
            throw new IllegalArgumentException("empty filter");
        filter_ = filter;
        connection = conn;
        this.resolvedMap = resolvedMap;

        listener_ = (Notification notification, Object handback) -> {
            MBeanServerNotification mbs = (MBeanServerNotification) notification;

            if (!filter_.stream()
                    .anyMatch((f) -> f.apply(mbs.getMBeanName()))) {
                LOG.log(Level.FINER, "notification for {0} ignored: does not match filter {1}", new Object[]{mbs.getMBeanName(), filter_});
                return;
            }

            if (null != mbs.getType()) {
                switch (mbs.getType()) {
                    case MBeanServerNotification.REGISTRATION_NOTIFICATION:
                        LOG.log(Level.INFO, "MBean Registered [{0}]", mbs.getMBeanName());
                        onNewMbean(mbs.getMBeanName());
                        break;
                    case MBeanServerNotification.UNREGISTRATION_NOTIFICATION:
                        LOG.log(Level.INFO, "MBean Unregistered [{0}]", mbs.getMBeanName());
                        onRemovedMbean(mbs.getMBeanName());
                        break;
                }
            }
        };

        decorator_ = (MBeanServerConnection mbsc) -> {
            /* Clear gathered metrics, since they'll belong to the old connection and won't function properly. */
            Collection<ObjectName> allNames = new ArrayList<ObjectName>() {
                {
                    addAll(detected_groups_.keySet());
                }
            };
            allNames.forEach(this::onRemovedMbean);

            /* Initialize listener for new MBean registrations. */
            try {
                mbsc.addNotificationListener(MBeanServerDelegate.DELEGATE_NAME, listener_, null, null);
            } catch (InstanceNotFoundException ex) {
                LOG.log(Level.SEVERE, "your MBeanServer is not compliant", ex);
                throw new IOException("your MBeanServer is not compliant", ex);
            }

            /* Add all objects that the listener misses, due to them existing already. */
            for (ObjectName f : filter_) {
                mbsc.queryNames(f, null)
                        .forEach((objname) -> onNewMbean(objname));
            }
        };
    }

    /**
     * Respond to MBeans being added.
     *
     * @param obj The name of the MBean being added.
     */
    private synchronized void onNewMbean(ObjectName obj) {
        if (detected_groups_.keySet().contains(obj)) {
            LOG.log(Level.WARNING, "skipping registration of {0}: already present", obj);
            return;
        }

        MBeanGroup instance = new MBeanGroup(obj, resolvedMap);
        detected_groups_.put(obj, instance);
        LOG.log(Level.FINE, "registered metrics for {0}: {1}", new Object[]{obj, instance});
    }

    /**
     * Respond to MBeans being removed.
     *
     * @param obj The name of the MBean being removed.
     */
    private synchronized void onRemovedMbean(ObjectName obj) {
        if (!detected_groups_.keySet().contains(obj)) {
            LOG.log(Level.WARNING, "skipping de-registration of {0}: not present", obj);
            return;
        }

        MBeanGroup instance = detected_groups_.get(obj);
        detected_groups_.remove(obj);
        LOG.log(Level.FINE, "de-registered metrics for {0}: {1}", new Object[]{obj, instance});
    }

    public synchronized void enable() throws IOException {
        if (is_enabled_) return;

        connection.addRecoveryCallback(decorator_);
        is_enabled_ = true;
        LOG.log(Level.FINER, "enabled");
    }

    public synchronized void disable() throws IOException {
        if (!is_enabled_) return;

        is_enabled_ = false;

        try {
            Optional<MBeanServerConnection> optionalConnection = connection.getOptionalConnection();
            try {
                if (optionalConnection.isPresent())
                    optionalConnection.get().removeNotificationListener(MBeanServerDelegate.DELEGATE_NAME, listener_);
            } catch (InstanceNotFoundException ex) {
                LOG.log(Level.SEVERE, "MBean Platform server has mysteriously disappeared...", ex);
                throw new IllegalStateException("MBean Platform server has mysteriously disappeared", ex);
            } catch (ListenerNotFoundException ex) {
                LOG.log(Level.SEVERE, "Listener was not found...", ex);
            }
        } finally {
            /* Remove gathered items even if connection logic fails. */
            Collection<ObjectName> allNames = new ArrayList<ObjectName>() {
                {
                    addAll(detected_groups_.keySet());
                }
            };
            allNames.forEach(this::onRemovedMbean);

            LOG.log(Level.FINER, "disabled");
        }
    }

    public ObjectName[] getFilter() {
        return filter_.toArray(new ObjectName[0]);
    }

    public boolean isEnabled() {
        return is_enabled_;
    }

    public ObjectName[] getDetectedNames() {
        return detected_groups_.keySet().toArray(new ObjectName[0]);
    }

    @Override
    public void close() throws IOException {
        disable();
    }

    @Override
    public synchronized Collection<CompletableFuture<? extends Collection<? extends MetricGroup>>> getGroups(Executor threadpool, CompletableFuture<TimeoutObject> timeout) {
        /*
         * Force the connection to open, even if there are no groups to scan.
         *
         * If there are no groups registered, the connection opening won't be triggered otherwise.
         * And if that isn't triggered, registering groups won't happen either.
         */
        CompletableFuture<List<MetricGroup>> future = connection.getConnection(threadpool)
                .applyToEitherAsync(
                        timeout.thenApply(timeoutObject -> (MBeanServerConnection) null),
                        conn -> {
                            if (conn == null)
                                throw new RuntimeException("connection unavailable");

                            List<MetricGroup> result = new ArrayList<>();
                            for (MBeanGroup group : detected_groups_.values()) {
                                if (timeout.isDone())
                                    throw new RuntimeException("timed out");
                                group.getMetrics(conn).ifPresent(result::add);
                            }
                            return result;
                        },
                        threadpool);
        timeout.thenAccept(timeoutObject -> future.cancel(true));
        return singleton(future);
    }
}
