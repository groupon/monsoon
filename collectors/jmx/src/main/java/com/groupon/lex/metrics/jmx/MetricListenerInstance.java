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
import com.groupon.lex.metrics.Tags;
import com.groupon.lex.metrics.jmx.JmxClient.ConnectionDecorator;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import static java.util.Collections.singleton;
import static java.util.Collections.unmodifiableList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import static java.util.Objects.requireNonNull;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
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

/**
 * A JMX bean listener, that detects new MBeans with a given name pattern and
 * registers them as JMX based metrics.
 *
 * @author ariane
 */
public class MetricListenerInstance implements MetricListener, GroupGenerator, AutoCloseable {
    private final Collection<ObjectName> filter_;
    private final Map<ObjectName, MBeanGroupInstance> detected_groups_ = new HashMap<>();
    private boolean is_enabled_ = false;
    @Getter
    private final JmxClient connection;
    private final ConnectionDecorator decorator_;
    private final NotificationListener listener_;
    @Getter
    private final List<String> subPath;
    @Getter
    private final Tags tags;
    private int failCount = 0;

    public MetricListenerInstance(JmxClient conn, Collection<ObjectName> filter, List<String> sub_path, Tags tags) throws IOException, InstanceNotFoundException {
        if (filter.isEmpty())
            throw new IllegalArgumentException("empty filter");
        filter_ = filter;
        connection = conn;
        subPath = unmodifiableList(new ArrayList<>(sub_path));
        this.tags = requireNonNull(tags);

        listener_ = (Notification notification, Object handback) -> {
            MBeanServerNotification mbs = (MBeanServerNotification) notification;

            if (!filter_.stream()
                    .anyMatch((f) -> f.apply(mbs.getMBeanName()))) {
                Logger.getLogger(getClass().getName()).log(Level.FINER, "notification for {0} ignored: does not match filter {1}", new Object[]{mbs.getMBeanName(), filter_});
                return;
            }

            if (null != mbs.getType()) {
                switch (mbs.getType()) {
                    case MBeanServerNotification.REGISTRATION_NOTIFICATION:
                        Logger.getLogger(getClass().getName()).log(Level.INFO, "MBean Registered [{0}]", mbs.getMBeanName());
                        onNewMbean(mbs.getMBeanName());
                        break;
                    case MBeanServerNotification.UNREGISTRATION_NOTIFICATION:
                        Logger.getLogger(getClass().getName()).log(Level.INFO, "MBean Unregistered [{0}]", mbs.getMBeanName());
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
                Logger.getLogger(MetricListenerInstance.class.getName()).log(Level.SEVERE, "your MBeanServer is not compliant", ex);
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
            Logger.getLogger(MetricListenerInstance.class.getName()).log(Level.WARNING, "skipping registration of {0}: already present", obj);
            return;
        }

        MBeanGroupInstance instance = new MBeanGroupInstance(connection, obj, subPath, tags);
        detected_groups_.put(obj, instance);
        Logger.getLogger(MetricListenerInstance.class.getName()).log(Level.INFO, "registered metrics for {0}: {1}", new Object[]{obj, instance});
    }

    /**
     * Respond to MBeans being removed.
     *
     * @param obj The name of the MBean being removed.
     */
    private synchronized void onRemovedMbean(ObjectName obj) {
        if (!detected_groups_.keySet().contains(obj)) {
            Logger.getLogger(MetricListenerInstance.class.getName()).log(Level.WARNING, "skipping de-registration of {0}: not present", obj);
            return;
        }

        MBeanGroupInstance instance = detected_groups_.get(obj);
        detected_groups_.remove(obj);
        Logger.getLogger(MetricListenerInstance.class.getName()).log(Level.INFO, "de-registered metrics for {0}: {1}", new Object[]{obj, instance});
    }

    @Override
    public synchronized void enable() throws IOException {
        if (is_enabled_)
            return;

        connection.addRecoveryCallback(decorator_);
        is_enabled_ = true;
        Logger.getLogger(MetricListenerInstance.class.getName()).log(Level.INFO, "enabled");
    }

    @Override
    public synchronized void disable() throws IOException {
        if (!is_enabled_)
            return;

        is_enabled_ = false;

        try {
            Optional<MBeanServerConnection> optionalConnection = connection.getOptionalConnection();
            try {
                if (optionalConnection.isPresent())
                    optionalConnection.get().removeNotificationListener(MBeanServerDelegate.DELEGATE_NAME, listener_);
            } catch (InstanceNotFoundException ex) {
                Logger.getLogger(MetricListenerInstance.class.getName()).log(Level.SEVERE, "MBean Platform server has mysteriously disappeared...", ex);
                throw new IllegalStateException("MBean Platform server has mysteriously disappeared", ex);
            } catch (ListenerNotFoundException ex) {
                Logger.getLogger(MetricListenerInstance.class.getName()).log(Level.SEVERE, "Listener was not found...", ex);
            }
        } finally {
            /* Remove gathered items even if connection logic fails. */
            Collection<ObjectName> allNames = new ArrayList<ObjectName>() {
                {
                    addAll(detected_groups_.keySet());
                }
            };
            allNames.forEach(this::onRemovedMbean);

            Logger.getLogger(MetricListenerInstance.class.getName()).log(Level.INFO, "disabled");
        }
    }

    @Override
    public ObjectName[] getFilter() {
        return filter_.toArray(new ObjectName[0]);
    }

    @Override
    public boolean isEnabled() {
        return is_enabled_;
    }

    @Override
    public ObjectName[] getDetectedNames() {
        return detected_groups_.keySet().toArray(new ObjectName[0]);
    }

    @Override
    public void close() throws IOException {
        disable();
    }

    @Override
    public Collection<CompletableFuture<Collection<MetricGroup>>> getGroups(ExecutorService executor, CompletableFuture<?> timeout) {
        CompletableFuture<Collection<MetricGroup>> future
                = CompletableFuture.supplyAsync(() -> readMetricGroups(timeout), executor);

        timeout.whenCompleteAsync((ignoredValue, ignoredExc) -> {
            if (!future.isDone()) {
                future.cancel(true);
                if (++failCount > 5)
                    connection.rejectCurrentConnection();
            }
        }, executor);

        return singleton(future);
    }

    private synchronized Collection<MetricGroup> readMetricGroups(CompletableFuture<?> timeout) {
        try {
            /*
             * Force the connection to open, even if there are no groups to scan.
             *
             * If there are no groups registered, the connection opening won't be triggered otherwise.
             * And if that isn't triggered, registering groups won't happen either.
             */
            connection.getConnection();
        } catch (IOException ex) {
            /*
             * Connection down, can't collect any data.
             */
            throw new RuntimeException(ex.getMessage(), ex);
        }

        final List<MetricGroup> groups = new ArrayList<>(detected_groups_.size());
        for (MBeanGroupInstance group : detected_groups_.values()) {
            if (timeout.isDone())
                throw new CancellationException("timed out");
            group.getMetrics().ifPresent(groups::add);
        }
        return groups;
    }
}
