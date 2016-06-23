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

import java.lang.management.ManagementFactory;
import java.util.Hashtable;
import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanRegistrationException;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;

/**
 *
 * @author ariane
 */
public class Support {
    private final String package_name_;

    public Support(String package_name) { package_name_ = package_name; }
    public Support(Class implementation) { this(implementation.getPackage().getName()); }

    private ObjectName objectNameForUnnamedObject(Object instance) throws MalformedObjectNameException {
        Hashtable<String, String> properties = new Hashtable<String, String>();
        properties.put("type", instance.getClass().getSimpleName());
        return new ObjectName(package_name_, properties);
    }

    private ObjectName objectNameForNamedObject(Object instance, String name) throws MalformedObjectNameException {
        Hashtable<String, String> properties = new Hashtable<String, String>();
        properties.put("type", instance.getClass().getSimpleName());
        properties.put("name", name);
        return new ObjectName(package_name_, properties);
    }

    public void registerNamedObject(Object instance, String name) throws NotCompliantMBeanException, InstanceAlreadyExistsException, MBeanRegistrationException, MalformedObjectNameException {
        ManagementFactory.getPlatformMBeanServer().registerMBean(instance, objectNameForNamedObject(instance, name));
    }

    public void unregisterNamedObject(Object instance, String name) throws InstanceNotFoundException, MBeanRegistrationException, MalformedObjectNameException {
        ManagementFactory.getPlatformMBeanServer().unregisterMBean(objectNameForNamedObject(instance, name));
    }

    public void registerUnnamedObject(Object instance) throws NotCompliantMBeanException, InstanceAlreadyExistsException, MBeanRegistrationException, MalformedObjectNameException {
        ManagementFactory.getPlatformMBeanServer().registerMBean(instance, objectNameForUnnamedObject(instance));
    }

    public void unregisterUnnamedObject(Object instance) throws InstanceNotFoundException, MBeanRegistrationException, MalformedObjectNameException {
        ManagementFactory.getPlatformMBeanServer().unregisterMBean(objectNameForUnnamedObject(instance));
    }

    public String getPackageName() {
        return package_name_;
    }
}
