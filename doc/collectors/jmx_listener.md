Collector: Jmx Listener
====

The JMX listener is responsible for querying a Java JVM for JMX beans and exposing them as metrics.
It uses MBean Object Name patterns to filter which metrics to expose.
To see a list of JMX beans exposed in your java application, use the ```mbeans``` tab in ```jconsole```.

The JMX listener works best with an instrumented binary, for example using Codahale metrics with the JMX-exporter enabled.

Syntax
----

**collect** **jmx_listener** patterns... **;**

Patterns: one or more MBean Object Name patterns.
Please refer to the [documentation on ObjectName](https://docs.oracle.com/javase/8/docs/api/javax/management/ObjectName.html) for how the patterns are formatted.
All MBeans matching the patterns supplied will be recorded as groups.

Example:

    collect jmx_listener "java.lang:*", "java.lang.*:*", "metrics:name=*", "java.nio:*";

This would import all MBeans from the JVM and metrics exposed by Codahale.
It is equivalent to specifying them separately:

    collect jmx_listener "java.lang:*";
    collect jmx_listener "java.lang.*:*";
    collect jmx_listener "metrics:name=*";
    collect jmx_listener "java.nio:*";

It is recommended to combine them together, as each jmx_listener declaration yields a separate listener, as the combined listener has slightly less overhead than each listener separately.

Group Name
----

The object names are turned into metric names, according to the following rules:
* if the MBean Object Name contains a ```name=...``` property, it will be exposed as a metric under that name.
* if the MBean Object Name contains a ```type=...``` property, it will be exposed under that name, prefixed by its package name.
* otherwise, the MBean will not be exposed.

Example:
- The MBean ```java.lang:type=MemoryPool,name=Compressed Class Space``` will be exposed as ```Compressed Class Space```, according to the first rule, because it has a ```name=...``` property.
- The MBean ```java.lang:type=Runtime``` will be exposed as ```java.lang.Runtime```, according to the second rule.
  The package name (the part before the colon character) is prepended to the type property.

Metric Name
----

The individual properties of the MBean will be exposed as metrics.
The name of the metric is the same as the name of the property.

The metric value will be resolved according to the following rules:
- A boolean value, if the property holds a boolean value.
- An integral number, if the property holds an integral value; the value will be converted to a 64-bit signed integral, for the datamodel.
- A floating point number, if the property holds a floating point value; the value will be converted to a 64-bit IEEE double, for the datamodel.
- A string, if the property holds a string value.
- Empty metric value, if the property holds an object reference or a serialized object (composite MBean).
  *This means nested structs are currently not supported.*
