Development: Collectors
====

To create a new collector, you have to implement the [GroupGenerator interface](https://github.groupondev.com/lex/jmx-monitord/blob/master/intf/src/main/java/com/groupon/lex/metrics/GroupGenerator.java).

This interface is responsible for creating groups and metrics each time its ``getGroups()`` method is invoked.
The interface extends the ``AutoCloseable`` interface, which can be used to hook in actions when the system goes down.

To test the collector, you can simply instantiate a new Configuration, use that to create a RegistryInstance and then manually add your collector.

An easy way to implement your collector is to instantiate the ``SimpleMetric`` and ``SimpleMetricGroup`` classes.

Configuration
----

Once you're happy with the collector, you'll want to hook it up to the configuration.

At the moment, Configuration parsers are hardcoded in the grammar.
An instance of ConfigStatement is used as an intermediary, between the parser completing and the registry being instantiated.
A callback on this interface is used to instantiate your collector and add it to the MetricRegistryInstance.

The ConfigStatement needs to have a configString() method, which will output a statement that will lead to instantiating the same ConfigStatement when evaluated by the parser.
It is important that the format is very deterministic, as it is intended for debugging purposes and evaluating differences.
Please don't omit defaults, as they are useful for people to figure out how to change behaviour of your importer without the documentation handy.

Known Issues
----

The interfaces described under header *Configuration* (above), will have to change a bit, as it's currently pretty closely related to the JmxClient.
Talk to Ariane <avandersteldt@groupon.com> if you're starting to work on wiring up your collector to the configuration language.
