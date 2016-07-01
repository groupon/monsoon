<img src="Monsoon-Logo.png" Monsoon-Logo.png width="200">
====
Mon-soon
====

An extensible monitor system that checks java processes and exposes metrics based on them.

The project is extensible and allows for new datasources to be created, so it's not just Java it can monitor.

It should be easy to implement a new processor, to direct monitored data to other systems.

Usage
----

The system uses processors to hook up to external metric and alerting systems.
Processors are what turns the system from a pile of code into a useful component of your production deployment.

Configuration
----

Please refer to the [configuration documentation](doc/config.md).

----

Prometheus Integration
----

The Prometheus exporter will take all jmx metrics and convert them to a format that Prometheus can scrap
[prometheus.jar](doc/prometheus/README.md).



Development
----

Development documentation has its [own section](doc/dev/README.md).
