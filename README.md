<img src="Monsoon-Logo.png" Monsoon-Logo.png width="200">


Mon-soon
====

An extensible monitor system that checks java processes and exposes metrics based on them.

The project is extensible and allows for new datasources to be created, so it's not just Java it can monitor.

It should be easy to implement a new processor, to direct monitored data to other systems.

Monsoon supports:

1. collectd Metrics 
2. JMX local and remote.
3. URL HTTP (think a simple pingdom)
4. Scrape a JSON endpoint over HTTP, I.E Hadoop metricz endpoint.
5. A history module that will write time series to disk.
6. It has metrics API endpoint for graphing ( work in progress).
7. It also has a prometheus exporter.

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

[![Build Status](https://travis-ci.org/groupon/monsoon.svg?branch=master)](https://travis-ci.org/groupon/monsoon)
