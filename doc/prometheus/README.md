Prometheus
====

[Prometheus](http://prometheus.io/) is "An open-source service monitoring system and time series database".

Prometheus is pull based for metric collection, this means that at a set period of time call out to an http endpoint that has metrics
in a format that Prometheus can understand.

Prometheus can be setup to scrape mon-soon at any interval 1 - 60 seconds, depending on the metric resolution that is required.

It is recommended that 5 seconds be the minimum that the scrape interval is set too, as any lower than this can increase the load on mon-soon.



Invocation
----

    java -jar prometheus.jar prometheus_port=9001 prometheus_path=/metrics config=/path/to/monsoon/config.cfg
    
    If there is no port set it will default to port 9001 and if there is no path set it will default to /metrics 
