Auto-grapher Pipe Example
====
Suppose we want to create latency graphs for every appropriate metric.
We start the application (in this case, brands-modintf) in our datacenter.

Then, on a host in the same datacenter (this could be the brands-modintf host, but any host will do) we run:

    java -jar monsoon-auto-grapher.jar list \
        jmx_host=brands-modintf1-uat.snc1 \
        jmx_port=9999 \
        template=latency | \
    java -jar monsoon-auto-grapher.jar create \
        template=latency

The first statement loads all metrics from the application and tries to match each of them with the *latency* template.
Only metrics that match the template requirements are emitted on ``stdout``.

The output from the first statement is piped into the second statement.
The second statement creates a graph in all datacenters, by applying the *latency* template on each metric fed via ``stdin``.

If you want to filter, you can ofcourse use ``grep`` in between the listing of the metrics and the creation of graphs.
Likewise, you can feed the ``create`` statement a list of metrics you type in/from a file, and it will create those.
