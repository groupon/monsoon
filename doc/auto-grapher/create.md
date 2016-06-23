monsoon-auto-grapher create
====
Create graphs, by combining a [metric group](../expressions/groupname.md) and a [grapher template](list-templates.md).

The group names, for which a graph is to be created, are read from stdin.

Syntax
----
**java -jar monsoon-auto-grapher.jar**
  **[** **dc=**lup1 **dc=**snc1 ... **]**
  **[** **template=**latency ... **]**

- *dc=*  
  One or more [datacenters](list-dcs.md) in which to create the graph.
  If this argument is omitted, graphs will be created in each datacenter.
- *template=*  
  One or more [templates](list-templates.md) to apply to the metrics.
  If this argument is omitted, all templates are applied.

*NOTE:*
This subcommand does not check if it makes sense to create a graph for the given metric.
For example, if you tell it to create a *latency* graph for metric group *foobarium*, it will do so without checking:
- if *foobarium* is a real metric
- if *foobarium* holds the correct information to have this graph apply.
It shouldn't matter much, as grapher will do both checks too.

Example
----
The command:

    java -jar monsoon-auto-grapher.jar create template=latency

will read graph names from ``stdin`` and create a *latency* graph for each metric, in each datacenter.

If fed the graph ``Monsoon.Demo``, it yields the following output:

    Monsoon.Demo template=latency dc=dub1: FAILURE
      HTTP response: HTTP/1.1 200 OK
    Monsoon.Demo template=latency dc=lup1: OK
    Monsoon.Demo template=latency dc=sac1: FAILURE
      exception: s
    Monsoon.Demo template=latency dc=snc1: OK

creating latency graphs for *Monsoon.Demo* in both [lup1](http://grapher-lup1.groupondev.com/) and [snc1](http://grapher-snc1.groupondev.com/).

Bugs
----
It looks like ``snc1`` and ``sac1`` share their list of graphs.
It also looks like that is the case between ``lup1`` and ``dub1``.
From what I notice, it seems the ``dub1`` and ``sac1`` grapher instances are read-only.
This will cause graph creation to fail in those datacenters.

The ``create`` command talks to the grapher UI (as there is no API endpoint) and tries to interpret responses that were meant for a browser and human consumption.
This means that a failed graph creation may (confusingly) claim it failed with a [HTTP 200 OK](https://en.wikipedia.org/wiki/List_of_HTTP_status_codes#2xx_Success) response code.
This code is emitted by grapher if form validation fails or if *a graph with the same name* already exists.
