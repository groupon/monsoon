monsoon-auto-grapher list
====
Shows a list of all [metric groups](../expressions/groupname.md) that match a given template.

This subcommand is read-only: it parses your [monsoon configuration](../config.md) and scans your application.
The metrics from your application are then filtered, showing only matches against a template.

Syntax
----
**java -jar monsoon-auto-grapher.jar** **list**
  **[** **config=**/path/to/config.jmxmon **]**
  **[** **jmx\_host=**localhost **]**
  **[** **jmx\_port=**9999 **]**
  **[** **template**=*latency* ... **]**

- *config=*  
  The path to a [monsoon configuration file](../config.md).
  If this argument is omitted, the built-in default configuration will be used.
- *jmx\_host=*  
  The hostname where your application runs, to allow auto-grapher to scan it.
  By default, *localhost* will be used.
- *jmx\_port=*  
  The port on which JMX is bound.
  By default, port *9999* will be used, which is also the default for monsoon.
- *template=*  
  Limit scanning for metrics to any of the given templates.
  Omitting this template will allow any template to apply.
  If you use this argument to script graph creation, it is recommended to limit listing to a single statement.

*NOTE:*
Due to auto-grapher using the JMX protocol, it is recommended to be close to the machine that is used for scanning (i.e., the same datacenter).
The JMX/RMI protocol that is used, tends to need a high number of requests, which means latency has a huge effect on the time it takes to scan the application.

Example
----
The command:

    java -jar monsoon-auto-grapher.jar list \
        config=/var/groupon/jmx-monitord/queryintf_service-config.jmxmon \
        jmx_host=brands-queryintf5.snc1 jmx_port=9999 \
        template=latency

will scan the application running on *brands-queryintf5.snc1*, using JMX exposed on port *9999*, to acquire all metrics that are available.
It will apply the configuration file ``/var/groupon/jmx-monitord/queryintf_service-config.jmxmon`` to the metrics.
After acquiring the metrics this way, it attempts to match the *latency* template to them, outputting only those metric groups that match the template.

It produces this output:

    com.groupon.lex.controller.CountryBrandController.getAll
    com.groupon.lex.controller.CountryBrandController.getItem
    com.groupon.lex.controller.ForcedUpdateController.manualReload
    com.groupon.lex.controller.ForcedUpdateController.manualUpdate
    com.groupon.lex.controller.WorldwideBrandController.getAll
    com.groupon.lex.controller.WorldwideBrandController.getItem
    com.groupon.lex.heartbeat.controller.HeartbeatController.drain
    com.groupon.lex.heartbeat.controller.HeartbeatController.getHeartbeatStatus
    com.groupon.lex.heartbeat.controller.HeartbeatController.undrain
    com.groupon.lex.queryintf.CountryBrandRepository.findByUpdatedAtGreaterThanOrderByUpdatedAtDesc
    com.groupon.lex.queryintf.WorldwideBrandRepository.findByUpdatedAtGreaterThanOrderByUpdatedAtDesc
