monsoon-auto-grapher list-alerts
====
Shows a list of all alerts that are configured with monsoon.

This subcommand is read-only: it scans your application and config file and reports on all declared alerts.

Syntax
----
**java -jar monsoon-auto-grapher.jar** **list-alerts**
  **[** **config=**/path/to/monsoon.jmxmon **]**
  **[** **jmx\_host=**localhost **]**
  **[** **jmx\_port=**9999 **]**

- *config=*  
  The path to a [monsoon configuration file](../config.md).
  If this argument is omitted, the built-in default configuration will be used, consequently only displaying the built-in alerts.
- *jmx\_host=*  
  The hostname where your application runs, to allow auto-grapher to scan it.
  By default, *localhost* will be used.
- *jmx\_port=*  
  The port on which JMX is bound.
  By default, port *9999* will be used, which is also the default for monsoon.

*NOTE:*
Due to auto-grapher using the JMX protocol, it is recommended to be close to the machine that is used for scanning (i.e., the same datacenter).
The JMX/RMI protocol that is used, tends to need a high number of requests, which means latency has a huge effect on the time it takes to scan the application.

Example
----
The command:

    java -jar monsoon-auto-grapher.jar list-alerts \
        config=/var/groupon/jmx-monitord/queryintf_service-config.jmxmon \
        jmx_host=brands-queryintf5.snc1 jmx_port=9999

will show (on stdout) a list of alert names, that monsoon will alert on.

It produces this output:

    QueryIntf.cache.country_brands.age_in_seconds
    QueryIntf.cache.worldwide_brands.age_in_seconds
    com.groupon.lex.controller.CountryBrandController.getAll
    com.groupon.lex.controller.CountryBrandController.getItem
    com.groupon.lex.controller.ForcedUpdateController.manualReload
    com.groupon.lex.controller.ForcedUpdateController.manualUpdate
    com.groupon.lex.controller.WorldwideBrandController.getAll
    com.groupon.lex.controller.WorldwideBrandController.getItem
    com.groupon.lex.http_1xx_rate
    com.groupon.lex.http_2xx_rate
    com.groupon.lex.http_3xx_rate
    com.groupon.lex.http_4xx_rate
    com.groupon.lex.http_5xx_rate
    monitor
    monitor.configuration_missing
    monitor.down
    qps_high
    too_many_restarts
