monsoon-auto-grapher list-yaml-alerts
====
Shows a list of all alerts that are configured with monsoon.

This subcommand is read-only: it scans your application and config file and reports on all declared alerts.

The [list-alerts](list-alerts.md) command is similar to this command.
This command formats all alerts in a way to easily put them in your host.yml/hostclass.yml.
It also uses the correct escaping for names in monsoon and names in grapher/nagios.

Syntax
----
**java -jar monsoon-auto-grapher.jar** **list-yaml-alerts**
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

    java -jar monsoon-auto-grapher.jar list-yaml-alerts \
        config=/var/groupon/jmx-monitord/queryintf_service-config.jmxmon \
        jmx_host=brands-queryintf5.snc1 jmx_port=9999

will show (on stdout) a list of alert names, that monsoon will alert on.
The output will be formatted as a yaml snippet for inclusion in host.yml/hostclass.yml.

It produces this output:

    monitors:
      QueryIntf_cache_country_brands_age_in_seconds:
        run_every: 60
      QueryIntf_cache_worldwide_brands_age_in_seconds:
        run_every: 60
      com_groupon_lex_controller_CountryBrandController_getAll:
        run_every: 60
      com_groupon_lex_controller_CountryBrandController_getItem:
        run_every: 60
      com_groupon_lex_controller_ForcedUpdateController_manualReload:
        run_every: 60
      com_groupon_lex_controller_ForcedUpdateController_manualUpdate:
        run_every: 60
      com_groupon_lex_controller_WorldwideBrandController_getAll:
        run_every: 60
      com_groupon_lex_controller_WorldwideBrandController_getItem:
        run_every: 60
      com_groupon_lex_http_1xx_rate:
        run_every: 60
      com_groupon_lex_http_2xx_rate:
        run_every: 60
      com_groupon_lex_http_3xx_rate:
        run_every: 60
      com_groupon_lex_http_4xx_rate:
        run_every: 60
      com_groupon_lex_http_5xx_rate:
        run_every: 60
      monitor:
        run_every: 60
      monitor_configuration_missing:
        run_every: 60
      monitor_down:
        run_every: 60
      qps_high:
        run_every: 60
      too_many_restarts:
        run_every: 60
