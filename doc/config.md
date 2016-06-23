Configuration
====

The configuration language consists of two distinct entities: collectors and rules.

The collectors declare where metrics come from, while the rules decide how the metrics are processed.

Configuration statements always end with either a semi-colon (```statement ;```) or a body delimited by curly braces (```statement { ... }```).

Since the configuration languages deals very much with operations on metrics/TSData, it is recommended to read up on the [datamodel](datamodel.md) first.

Comments
----

Anything starting at the **#** character, until the end of the line, is treated as a comment.
Comments are skipped during rule evaluation.

Collectors
----

In general, the import statement looks like:

    collect collector_name ...;
    collect collector_name { ... }

Please refer to the [documentation on each collector](collectors/index.md) for details.

Rules
----

Rules take action on data gathered by the importers.
The end goal of rules is to allow transformations on metrics and to generate alerts.

- [alert](rules/alert.md)
  Describes how to process alerts.
- [alias](rules/alias.md)
  Refer to a group by a convenient (and shorter) identifier.
- [constant](rules/constant.md)
  Describes simple constants that are to be added to existing groups.
- [define](rules/define.md)
  Define new metrics, based on expressions from other metrics.
- [match](rules/match.md)
  Describes how to apply a group of statements onto a wildcard of groups.
  Think of it as a foreach body applied to a select (as in SQL select) of groups.
- [tag](rules/tag.md)
  Describes how to assign extra tags to a group, by evaluating an expression.

Expressions
----

Rules make use of expressions.
Most simple arithmatic and boolean logic has been implemented.

- [metric](expressions/metric.md)
  Describes how to read a metric.
- [group selector](expressions/groupselector.md)
  Describes how to select groups using a pattern.
- [operators](expressions/operators.md)
  Describes the operators that are implemented.
- [count function](expressions/count.md)
  Describes the counting function.
- [sum function](expressions/sum.md)
  Describes the summation function.
- [avg function](expressions/avg.md)
  Describes the average function.
- [min/max functions](expressions/min_max.md)
  Describes the min and max functions.
- [percentile\_agg function](expressions/percentile_agg.md)
  Describes the percentile aggregate.
- [str function](expressions/str.md)
  Describes the string concatenation function.
- [rate function](expressions/rate.md)
  Describes the rate function, which is used to convert a monotonic increasing counter into a rate per second value.
- [regexp function(expressions/regexp.md)
  Describes how to reformat a string, using a regular expression and back-references.
- [tag function](expressions/tag.md)
  Describes the tag extraction function.

Example
----

This is an example of a configuration for a java application written using [spring-boot](http://projects.spring.io/spring-boot/).
Comments to explain how it works are inline.

    # Declare which raw metrics to gether.
    #
    collect jmx_listener "java.lang:*",      # Gather all MXBeans in java.lang
                         "java.lang.*:*",    # and all MXBeans in subpackages.
                         "metrics:name=*",   # Gather Codahale metrics.
                         "java.nio:*";       # Also gather java NIO data.

    # Add a latency_critical constant to each of the request handlers.
    # These are later tested with alerts.
    #
    # If someone introduces a new handler, but forgets to add monitoring,
    # this rule will allow for a default latency measurement.
    #
    match com.groupon.lex.controller.*.* as handler {
      constant handler latency_critical 20000;
    }

    # Specify better constants for selected request handlers.
    #
    # Note that the constants inside the match statement above are overriden
    # using these values.
    #
    constant com.groupon.lex.controller.MerchantController.listDeals latency_critical 2500;
    constant com.groupon.lex.controller.MerchantController.uploadAsset latency_critical 5000;

    # Declare an alert for each handler, based on the 50thPercentile as it was
    # measured by the application, compared against the corresponding
    # critical value declared as a constant above.
    #
    # The syntax ${handler} allows us to get the name of the handler,
    # so the alert will have a meaningful name that is related to its metrics.
    #
    match com.groupon.lex.controller.*.* as handler {
      alert ${handler} if handler '50thPercentile' > handler latency_critical;
    }

    # Declare an alert for the rate of queries.
    # Spring-boot exposes a QPS metric for each endpoint,
    # so we have to sum them together for the entire application.
    #
    # By comparing it against a value derived from the number of cores
    # of the machine, we can use the same logic regardless of the number
    # of CPUs the machine actually has.
    #
    alert qps_high
    if  ( com.groupon.lex.controller.MerchantController.cloneDeal OneMinuteRate
        + com.groupon.lex.controller.MerchantController.createDeal OneMinuteRate
        + com.groupon.lex.controller.MerchantController.deleteDeal OneMinuteRate
        + com.groupon.lex.controller.MerchantController.findDeal OneMinuteRate
        + com.groupon.lex.controller.MerchantController.getCategories OneMinuteRate
        + com.groupon.lex.controller.MerchantController.getMessages OneMinuteRate
        + com.groupon.lex.controller.MerchantController.json2csv OneMinuteRate
        + com.groupon.lex.controller.MerchantController.listDeals OneMinuteRate
        + com.groupon.lex.controller.MerchantController.csv2json OneMinuteRate
        + com.groupon.lex.controller.MerchantController.setMessages OneMinuteRate
        + com.groupon.lex.controller.MerchantController.updateDeal OneMinuteRate
        + com.groupon.lex.controller.MerchantController.uploadAsset OneMinuteRate)
      > 10 * java.lang.OperatingSystem AvailableProcessors;  # 10 qps per core

    # Furthermore, we measure the response codes from the webserver.
    # The application has a very low QPS rate (see above).
    # The purpose of these alerts is not to alert if the service is overloaded
    # (that is the role of the 'qps_high' alert).
    # Instead, it is to alert if the service seems to be misbehaving.
    #
    # As the application never responds with 100 and 300 status codes,
    # we set their threshold to zero.
    #
    alert com.groupon.lex.dealcentre.http_1xx_rate
    if com.groupon.lex.dealcentre.http_1xx_rate OneMinuteRate > 0;
    alert com.groupon.lex.dealcentre.http_2xx_rate
    if com.groupon.lex.dealcentre.http_2xx_rate OneMinuteRate > 50;
    alert com.groupon.lex.dealcentre.http_3xx_rate
    if com.groupon.lex.dealcentre.http_3xx_rate OneMinuteRate > 0;
    alert com.groupon.lex.dealcentre.http_4xx_rate
    if com.groupon.lex.dealcentre.http_4xx_rate OneMinuteRate > 10;
    alert com.groupon.lex.dealcentre.http_5xx_rate
    if com.groupon.lex.dealcentre.http_5xx_rate OneMinuteRate > 10;
