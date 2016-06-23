Rule: Alert
====

Alerts rules declare an alerting condition.
The end result is usually that someone gets paged or e-mailed.
Or that a red box appears on some HTML page.

Syntax
----

**alert** alertname **if** predicate [ **for** duration ] [ **message** "message\_string" ] **;**

- *alertname*
  The name as which the alert is to be exposed.
  Alert names have their own name space and don't affect metric groups with the same name.
  It is recommended, if an alert corresponds to data specific to a metric group, to share the same name.
  The alert name is a sequence of at least one identifier, separated by dots.
- *predicate*
  A condition upon which the alert should trigger.
- *duration*
  An optional duration for which the alert should trigger, before it starts paging someone.
  If no duration is specified, an alert will start paging immediately.
- *message\_string*
  A string that is added to the alert metadata.
  It is recommended to make this a link to a page that gives more information on the alert and how to deal with it.
  If this argument is omitted, the *predicate* is used as metadata.

Alert States
----

An alert can be in 4 states.

- **OK**
  The alert is healthy.
  This state is reached if the predicate evaluates to ```false```.
- **TRIGGERING**
  The alert is triggering, but hasn't been doing so for the required duration.
  An alert in the triggering state, will change into the **FIRING** state, if it has spent the configured duration in this state.
  Note that if no duration is specified or a duration of ```0s``` is specified, the alert will skip this state and go straight to the **FIRING** state.
- **FIRING**
  The alert is triggering and has been doing so for the required duration.
  The exact action depends on the processor implementation, but it will usually involve nagging a human.
- **UNKNOWN**
  The alert is in an unknown state.
  This state is only reached if the predicate resolution failed.
  Note that if any of the required parameters in the predicate are *nil* or absent, the predicate will resolve to *nil*, which in turn will trigger this state.
  This state may indicate a problem with the configuration (for example, a typo in the expression) or could mean the application didn't initialize all its metrics.

Example
----

    alert never.let.me.sleep
    if true;

    alert monitor.fine
    if false;

    alert com.groupon.brands.requesthandlers.CountryBrandsFuzzySearchHandler
    if com.groupon.brands.requesthandlers.CountryBrandsFuzzySearchHandler '99thPercentile' > 8000
    for 10m;

    alert qps_high
    if com.groupon.brands.Qps OneMinuteRate > 100
    message "Link to Documentation/qps_high.md";

Creates the following alerts:
- **never.let.me.sleep**
  Which is always alerting.
  I can't figure out why you would want this...
- **monitor.fine**
  Which never alerts.
  If your alerting implementation checks for stale alerts, this is a useful alert to check if the monitor is functioning.
- **com.groupon.brands.requesthandlers.CountryBrandsFuzzySearchHandler**
  An alert that alerts if the Fuzzy Search handler has a 99th percentile latency of 8 seconds.
  (This assumes ofcourse, that there is a group with that metric and that the metric exposes values in milliseconds.)
  The alert will only trigger if the latency exceeds 8000 msec, for 10 minutes.
- **qps_high**
  Alerts if the queries exceeds 100/second.
  The alert doesn't have a duration, so it'll trigger immediately, which is probably not something you want...
  The alert does however have a message, so if it fires, the alert (e-mail, pagerduty, nagios info field...) will contain this link.
