Rule: Constant
====

Constants are group decorators, adding trivial [values](../expressions/value.md).

Constants are useful to add to metric groups, to later compare values inside the metric group with that constant.

Constants thus added are also emitted as metrics.

Syntax
----

**constant** groupname metricname value **;**

- *groupname*
  The name of a group, upon which to apply the constant.
- *metricname*
  The name of a metric.
- *value*
  A value to assign to the metricname.

If the *groupname* exists, a metric will be added, with the given *value* under the name specified as *metricname*.
If the group does not exist, no action will be taken.
If the metric already exists, it will be overwritten.

Example
----

    constant qps critical 200;
    constant specific.endpoint.qps critical 20;

If the group *qps* exists, will add or replace a metric *critical* with the value *200*.
If the group *qps* doesn't exist, the constant won't be added.

Similarly for the *specific.endpoint.qps* group, which will have *critical* = *20*.

If, later in the configuration file, a match statement declares an alert for both,
the alert can be made to evaluate against different values:

    match **.qps as qps_metric {
      alert ${qps_metric}
      if qps_metric value > qps_metric critical
      for 10m;
    }

This will evaluate to:

    alert qps
    if qps value > qps critical  # qps critical = 200
    for 10m;

    alert specific.endpoint.qps
    if specific.endpoint.qps value > specific.endpoint.qps critical  # = 20
    for 10m;
