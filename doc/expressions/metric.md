Expression: Metric Value
====

A metric value is addressed by [selecting a Group](groupname.md) and then selecting the metric inside it.

Syntax
----

group metric

- *group*
  [Selection of a group](groupname.md) that contains the metric.
- *metric*
  The name of a metric inside that group.
Note that the group and metric are separated by a space.

If the group or metric doesn't exist, the expression will evaluate to an empty value.

Example
----

Consider a TSData (timestamps omitted) with the following metrics:

    com.groupon.requesthandlers.ApiClientsListHandler {
      50thPercentile = 201,
      75thPercentile = 219,
      95thPercentile = 260,
      98thPercentile = 350,
      99thPercentile = 541,
      999thPercentile = 871
    }

    java.lang.Memory {
      ObjectPendingFinalizationCount = 761,
      Verbose = false
    }

And the following metric expressions:

    com.groupon.requesthandlers.ApiClientsListHandler '98thPercentile'  # (1)
    com.groupon.requesthandlers.ApiClientsListHandler '99thPercentile'  # (2)
    java.lang.Memory ObjectPendingFinalizationCount                     # (3)

would evaluate to resp.:

    350                                                                 # (1)
    541                                                                 # (2)
    761                                                                 # (3)

The expressions

    com.groupon.requesthandlers.ApiClientsListHandler Verbose           # (4)
    not.a.group                                       Verbose           # (5)

would evaluate to an empty value.
Expression 4 would be empty, because the group ```com.groupon.requesthandlers.ApiClientsListHandler``` doesn't have a metric ```Verbose```.
Expression 5 would be empty, because the group ```not.a.group``` doesn't exist.
