Expression: Sum function
====

The sum function calculates the sum of one or more metrics.
The function will accept metric wildcards.

The sum function differs from simply adding up each element, in that it will not yield ``nil`` upon evaluating absent values, instead it will return ``0``.

Syntax
----

**sum** **(** groupselector metric | expression [ **,** ... ] **)** [ **by**|**without** **(** tag [ **,** ... ] **)** ] [ **keep_common** ]

- *groupselector metric*
  A [wildcard group selector](groupselector.md) and metric name upon which the group must match.
  The match will select all groups matching the wildcard pattern, if they contain the given metric.
  The match will resolve to each metric on these groups.
- *expression*
  A metric expression.
- *tag*
  Group the sum based on each tag.
  This is comparable to using ``sum ... group by`` in SQL.

Example
----

    sum(com.groupon.requesthandler.* count)                          # (1)
    sum(1, 2, 3 + 4)                                                 # (2)
    sum(com.groupon.requesthandler.Create count,
        com.groupon.requesthandler.Delete count)                     # (3)
    sum(endpoint qps) by (host) keep_common                          # (4)

1. Evaluates the sum of ``count`` in all request handlers.
2. Evaluates the sum of the expressions ``1``, ``2``, ``3 + 4`` and yields the value 10.
3. Calculates the sum of ``count`` in the ``Create`` and ``Delete`` request handlers.
   Note that if one of them is absent, it will still calculate the remaining selectors.
4. Calculates the QPS for each host, by summing the endpoints for each host.
