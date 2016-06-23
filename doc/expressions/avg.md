Expression: Avg function
====

The avg function calculates the average of one or more metrics.
The function will accept metric wildcards.

The average of multiple metrics will skip ``nil`` elements and strings.
If the set of numbers is empty, the result of the average will be ``nil``.

Syntax
----

**avg** **(** groupselector metric | expression [ **,** ... ] **)** [ **by**|**without** **(** tag [ **,** ... ] **)** ] [ **keep_common** ]

- *groupselector metric*
  A [wildcard group selector](groupselector.md) and metric name upon which the group must match.
  The match will select all groups matching the wildcard pattern, if they contain the given metric.
  The match will resolve to each metric on these groups.
- *expression*
  A metric expression.
- *tag*
  Group the average based on each tag.
  This is comparable to using ``avg ... group by`` in SQL.

Example
----

    avg(com.groupon.requesthandler.* count)                          # (1)
    avg(1, 2, 3 + 4)                                                 # (2)
    avg(com.groupon.requesthandler.Create count,
        com.groupon.requesthandler.Delete count)                     # (3)
    avg(endpoint qps) by (host) keep_common                          # (4)

1. Evaluates the average of ``count`` in all request handlers.
2. Evaluates the average of the expressions ``1``, ``2``, ``3 + 4`` and yields the value 3.333.
3. Calculates the average of ``count`` in the ``Create`` and ``Delete`` request handlers.
   Note that if one of them is absent, it will still calculate the remaining selectors.
4. Calculates the average QPS per host.
