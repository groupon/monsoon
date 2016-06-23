Expression: Min/Max functions
====

The min and max functions calculates the minimum and maximum value of one or more metrics.
The function will accept metric wildcards.

The min/max functions will compare metrics based on numeric coercion, i.e. ``false`` will be treated as ``0`` and ``true`` as 1.

Syntax
----

**min** **(** groupselector metric | expression [ **,** ... ] **)** [ **by**|**without** **(** tag [ **,** ... ] **)** ] [ **keep_common** ]  
**max** **(** groupselector metric | expression [ **,** ... ] **)** [ **by**|**without** **(** tag [ **,** ... ] **)** ] [ **keep_common** ]

- *groupselector metric*
  A [wildcard group selector](groupselector.md) and metric name upon which the group must match.
  The match will select all groups matching the wildcard pattern, if they contain the given metric.
  The match will resolve to each metric on these groups.
- *expression*
  A metric expression.
- *tag*
  Group the min/max based on each tag.
  This is comparable to using ``min ... group by`` in SQL.

Example
----

    max(com.groupon.requesthandler.* count)                          # (1)
    min(1, 2, 3 + 4)                                                 # (2)
    max(com.groupon.requesthandler.Create count,
        com.groupon.requesthandler.Delete count)                     # (3)
    min(endpoint qps) by (host) keep_common                          # (4)

1. Evaluates the maximum of ``count`` in all request handlers.
2. Evaluates the minimum of the expressions ``1``, ``2``, ``3 + 4`` and yields the value 1.
3. Calculates the maximum of ``count`` in the ``Create`` and ``Delete`` request handlers.
   Note that if one of them is absent, it will still calculate over the remaining selectors.
4. Calculates the lowest QPS for each host, by comparing the endpoints for each host.
