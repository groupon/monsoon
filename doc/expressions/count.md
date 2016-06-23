Expression: Count function
====

The count function evaluates expressions and counts the successful evaluations.
The function will accept metric wildcards.

Syntax
----

**count** **(** groupselector metric | expression [ **,** ... ] **)**

- *groupselector metric*
  A [wildcard group selector](groupselector.md) and metric name upon which the group must match.
  The match will select all groups matching the wildcard pattern, if they contain the given metric.
  The match will resolve to each metric on these groups.
- *expression*
  A metric expression.

Example
----

    sum(com.groupon.requesthandler.* count)                          # (1)
    sum(1, 2, 3 + 4)                                                 # (2)
    sum(com.groupon.requesthandler.Create count,
        com.groupon.requesthandler.Delete count)                     # (3)

1. Counts the number of request handlers, containing a metric ``count``.
2. Counts the number of expressions and returns 3.
3. Calculates the number of ``Create`` and ``Delete`` request handlers with a metric ``count``.
   Note that if one of them is absent, it will still calculate the remaining selectors.
