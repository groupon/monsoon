Expression: Rate function
====

The rate function is used to evaluate the rate of change of a metric value over time.

The effect is the same as the pseudo function:

    function rate(expr) {
        return (expr.current_value() - expr.previous_value()) /
          expr.interval_in_seconds();
    }

Syntax
----

**rate** **(** expression **)**

- *expression*
  An expression resolving to a value.
  Note that the expression must be coercible to a floating point value, i.e. we can't derive the rate of change for a string.

Example
----

    rate(42)                                                         # (1)
    rate(com.groupon.requests count)                                 # (2)

1. Calculates the rate of change of the value 42.
   Since this value is always the same between collections, the rate function will return 0.
2. Calculate the rate of change for the ``com.groupon.requests count`` metric.
   The output of this invocation gives the requests per second.
