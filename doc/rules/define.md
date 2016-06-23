Rule: Define
====

Defined rules allow the creation of a derived metric.
A defined metric is useful to calculate information based off of gathered data and expose it to the processor (for example, grapher cannot (easily) declare a metric expression, so by declaring it here you can make the derivation show up as any other metric).

Syntax
----

**define** group\_name metric **=** expression **;**

- *group\_name*
  The name of a group.
  The *metric* will be created inside this group.
  If the group doesn't exist, it will be created.
- *metric*
  The metric name in the group under which to expose the expression.
  If the *group* already holds a metric under this name, it will be replaced.
- *expression*
  An [expression](../config.md#expressions) that yields a value for the metric.

Example
----

    define com.groupon.brands.requesthandlers.CountryBrandsFuzzySearchHandler qps =
      rate(com.groupon.brands.requesthandlers.CountryBrandsFuzzySearchHandler Count);

Define a qps rate, based on a monotonic incrementing counter.
The resulting metric will be exposed as ``com.groupon.brands.requesthandlers.CountryBrandsFuzzySearchHandler qps``.

    match com.groupon.brands.requesthandlers.* as handler {
      define ${handler} qps = rate(handler Count);
    }

Define a qps rate for all endpoints, based on a monotonic incrementing counter.
Note that the left-hand-side of the expression uses the name resolution,
while the right-hand-side of the expression uses the normal resolution.
The resulting metric will be exposed on the same group, with the name ``qps``.
