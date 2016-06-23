Rule: Alias
====

The alias rule is a convenience statement, to reduce the need to type long path names every time.

The identifiers created with an alias statement are not metric groups and won't get emitted to the [Processor](../datamodel.md#processing).

Syntax
----

**alias** groupname **as** identifier **;**

- *groupname*
  The [name of a group](../expressions/groupname.md).
- *identifier*
  A variable name under which to bind the group.

Example
----

Consider a [TSData](../datamodel.md#time-series) with the following data:

    com.groupon.brands.strsearch.CachedBasicNormalizedLevenshteinNameSearch.WorldwideBrandNameSearch.loadAllUncached {
      50thPercentile 240,
      75thPercentile 241,
      95thPercentile 273,
      98thPercentile 301,
      99thPercentile 353,
      999thPercentile 798
    }

and the need to refer to this metric a lot.
Since it's inconvenient to type out the entire name in the expressions each time, we want to have a shorter name to refer to it.

    alias com.groupon.brands.strsearch.CachedBasicNormalizedLevenshteinNameSearch.WorldwideBrandNameSearch.loadAllUncached
    as WWB_namesearch_ldUncached;

Now, instead of writing future rules like this:

    alert my_alert
    if com.groupon.brands.strsearch.CachedBasicNormalizedLevenshteinNameSearch.WorldwideBrandNameSearch.loadAllUncached as WWB_namesearch_ldUncached '50thPercentile' > 500
    for 10m;

we can instead write:

    alert my_alert
    if WWB_namesearch_ldUncached '50thPercentile' > 500
    for 10m;

which is much more readable.
