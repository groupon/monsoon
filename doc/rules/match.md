Rule: Match
====

A match statement is a way to apply a set of rules to each group that matches a specific selection criterion.

Multiple match statements may be nested.

Syntax
----

**match** selector [ **,** selector **,** ...]
[ **where** predicate ]
**{**
rules...
**}**

with *selector* being:
- groupselector **as** groupselectorname
- groupselector metricselector **as** groupselectorname **,** metricselectorname

- *groupselector*
  A group name pattern, to match groups.
  Please refer to the documentation on [group selectors](../expressions/groupselector.md) for more information.
- *metricselector*
  A metric name pattern, to match groups.
  Please refer to the documentation on [group selectors](../expressions/groupselector.md) for more information.
- *groupselectorname*
  A variable name to bind the group to, so it can be referred to inside the body of the match statement.
- *metricselectorname*
  A variable name to bind the metric to, so it can be referred to inside the body of the match statement.
- *predicate*
  A predicate limiting the groups on which the body is applied.
  A group matching multiple patterns should almost certainly use a where predicate,
  as the *match* statement will evaluate the cartesian product of possible matches.
  *selectorname* can be used to refer to the captured groups.
- *rules...*
  Zero or more rules that are applied.
  Inside the block, the *selectorname* is bound to each group.
  In places where a name is required, the syntax ```${selectorname}``` can be used to get the name of the group.

Cartesian Product
----

A match statement may have multiple selectors, in which case each selector is evaluated and the match will replay against the cartesian product of the selectors.
For example:

    match com.** as a, com.** as b { ... }

when the following metric groups are present:

    com.groupon.CoolName
    com.groupon.BoringName
    com.groupon.NoName

will apply the following combinations:

    (a, b) = (com.groupon.CoolName,   com.groupon.CoolName)
    (a, b) = (com.gorupon.BoringName, com.groupon.CoolName)
    (a, b) = (com.gorupon.NoName,     com.groupon.CoolName)
    (a, b) = (com.groupon.CoolName,   com.groupon.BoringName)
    (a, b) = (com.gorupon.BoringName, com.groupon.BoringName)
    (a, b) = (com.gorupon.NoName,     com.groupon.BoringName)
    (a, b) = (com.groupon.CoolName,   com.groupon.NoName)
    (a, b) = (com.gorupon.BoringName, com.groupon.NoName)
    (a, b) = (com.gorupon.NoName,     com.groupon.NoName)

and replay the rules ```...``` for each of those combinations.

Note that there is no exclusion between groups with the same name.

The combined syntax is equivalent to:

    match com.** as a {
      match com.** as b {
        ...
      }
    }

except that the combined form is more efficient.

Example
----

    match com.groupon.brands.requesthandlers.* as handler {
      alert ${handler} if handler '99thPercentile' > handler latency_critical for 10m;
    }

This rule matches all handlers with the pattern ```com.groupon.brands.requesthandlers.*```.
Each matched group will result in an alert being declared, which:
- has the same name as the group:
  because the expression ```${handler}``` is replaced with the name of the matched group.
- checks the ```99thPercentile``` value against the ```latency_critical``` metrics of that group:
  because the expression ```handler``` evaluates to the matched group.

    match com.groupon.brands.requesthandlers.* as handler {
      alert ${handler[-1]}.latency if handler '99thPercentile' > handler latency_critical for 10m;
    }

This rule is similar to the one above, except that it only uses the last path element of the captured group.
In this case, it's useful because it strips the unnecessary noise from the alert name.

    match ** 99thPercentile as handler, pct {
      alert ${handler[-1]}.latency if pct > 250 for 10m;
    }

This rule matches every metric group that exposes a 99thPercentile and declares an alert for each.
