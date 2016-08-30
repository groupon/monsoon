Expression: Name function
====

The ``name`` function is used to evaluate a resolved name.

Syntax
----

**name** **(** selector **)**

- *selector*
  A (group or metric) name.
  The group/metric does not have to exist.

The function will evaluate the name and try to coerce the name into a sensible value.
- ``true`` and ``false`` will be turned into their boolean counterparts.
- *numbers* will be evaluated into numeric values.
- *strings* will be evaluated into strings.
- *paths with multiple components* will have their path components concatenated, separated by dots (``.``).

Example
----

    name(example.metric.name)                                          # (1)
    name('true')                                                       # (2)
    name('17')                                                         # (3)
    name('0.314e1')                                                    # (4)
    name('0xff')                                                       # (5)

1. Yields the metric value ``"example.metric.name"``.
2. Yields the metric value ``true`` (a boolean).
3. Yields the metric value ``17`` (an integral number).
4. Yields the metric value ``3.14`` (a floating point number).
5. Yields the metric value ``255`` (an integral number, interpreted in hexadecimal notation).

This is very powerful to use with match statements, for instance:

    match example.*.metric.name as G {
        tag G as index = name(G[1]);
    }

This will take the second component of ``example.*.metric.name`` and assign it as a tag to that metric.  
For example:

- ``example.'4'.metric.name`` will be tagged as ``example.'4'.metric.name{ index = 4 }``.
- ``example.foobar.metric.name`` will be tagged as ``example.'4'.metric.name{ index = "foobar" }``.
