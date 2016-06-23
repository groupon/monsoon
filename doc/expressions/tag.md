Expression: Tag function
====

The tag function extracts the value of a specific tag from a metric group.

Syntax
----

**tag** **(** group **, ** tag **)**

- *group*
  A [group name](groupname.md).
- *tag*
  A tag identifier.

Example
----

    tag(system.cpu, host)                                            # (1)
    tag(com.groupon.requests, 'json endpoint')                       # (2)

1. Extracts the ``host`` tag from the metric group ``system.cpu``.
2. Extracts the ``json endpoint`` tag from the metric group ``com.groupon.requests``.
   The tag name was quoted, to allow the space to be encoded.
