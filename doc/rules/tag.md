Rule: Tag
====

The tag rule allows for adding new tags to an existing metric group, by evaluating an expression.

Syntax
----

**tag** group\_name **as** tag **=** expression **;**

**tag** group\_name **{** tag **=** expression [ **,** ... ] **}**

- *group\_name*
  The [name of a group](../expressions/groupname.md).
- *tag*
  An identifier representing the tag name to create.
  If the tag already exists, it will be overwritten.
- *expression*
  An [expression](../config.md#expressions) that yields a value for the metric.
  An unresolved expression will cause the tag not to be assigned.

Example
----

    tag com.groupon.brands.requesthandlers.CountryBrandsFuzzySearchHandler as host =
      com.groupon.brands.requesthandlers.CountryBrandsFuzzySearchHandler hostname;

Define a host tag, based on a metric called ``hostname`` in the same handler.

    tag system.cpu as moncluster = "my monitoring cluster";

adds (or replaces) a tag ``moncluster`` with the value ``"my monitoring cluster" to the ``system.cpu`` group.

    tag system.cpu as 'cluster host' = str(tag(system.cpu cluster), "-", tag(system.cpu host));

will
- extract the ``cluster`` and ``host`` tags
- combine them, separated by a dash
- and assign that to the ``'cluster host'`` tag.
Note that ``'cluster host'`` is quoted, because space is not a valid character in an identifier otherwise.

    tag system.cpu as tagname = tag(system.cpu 'tag does not exist');

tries to assign the tag ``tagname``.
Assuming ``'tag does not exist'`` is an non-existant tag, this will cause the expression to fail evaluation.
Due to the expression evaluation failing, the statement does not change the ``system.cpu`` group.
