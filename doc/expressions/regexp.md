Expression: Regexp function
====

The regexp function uses a regular expression and a template, to modify a string argument.

Syntax
----

**regexp** **(** expression **,** pattern **,** template **)**

- *expression*
  An expression, which will be coerced to a string.
- *pattern*
  A Regular expression pattern, which will be applied to the expression.
  For an in-depth explanation of the pattern, consult the [Java documentation on Regular Expressions](https://docs.oracle.com/javase/8/docs/api/java/util/regex/Pattern.html).
- *template*
  A string template, with placeholders that will be replaced by the matched groups.

Notes
----
Regular expression rewriting is very powerful,
but it is very hard to figure out what's going on.
I recommend you put a comment near the regexp, to describe what you're doing.

Example
----

    regexp("hello world!",
        "^([^ ])* ([^ ]*)$",
        "bye $2")            # (1)
    regexp(tag(system.cpu host),
        "^([^\\.]*)(*\\..*)?$",
        "$1")       # (2)
    regexp(tag(system.cpu host),
        "^(?<host>[^\\.]*)(?<domain>*\\..*)?$",
        "${host}")                                                   # (3)

1. Matches the string ``hello world!`` and replaces the first word with ``bye``, returning ``"bye world!"``.
2. Takes the hostname of the ``system.cpu`` group and takes everything up-to the first dot.
   This effectively yields the hostname, removing any domain name component.
   The output of this invocation gives ``"localhost"``, if the ``host`` tag held the value ``localhost.localdomain``.
3. The same as (2), but using named capturing groups.

Note the double escaping of the backslash:
- the regexp requires the dot (``\.`` to be escaped, to be taken literal,
- since the regexp is a string, it requires the backslash to be escaped as well.
