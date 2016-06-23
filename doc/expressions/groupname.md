Expression: Group
====

A group name is used to refer to a group.
Group names are text, separated by dots.
Since a collector can emit any string, including keywords and characters that are invalid identifiers, components of a group name may be quoted.

Syntax
----

pathelem [ **.** pathelem [ **.** pathelem [ ... ] ] ]
'pathelem' [ **.** pathelem [ **.** 'pathelem' [ ... ] ] ]

- *pathelem*
  A component path in the group name.
  The path element must be a valid identifier, or must be quoted using single quotes.
  A path element may use escape sequences, if it is between single quotes.

Note that, if quotes are used, the entire path component must be between quotes.

Example
----

    com.groupon.frontend                                             # (1)
    'alert'.foobar                                                   # (2)
    'name.name'                                                      # (3)
    'group\nname\nwith\nnewlines'                                    # (4)
    '\'quotes\''                                                     # (5)
    '99thPercentile'                                                 # (6)

1. Refers to a group with the path components ``com``, ``groupon`` and ``frontend``.
2. Uses single quotes to escape the *alert* keyword.
   It refers to a path with components ``alert`` and ``foobar``.
3. Uses quotes to escape a dot, refering to a path with the single component ``name.name``.
   If your collector emits group names with dots, you'll need to use the quotes to refer to it.
   Without the quotes, the group would instead be ``name`` followed by ``name``.
4. Uses quotes and escape codes to select newlines.
   It refers to a group path with a single component:

       ``group``  
       ``name``  
       ``with``  
       ``newlines``

5. Uses quotes to escape single quotes.
   Refers to a group with a single path component: ``'quotes'``.
6. Refers to a group name that starts with a number.
   Since group names starting with a number are not valid identifiers, it has to be quoted.
   The group referred to is ``99thPercentile``.
