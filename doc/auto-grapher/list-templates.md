monsoon-auto-grapher list-templates
====
Shows a list of all grapher templates that auto-grapher knows.

This subcommand is read-only: it simply shows a list of built-in templates.

Syntax
----
**java -jar monsoon-auto-grapher.jar** **list-templates**

Example
----
The command:

    java -jar monsoon-auto-grapher.jar list-templates

will show (on stdout) a list of built-in template names.

It produces this output:

    latency

Bugs
----
Templates are hard-coded in auto-grapher.
Users should be able to provide templates using a file.
