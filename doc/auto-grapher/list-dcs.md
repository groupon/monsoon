monsoon-auto-grapher list-dcs
====
Shows a list of all datacenters that auto-grapher knows.

This subcommand is read-only: it simply shows a list of built-in datacenters.

Syntax
----
**java -jar monsoon-auto-grapher.jar** **list-dcs**

Example
----
The command:

    java -jar monsoon-auto-grapher.jar list-dcs

will show (on stdout) a list of built-in datacenters.

It produces this output:

    dub1
    lup1
    sac1
    snc1

Bugs
----
Datacenters are hard-coded in auto-grapher.
