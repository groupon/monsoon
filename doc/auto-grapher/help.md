monsoon-auto-grapher help
====
Provides helpful text for invoking auto-grapher.

This subcommand doesn't do anything, but is intended to aid people into using it, to provide the correct arguments to their commands.

Syntax
----
**java -jar monsoon-auto-grapher.jar** **help**  
**java -jar monsoon-auto-grapher.jar** **help** *subcommand*

Global Help
----
Running ``help`` with no arguments, displays the list of subcommands and a one-line description on how to invoke them.

Subcommand Specific Help
----
Running ``help subcommand`` shows helpful text for invoking the specific subcommand and which arguments it will take.

For example:

    java -jar monsoon-auto-grapher.jar help create

will display information on how to invoke the *create* subcommand.

Output looks like:

    Usage:  java -jar monsoon-auto-grapher.jar create     [ dc= template= ]


    Create graphs in grapher.

    .
    .
    .

    dc=        -- Limit graph creation to the given DC (can be specified multiple times).
    template=  -- Limit metrics filtering to the given templates only (default: match all templates).

The first line displays the command invocation ``java -jar monsoon-auto-grapher.jar create`` followed by the parameters it takes: ``dc=`` and ``template=``.

After that, it is followed by documentation on what the command does, in this case it allows graph creation.

The end of the output lists each parameter and what it signifies, for example, the ``dc`` parameter specified which datacenters are used during graph creation.
