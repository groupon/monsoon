Auto Grapher
====

Auto-grapher (monsoon-auto-grapher.jar) is a command line utility, to easily create graphs in grapher based on a template definition.
Auto-grapher supports creating the same graph in multiple datacenters and automatically suggesting which graphs to create, based on a running application (although it is assumed you'll use monsoon to handle the monitoring in that case).

Download
----
Everything related to monsoon is published on the internal nexus.
To download the autographer binary, please look for it [here](http://nexus-dev.snc1/index.html#nexus-search;quick~monsoon-auto-grapher) (requires VPN proxy or http proxying).

If you're looking into scripting creation of graphs, you may want to check out [this example](pipe_example.md).

Commands
----
Auto-grapher uses subcommands to implement different functionality.

The subcommands are:
- **[help](help.md)**
  Provides helpful text for invoking auto-grapher.
- **[list](list.md)**
  List monsoon groups for which a graph can be created, using a specific template.
- **[list-templates](list-templates.md)**
  Show all templates that the system knows about.
- **[list-dcs](list-dcs.md)**
  Show all datacenters that the system knows about.
- **[create](create.md)**
  Automatically create graphs in some/all datacenters.
- **[list-alerts](list-alerts.md)**
  List all alerts, by parsing the monsoon configuration and analyzing your application.
- **[list-yaml-alerts](list-yaml-alerts.md)**
  List all alerts, but instead of displaying them in monsoon syntax, provide a YAML snippet that can be copy/pasted into your host.yml or hostclass.yml.

Command Line Help
----
Auto-grapher provides a command line help:

    java -jar monsoon-auto-grapher.jar help

    Usage:  java -jar monsoon-auto-grapher.jar help
            java -jar monsoon-auto-grapher.jar list
            java -jar monsoon-auto-grapher.jar list-templates
            java -jar monsoon-auto-grapher.jar list-dcs
            java -jar monsoon-auto-grapher.jar create

    create     -- Create graphs based on a template.
    help       -- Display usage information.
    list       -- Retrieve a filtered monsoon groups from a running
                  application.
    list-dcs   -- Retrieve a list of all known datacenters.
    list-templates -- Retrieve a list of all known templates.

    For details on how to use each command, invoke help on the command.  For
    example:
        java -jar monsoon-auto-grapher.jar help help
    will display instructions on how to use the help command.

Each of the sub-commands has a help section of its own, example:

    java -jar monsoon-auto-grapher.jar help help

    Usage:  java -jar monsoon-auto-grapher.jar help
            java -jar monsoon-auto-grapher.jar list
            java -jar monsoon-auto-grapher.jar list-templates
            java -jar monsoon-auto-grapher.jar list-dcs
            java -jar monsoon-auto-grapher.jar create

    create     -- Create graphs based on a template.
    help       -- Display usage information.
    list       -- Retrieve a filtered monsoon groups from a running
                  application.
    list-dcs   -- Retrieve a list of all known datacenters.
    list-templates -- Retrieve a list of all known templates.

    For details on how to use each command, invoke help on the command.  For
    example:
        java -jar monsoon-auto-grapher.jar help help
    will display instructions on how to use the help command.
