Developer Documentation
====

If you're here, it probably means you're
- curious how the project works internally,
- are debugging an bug,
- want to write a new collector or
- want to write new integration.

If you're just looking how to use the monitor, you're probably more interested in reading up on the [configuration file](../config.md) or the different [processors](../processors/index.md).

The documentation here assumes you've at least read the [data model](../datamodel.md) document.

Build Process
----

The project is written in [Java 8](http://openjdk.java.net/) and built using [maven](http://maven.apache.org/).
As of time of writing, all tests are unit tests and should work entirely independant of the environment you are running.
Make sure your tests pass: ``mvn test``.

Note that older releases of JDK 8 have a bug which will cause crashes while running this code. It is recommended that you use a recent build of JDK 8 (at the time of writing, version 1.8.91 is being used). Monsoon does not run on JDK versions below 8 due to a reliance on language features introduced in version 8.

Release Process
----

This project uses the [Maven Release Plugin](http://maven.apache.org/maven-release/maven-release-plugin/index.html) to perform releases. There are two steps for performing a release. First, on the master branch, once all commits for the release have been merged, tested and approved, the following maven target is run:

```ro
mvn release:prepare
```

This process prompts the user for new version and tag info, renames the versions in the pom.xml files, commits and tags the code. Once the build is successful, the release can be finalised (and deployed to nexus) using:

```
mvn release:perform
```

Writing Extensions
----

Most actions will involve writing new [Collectors](../collectors/index.md) or new [Processors](../processors/index.md).
To get started, please refer to the developer documentation on [Collectors](collector.md) or [Processors](processor.md).

Advanced Development
----

To change the inner guts of the project is quite a bit more advanced.
I should also really document this...
