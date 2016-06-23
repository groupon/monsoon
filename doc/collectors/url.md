Collector: url
====

The URL collector performs a HTTP request against the given URL and records response metrics.

The URL may contain placeholder arguments, which are declared as taking multiple values.
Each combination of placeholder arguments is emitted.

Syntax
----

**collect** **url** "url" **as** path **;**  
**collect** **url** "url" **as** path **{** 0 **=** **[** "arg", ... **],** ...  **}**

- *"url"*
  The URL to be tested.
  This can be a pattern, with ${0}, ${1}, ... as placeholders for arguments.
- *path*
  The base of the path, under which metrics are emitted.
  If placeholders are used, those are appended after the path, to create the complete group name.
- *0* **=** **[** *"arg"*, ... **]**  
  A placeholder argument with a set of values.
  Placeholder arguments must start with the index 0 and must be consequtive.

Example
----

    collect "http://www.google.com" as google;

Create a single metric group ``google`` containing metrics on the response when visiting www.google.com over http.

    collect "http://www.${0}.com" as ping {
      0 = [ "facebook", "google", "twitter", "groupon" ]
    }

Visits a set of URLs, using a single placeholder.

This results in 4 metrics:
``ping.facebook``, ``ping.google``, ``ping.twitter``, ``ping.groupon``.

    collect "${2}://${0}/${1}" as check.ping {
      0 = [ "my_service.dc1", "my_service.dc2, "my_service.dc3" ],
      1 = [ "status.json", "heartbeat", "healthcheck" ],
      2 = [ "http", "https" ]
    }

This creates a large collection of metrics, one for each combination of arguments:
- ``check.ping.'my_service.dc1'.'status.json'.http``
- ``check.ping.'my_service.dc2'.'status.json'.http``
- ``check.ping.'my_service.dc3'.'status.json'.http``
- ``check.ping.'my_service.dc1'.'heartbeat'.http``
- ``check.ping.'my_service.dc2'.'heartbeat'.http``
- ``check.ping.'my_service.dc3'.'heartbeat'.http``
- ``check.ping.'my_service.dc1'.'healthcheck'.http``
- ``check.ping.'my_service.dc2'.'healthcheck'.http``
- ``check.ping.'my_service.dc3'.'healthcheck'.http``
- ``check.ping.'my_service.dc1'.'status.json'.https``
- ``check.ping.'my_service.dc2'.'status.json'.https``
- ``check.ping.'my_service.dc3'.'status.json'.https``
- ``check.ping.'my_service.dc1'.'heartbeat'.https``
- ``check.ping.'my_service.dc2'.'heartbeat'.https``
- ``check.ping.'my_service.dc3'.'heartbeat'.https``
- ``check.ping.'my_service.dc1'.'healthcheck'.https``
- ``check.ping.'my_service.dc2'.'healthcheck'.https``
- ``check.ping.'my_service.dc3'.'healthcheck'.https``

Note that, because some of the path elements contain a dot, they have to be escaped if you refer to them.
