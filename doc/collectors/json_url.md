Collector: json\_url
====

The json\_url collector performs a HTTP request against the given URL and records response metrics.
It differs from the [url collector](url.md) in that it processes the response as a json document and exposes the properties it finds.

Syntax
----

**collect** **json\_url** "url" **as** path **;**  
**collect** **json\_url** "url" **as** path **{** 0 **=** **[** "arg", ... **],** ...  **}**

For a meaning of the arguments, please refer to [the documentation on url collector](url.md#syntax).

Example
----

    collect "http://$0/healthcheck" as healthcheck {
      0 = [ ums-api1.dc1,
            ums-api2.dc1,
            ums-api3.dc1,
            ums-api4.dc1 ]
    }

This scrapes the urls:
- ``http://ums-api1.dc1/healthcheck``
- ``http://ums-api2.dc1/healthcheck``
- ``http://ums-api3.dc1/healthcheck``
- ``http://ums-api4.dc1/healthcheck``

and stores the result in metrics:
- ``healthcheck.'ums-api1.dc1'``
- ``healthcheck.'ums-api2.dc1'``
- ``healthcheck.'ums-api3.dc1'``
- ``healthcheck.'ums-api4.dc1'``
