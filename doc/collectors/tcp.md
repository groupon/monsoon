Collector: tcp
====

The TCP collector performs a TCP connect against the given host and port, recording timing metrics.

Syntax
----

**collect** **tcp** **as** path **{** **(host, port)** **=** **[(** "hostname", 80 **),** ... **],** ...  **}**

- *host*
  In the tag section, selects the destination host; must be a string.
- *port*
  In the tag section, selects the destination port; must be a number.

Example
----

    collect tcp as google {
      (host, port) = { ("www.google.com", 80) }
    }

Create a single metric group ``google`` containing metrics on the TCP connect timing.
This creates the following metrics:

    google{host="www.google.com", port=80} {
      latency = 43,                                                    # 1
      up = true,                                                       # 2
      error.msg = (none),                                              # 3
      error.type = "OK",                                               # 4

      error.timed_out = false,                                         # 5
      error.no_route_to_host = false,
      error.port_unreachable = false,
      error.unknown_host = false,
      error.unknown_service = false,
      error.connect_failed = false,
      error.protocol_error = false,
      error.bind_failed = false,
      error.io_error = false
    }

1. ``latency`` indicates the time until the connection was set up.
2. ``up`` indicates the connection succeeded.
3. ``error.msg`` indicates a (java) error message, explaining why the connect attempt failed.
4. ``error.type`` is the type of the error; ``"OK"`` indicates everything was fine.  Otherwise it will correspond to one of the error types.
5. ``error.*`` is a boolean indicating if the specified error triggered.

To check multiple hosts, the parameters can be split:

    collect tcp as social.media
      host = [ "www.facebook.com", "www.google.com", "www.twitter.com", "www.groupon.com" ],
      port = [ 80 ]
    }

Creates a TCP connection to the specified hosts, on port *80*.
