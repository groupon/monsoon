Expression: Percentile-aggregate function
====

The percentile\_agg function calculates a percentile of one or more metrics.
The function will accept metric wildcards.

The percentile\_agg will skip ``nil`` values and strings.
An empty set of data will result in a ``nil`` value.

The percentile\_agg function expects a collection of samples as its input.

Syntax
----

**percentile\_agg** **(** percentile **,** groupselector metric | expression [ **,** ... ] **)** [ **by**|**without** **(** tag [ **,** ... ] **)** ] [ **keep_common** ]

- *percentile*
  A number between 0 and 100 (inclusive) to select which percentile to calculate.
  Floating-point numbers are accepted, for example, ``99.999`` will select the percentile corresponding to *5 nines*.
  This needs to be an immediate value, no expressions are allowed.
- *groupselector metric*
  A [wildcard group selector](groupselector.md) and metric name upon which the group must match.
  The match will select all groups matching the wildcard pattern, if they contain the given metric.
  The match will resolve to each metric on these groups.
- *expression*
  A metric expression.
- *tag*
  Group the sum based on each tag.
  This is comparable to using ``sum ... group by`` in SQL.

Example
----

    percentile_agg(90, com.groupon.requesthandler.* latency)         # (1)
    percentile_agg(50, 1, 2, 3 + 4)                                  # (2)
    percentile_agg(99.9
                   com.groupon.requesthandler.Create duration,
                   com.groupon.requesthandler.Delete duration)       # (3)
    percentile_agg(10, ** latency)                                   # (4)

1. Calculates the 90th percentile of latencies.
2. Calculates the median of ``1``, ``2``, ``3 + 4``, yielding the value 2.
3. Calculates the 99.9th percentile of ``duration`` in the ``Create`` and ``Delete`` request handlers.
4. Calculates the 10th latency percentile over all metrics.

A more complete example:

    collect url "http://${host}/index.html" as homepage {
        host = [ "www1.example.tld", "www2.example.tld", "www3.example.tld" ],
        idx = [  "0",  "1",  "2",  "3",  "4",  "5",  "6",  "7",  "8",  "9",
                "10", "11", "12", "13", "14", "15", "16", "17", "18", "19",
                "20", "21", "22", "23", "24", "25", "26", "27", "28", "29",
                "30", "31", "32", "33", "34", "35", "36", "37", "38", "39",
                "40", "41", "42", "43", "44", "45", "46", "47", "48", "49",
                "50", "51", "52", "53", "54", "55", "56", "57", "58", "59",
                "60", "61", "62", "63", "64", "65", "66", "67", "68", "69",
                "70", "71", "72", "73", "74", "75", "76", "77", "78", "79",
                "80", "81", "82", "83", "84", "85", "86", "87", "88", "89",
                "90", "91", "92", "93", "94", "95", "96", "97", "98", "99" ]
    }

    define latency {
        p90 = percentile_agg(90, homepage latency) by (host) keep_common;
        p99 = percentile_agg(99, homepage latency) by (host) keep_common;
    }

    define overall.latency {
        p90 = percentile_agg(90, homepage latency);
        p99 = percentile_agg(99, homepage latency);
    }

Scrapes the ``index.html`` from each webserver 100 times and publishes the latency per host.
It also calculates the overall latency, by combining the scrapes of all hosts together and calculating the p90 and p99 percentiles over that.
