Change log
====

monsoon-2.2-SNAPSHOT
----

Features:
- Create stand-alone frontend for graphing, that queries a remote history server for data.

Bug fixes:
- Fix nullpointer exception on IntervalIterator if traversing empty collection.

monsoon-2.1 (Sep 17, 2016)
----

Features:
- New component: Remote History server.  
  **Note**: protocol is not entirely stable and may change in the future.
- Implemented Sun RPC history protocol.
- Aggregated queries now accept a time specification.  
  Example: ``percentile_agg[1h](75, monsoon timing.'collectors')``
- Step-size based iterations use Interpolation to fill in missing values.
- Collectors now use a builder pattern, so new collectors won't need parser changes.

Bug fixes:
- Rate query now correctly interpolates values for previous scrape.
- Pipeline builders now work correctly, if supplied with API server.
- Work around spontaneous reactor shutdown in URL collectors.

monsoon-2.0 (aug 31, 2016)
----

- Opensourced monsoon!
