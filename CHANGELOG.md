Change log
====

monsoon-2.2-SNAPSHOT
----

Features:
- Upgraded to angular 2.0.1. \o/
- Frontend chart now loads data incrementally and update continuously.
- Frontend timespec completely reworked.
- Frontend evaluation logic moved into separate component.

Bug fixes:
- Fix RPC timeout caused by BufferedIterator reading underlying iterator in the constructor.  This caused RPC calls to time out if the underlying iterator required more than 30 seconds for its first element to be available.

monsoon-2.1.1 (Sep 28, 2016)
----

Features:
- Create stand-alone frontend for graphing, that queries a remote history server for data.

Bug fixes:
- Fix nullpointer exception on IntervalIterator if traversing empty collection.
- Fix deadlock during expression evaluation if BufferedIterators nested deep enough to exhaust fork-join pool.

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