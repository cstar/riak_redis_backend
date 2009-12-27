Riak Redis Backend
==================

Installing
----------

You need :

 - the [erldis binaries](http://github.com/cstar/erldis/tree/binaries) branch available in your Riak installation.

 - [Redis](http://code.google.com/p/redis/) (Any version will do). 

 - [Riak](http://riak.basho.com/)

 - compile (`erlc riak_redis_backend.erl`) and copy the beam file in your riak installation.

Configure riak :

  `{storage_backend, riak_dets_backend}` in the `etc/app.config`

For the time being, Redis must run locally. I will certainly add the line to configure the redis host and port.

Other files
-----------

 * `riak_redis_prof` : runs the Riak `standard_backend_test`.

 * `riak_playground` : runs an insert test and 2 map-reduce tasks and give timing information.s

License
---------

Apache 2 license.

Author
--------
Eric Cestari 

[http://www.cestari.info/](http://www.cestari.info/)

ecestari+riak-backend@mac.com
