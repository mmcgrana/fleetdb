Performance
===========

FleetDB is designed for high performance, but quantifying database performance is notoriously difficult. We are currently preparing a comprehensive benchmarking suite for use against both FleetDB and related database systems. Until this suite is ready and the results collected, we can only provide very preliminary results.

Initial tests suggests that a FleetDB server running on a commodity Linux box can answer on the order of 10,000 simple read or write queries per second from a single instance of the bundled [`fleetdb.client`](...). This is within a factor of 2 of the rate at which it answers `ping` queries; hence simple query performance is limited substantially by the networking layer. In terms of raw insert throughput, a client using bulk inserts can add records to a collection at the rate of several tens of thousand per second.

Another important performance consideration is how quickly a server can load a database snapshot into memory on startup. For a typical collection, the server can load on the order of 100,000 records a second.

Finally, we should mention the memory usage patterns of FleetDB. The amount of space required to keep all data varies considerably with the nature of the data and the number and type of indexes. As a very rough guide, a collection of modestly sized records indexed on one attribute will require on the order of 1GB of RAM per 1 million records.
