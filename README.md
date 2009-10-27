FleetDB aims to be a fast, concurrent, flexible, and powerful main memory database. It will feature:

* A document-oriented data model
* Dynamic indexes to efficiently answer queries
* Full snapshot isolation for concurrent readers
* Atomic multi-document updates
* A rich query interface
* Optional durability via append-only logging

FleetDB is implemented in Clojure. It stores data in main memory using persistent data structures and defines database operations in a functional style.

Status
------

FleetDB is currently being actively developed. Note that some features are not yet implemented and that the API is unstable. 

License
-------

Copyright (c) 2009 Mark McGranaghan and released under an MIT license.
