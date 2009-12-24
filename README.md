FleetDB is a record-oriented, persistent, main-memory database implemented in Clojure and Java.

FleetDB combines the flexibility of schema-less records with the declarative indexing and querying capabilities of traditional databases. In-memory functional data structures provide high performance and excellent concurrency characteristics while append-only logging to disk provides complete durability.

Tutorial
--------

To get started, download the FleetDB standalone jar file and boot a server:

    $ curl -o fleetdb-standalone.jar http://cloud.github.com/downloads/mmcgrana/fleetdb/fleetdb-standalone.jar
    $ java -cp fleetdb-standalone.jar fleetdb.server -i text -f "/tmp/test.fdb"

Since we specified the `text` protocol, you can use `telnet` to interact with the server.

    $ telnet 127.0.0.1 3400

At the telnet prompt, type `[:ping]` and hit return. This will send a ping query to the server, which should respond with `"pong"`.

    > [:ping]
    < "pong"

Note that the `>` and `<` symbols are used to indicate requests and responses respectively and should not be typed.

A FleetDB database consists of collections of records. To create a collection, simply `:insert` records. For example, the following inserts a record into a new `:accounts` collection:

    > [:insert :accounts {:id 1 :owner "Eve" :credits 100}]
    < 1

The return value `1` indicates that 1 record was been added. To insert multiple records:

    > [:insert :accounts [{:id 2 :owner "Bob"   :credits 150}
                          {:id 3 :owner "Dan" :credits 50}]]

You can `:get` individual records by id:

    > [:get :accounts 2]
    < {:id 2 :owner "Bob", :credits 150}

You can also `:get` multiple records at the same time:

    > [:get :accounts [1 3]]
    < [{:id 1 :owner "Eve" :credits 100}
       {:id 3 :owner "Dan" :credits 50}]

The `:select` query finds records by more general criteria. For example, use it with the `:where` option to find accounts with at least 100 credits:

    > [:select :accounts {:where [:>= :credits 100]}]
    > [{:id 1 :owner "Eve" :credits 100}
       {:id 2 :owner "Bob" :credits 150}]

Or with the `:limit` and `:sort` options to find the 2 poorest accounts:

    > [:select :accounts {:order [:credits :asc] :limit 2}]
    < [{:id 3 :owner "Dan" :credits 50}
       {:id 1 :owner "Eve" :credits 100}]

FleetDB records are maps of attributes to values. Every record needs a unique `:id` attribute, but otherwise records may have different attributes:

    > [:insert :accounts {:id 4 :owner "Amy" :credits 1000 :vip true}]
    < 1

You can update records like this:

    > [:update :accounts {:credits 55} {:where [:= :owner "Dan"]}]
    < 1

This update merges the map `{:credits 55}` into those records matching the criteria `[:= :owner "Dan"]`.

To see what all of our records look like, issue a `:select` query without any conditions:

    > [:select :accounts]
    < [{:id 1 :owner "Eve" :credits 100}
       {:id 2 :owner "Bob" :credits 150}
       {:id 3 :owner "Dan" :credits 55}
       {:id 4 :owner "Amy" :credits 1000 :vip true}]

Count queries are formed similarly to select queries:

    > [:count :accounts {:where [:= :vip true]}]
    < 1

As are delete queries:

    > [:delete :accounts {:where [:= :id 3]}]
    < 1

As it stands, finding records by `:owner` requires scanning through all records and testing each one. We can verify this by asking FleetDB to explain its query plan for such a `:select` query:

    > [:explain [:select :accounts {:where [:= :owner "Eve"]}]]
    < [:filter [:= :owner "Eve"]
        [:record-scan :accounts]]

If you search by `:owner` often, you may want to add and index to speed query execution:

    > [:create-index :accounts :owner]
    < 1

Now FleetDB will use this index to answer queries that depend on the `:owner` attribute:

    > [:explain [:select :accounts {:where [:= :owner "Eve"]}]]
    < [:index-lookup [:accounts :owner "Eve"]]

FleetDB offers several queries for reading and writing a database that are accessed concurrently by other users. For example, if you want the result of multiple read queries to reflect a single snapshot of the database, it may not be safe to issue them individually. Instead you can compose read queries using the `:multi-read` query. Thus the following returns the names of the richest and poorest accounts according to a single snapshot of the database:

    > [:multi-read
        [[:select :accounts {:order [:credits :desc] :limit 1 :only :owner}]
         [:select :accounts {:order [:credits :asc]  :limit 1 :only :owner}]]]
    < [["Amy"]
       ["Eve"]]

Similarly you may want to execute multiple write queries atomically. Thus if you wanted to move 5 credits from "Amy" to "Bob":

    > [:multi-write
        [[:update :accounts {:credits 995} {:where [:= :owner "Amy"]}]
         [:update :accounts {:credits 105} {:where [:= :owner "Eve"]}]]]
    < [1
       1]

Finally, FleetDB provides an optimistic concurrency control query `:checked-write`. This query first executes a read query to ensure that it corresponds to an expected result before executing a write query. You can think of it as a generalized compare and swap. Thus to safely deduct 5 credits from Eve's account, which previously had 105 credits:

    > [:checked-write
         [:select :accounts {:where [:= :owner "Eve"] :only :credits}]
         [105]
         [:update :accounts {:credits 100} {:where [:= :owner "Eve"]}]]

Then if the write succeeds:

    < [true 1]

Or if it fails, for example because the actual checked balance was 120:

    < [false [120]]

Several housekeeping queries are available. To see all of the indexes on a collection:

    > [:list-indexes :accounts]
    < [:owner]

And to drop one of these indexes:

    > [:drop-index :accounts :owner]
    < 1

To show all collections:

    > [:list-collections]
    < [:accounts]

To remove a collection, drop its indexes (which we did above) and delete all of its records:

    > [:delete :accounts]
    < 3

    > [:list-collections]
    < []

Now that you have experimented with some basic FleetDB queries, we will see how to use a Clojure TCP client to interact with the database. First, we need to boot the FleetDB server into binary protocol mode, which is one of the protocols implemented by the Clojure client. Shut down the running server with CTRL-C and restart it with:

    $ java -cp fleetd-standalone.jar fleetdb.server -i binary -f "/tmp/test.fdb"

Note that the database will recover its state from the log at `/tmp/test.fdb`.

The Clojure client offers a similar interface to the text one we used above:

    $ java -cp fleetdb-standalone.jar clojure.contrib.repl_ln
    => (use '(fleetdb [client :as client]))
    => (def c (client/connect "127.0.0.1" 3400 :binary))
    => (client/query c [:ping])
    [0 "pong"]

Responses consists of vectors of 2 elements. The first element is a status code, which will by 0 if the operation completes successfully and non-zero in the case of an error. If there are no errors, the second element is the actual response value. If there is an error, the second element gives an error message:

Otherwise, the queries that we saw above behave the saw way:

    => (client/query c [:get :accounts 2])
    [0 {:id 2 :owner "Alice" :credits 150}]

Close the client when you are done with it:

    => (client/close c)

If at any point you want to backup or replicate the state of a FleetDB database, you can just copy its log file with e.g. cp, scp, or rsync:

    $ cp /tmp/test.fdb /tmp/backup-test.fdb

License
-------

Copyright (c) 2009 Mark McGranaghan and released under an MIT license.
