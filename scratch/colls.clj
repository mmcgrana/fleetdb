[:create-index {:on :name :where [:= :type :person]}]
[:insert {:id 2 :type :person :name "mark"}]
[:select {:where [:and [:= :type :person] [:= :name "mark"]]}]

[:create-index {:coll :people :on :name}]
[:insert :people {:id 2 :name "mark"}]
[:select :people {:where [:= :name "mark"]}]


; queries and responses
[:select <coll> {:where _ :order _ :offset _ :limit _ :only _}]
=> [<record> <record> <record>]

[:get <coll> <id-s>]
=> <record>
=> [<record> <record> <record>]

[:count <coll> {:where _ :order _ :offset _ :limit _}]
=> <count>

[:insert <coll> <record-s>]
=> [<db> <count>]

[:update <coll> <with> {:where _ :order _ :offset _ :limit _ :with _}]
=> [<db> <count>]

[:delete <coll> {:where _ :order _ :offset _ :limit _}]
=> [<db> <count>]

[:create-index <coll> <index_on>]
=> [<db> 1]

[:drop-index <coll> <index_on>]
=> [<db> 1]

[:list-indexes <coll>]
=> [<index_on> <index_on>]

[:multi-read [<select> <count>]]
=> [<records> <count>]

[:multi-write [<insert> <update> <delete> <index>]]
=> [<db> [<count> <count> <count> <count>]]

[:checked-write
   <read_or_multi-read>
   <expected result>
   <write_or_multi-write>]
=> [<db> [false <actual-count>]] [<db> [true [<actual-count> <actual-records>]]]
=> [<db> [true <count>]] [<db> [true [<records> <count>]]]

[:explain [:select _ _]]
=> <query_plan>

; server commands and responses
[:ping]
=> "pong"

[:compact]
=> true

[:snapshot {:path "/foo/bar/snap1"}]
=> true