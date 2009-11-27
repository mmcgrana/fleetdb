; queries and responses
[:select {:where <where> :order _ :offset _ :limit _ :only _}]
=> [<record> <record> <record>]

[:get <id-s>]
=> <record>
=> [<record> <record> <record>]

[:count {:where <where> :order _ :offset _ :limit _}]
=> <count>

[:insert <record-s>]
=> [<db> <count>]

[:update {:where <where> :order _ :offset _ :limit _ :with _}]
=> [<db> <count>]

[:delete {:where <where> :order _ :offset _ :limit _}]
=> [<db> <count>]

[:create-index {:on <index_on>}]
=> [<db> 1]

[:drop-index {:on <index_on>}]
=> [<db> 1]

[:list-indexes]
=> [<index_on> <index_on> <index_on>]

[:multi-read  {:queries [<select> <count>]}]
=> [<records> <count>]

[:multi-write {:queries [<insert> <update> <delete> <index>]}]
=> [<db> [<count> <count> <count> <count>]]

[:checked-write {:check    <read_or_multi-read>
                 :expected <expected result>
                 :write    <write_or_multi-write>}]
=> [<db> [false <actual-count>]] [<db> [true [<actual-count> <actual-records>]]]
=> [<db> [true <count>]] [<db> [true [<records> <count>]]]

[:explain [:select _]]
=> <query_plan>

; server commands and responses
[:ping]
=> "pong"

[:compact]
=> true

[:snapshot {:name "snap1"}]
=> true

; todo
proper compiled server module
qualified indexes or tables
benchmark suite
logging
bind limitations

; ideas
paramaterized queries
response normalization
query timeout option
query profile option
query statistics / logging
file management
atomic operations
full text search
replication
distribution