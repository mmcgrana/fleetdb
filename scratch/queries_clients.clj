; Queries and associated responses

[:select {:where <where> :order _ :offset _ :limit _ :only _}]
<= [<record> <record> <record>]

[:count {:where <where> :order _ :offset _ :limit _}]
=> <count>

[:insert {:records _}]
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

[:checked-write {:check  <read_or_multi-read>
                 :expect <expected result>
                 :write  <write_or_multi-write>}]
=> nil
=> [<db> <count>]
=> [<db> [<records> <count>]]

[:explain {:query [:select _]}]
=> <query_plan>

; server only
[:snapshot {:snapshot-path "/foo/snap.bin" :tmp-dir-path "/tmp"}]
=> "/tmp/snap123.tmp"

[:branch {:new-write-path "/foo/new.bin"}]
=> "/foo/old.bin"

[:compact {:compact-path :new-write-path :tmp-dir-path}]
=> ?

; embedded mode
enter pipe
  get old db
  comp result, new db
  if logging, write simplified query to log
  swap new db in
  return result
exit pipe

; operations
init
snapshot
branch
compact (tmp-dir log-head)

; snapshot
for non-persisting db only
close over db state
write to specified dos indicating no tail, then insert commands, then indexes
rename tmp file to specified path

; branch
for persisting db only
put into write pipe
  switch to new dos point to prev tail path

; compaction
from persisting db only
put into write pipe
  close over current db state
  switch to new dos pointing to both compaction path and prev tail path
  in new thread, freeing write pipe
    to temp path, write snapshot
    rename temp path to snapshot path

; log loading
(see old code)
get file seq from headers, going to second reference if first not found
reduce of file seq
  reduce over commands in file
    init empty database with

; server
(def dba (embedded/init))
(defn recieve [q] (embedded/query dba q))
bootstrap.setOption("child.tcpNoDelay", true);
bootstrap.setOption("child.keepAlive", true);


; Todo
X exceptions & interaction with threading
X log writing
x tag
x snapshot
X log compacting
x recursive log loading
x embedded
x optional persistence
x text server

testing persistent vs non-persistent servers
binary server
binary client

query timeout option
query profile option
response normalization
streaming request/response
qualified indexes
tables
benchmark suite

query statistics / logging
file management
atomic operations
full text search
replication
distribution