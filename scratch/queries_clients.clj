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


; Server

(def dba (embedded/init))

(defn recieve [q]
  (embedded/query dba q))


; Client

(def conn (connect params))

(query conn [:select {:where [:= :id id]}])
(query conn [:insert {:records records}])

; Embedded

enter executor
  get old db
  comp result, new db
  write to simplified query to log
  swap new db in
  return result
exit executor

; Log writing
open dos after database
do nothing on read query
write write and multi-write directly
write :write of checked-write
persistance can stay out of core, right? - just put in embedded

; log reading

open dis
reduce database over query-seq
then return database

; Todo
exceptions & interaction with threading
log writing
log loading
snapshoting
forking
recursive log loading
log compacting
embedded and server modes
initializing
stopping - ctr-c
optional persistence
query statistics
query profiling / response normalization
replication
distribution
tables
file management
text client-server
binary client-server
atomic operations
full text search
benchmark suite