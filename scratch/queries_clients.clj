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



; Raw

(def dba (atom (core/init)))
(def executor (executors/init))

(core/query @dba [:select {:where [:= :id id]}])

(executor/execute executor
  #(let [old-db @db
         [new-db result] (core/query old-db [:insert {:records records}])]
     (assert (compare-and-set! dba new-db old-db))
     result))


; Embedded

(defn atomized [db] (atom db :meta {:executor (executors/init)}))

(defn atomized? [dba] (:executor (meta dba)))

(defn atomized-query [dba [query-type :as q]]
  (assert (atomized? dba))
  (if (read-query-type? query-type)
    (core/query @dba q)
    (executor/execute (:executor (meta dba))
      #(let [old-db @dba
             [new-db result] (core/query old-db [:insert {:records records}])]
         (assert (compare-and-set! dba new-db old-db))
         result))))

(def dba (atomized (init)))

(core/atomized-query dba [:select {:where [:= :id id]}])
(core/atomized-query dba [:insert {:records records}])


; Server

(def dba (atomized (init)))

(defn recieve [q]
  (core/atomized-query dba q))


; Client

(def conn (connect params))

(query conn [:select {:where [:= :id id]}])
(query conn [:insert {:records records}])


; Todo
uuids - serialization and core performance
embedded and server modes
initializing
stopping - ctr-c
optional persistence
log writing
log loading
log compacting
recursive log loading
snapshoting
forking
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