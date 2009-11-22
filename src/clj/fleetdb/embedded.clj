(ns fleetdb.embedded
  (:use (fleetdb util serializer))
  (:require (fleetdb [core :as core] [executor :as executor])))

(def- write-query-type?
  #{:insert :update :delete :create-index :drop-index
    :multi-write :checked-write})

(defn- dba? [dba]
  (:write-pipe (meta dba)))

(defn init []
  (atom (core/init) :meta {:write-pipe (executor/init-pipe)}))

(defn query [dba [query-type :as q]]
  (assert (dba? dba))
  (if (write-query-type? query-type)
    (executor/execute (:write-pipe (meta dba))
      #(let [old-db          @dba
             [new-db result] (core/query old-db q)]
         (assert (compare-and-set! dba old-db new-db))
         result))
    (core/query @dba q)))

(defn close [dba]
  (executor/shutdown-now (:write-pipe (meta dba))))
