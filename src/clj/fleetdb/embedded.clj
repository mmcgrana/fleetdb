(ns fleetdb.embedded
  (:use (fleetdb serializer))
  (:require (fleetdb [core :as core] [executor :as executor])))

(def read-query-type?
  #{:select :count :explain :list-indexes :multi-read})

(defn- dba? [dba]
  (:executor (meta dba)))

(defn init []
  (atom (core/init) :meta {:executor (executor/init)}))

(defn query [dba [query-type :as q]]
  (assert (dba? dba))
  (if (read-query-type? query-type)
    (core/query @dba q)
    (executor/execute (:executor (meta dba))
      #(let [old-db          @dba
             [new-db result] (core/query old-db q)]
         (assert (compare-and-set! dba old-db new-db))
         result))))

(defn close [dba]
  (executor/shutdown (:executor (meta dba))))
