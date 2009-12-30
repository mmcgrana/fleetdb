(ns fleetdb.embedded
  (:use [fleetdb.util :only (def- ? spawn rassert raise)]
        [clojure.contrib.seq-utils :only (partition-all)])
  (:require (fleetdb [core :as core] [fair-lock :as fair-lock]
                     [io :as io] [file :as file] [lint :as lint]))
  (:import  (java.util ArrayList)))

(defn- dba? [dba]
  (? (:write-lock (meta dba))))

(defn- persistent? [dba]
  (? (:write-dos (meta dba))))

(defn- ephemeral? [dba]
  (not (persistent? dba)))

(defn- compacting? [dba]
  (? (:write-buf (meta dba))))

(defn- replay-query [db q]
  (first (core/query db q)))

(defn- read-from [read-path]
  (let [dis      (io/dis-init read-path)
        queries  (io/dis-deserialized-seq dis)
        empty    (core/init)]
    (reduce replay-query empty queries)))

(defn- write-to [db write-path]
  (let [dos (io/dos-init write-path)]
    (doseq [coll (core/query db ["list-collections"])]
      (doseq [records (partition-all 100 (core/query db ["select" coll]))]
        (io/dos-serialize dos ["insert" coll (vec records)]))
      (doseq [ispec (core/query db ["list-indexes" coll])]
        (io/dos-serialize dos ["create-index" coll ispec])))
    (io/dos-close dos)))

(defn- init* [db & [other-meta]]
  (atom db :meta (assoc other-meta :write-lock (fair-lock/init))))

(defn init-ephemeral []
  (init* (core/init)))

(defn load-ephemeral [read-path]
  (init* (read-from read-path)))

(defn init-persistent [write-path]
  (let [write-dos (io/dos-init write-path)]
    (init* (core/init) {:write-dos write-dos :write-path write-path})))

(defn load-persistent [read-write-path]
  (let [db        (read-from read-write-path)
        write-dos (io/dos-init read-write-path)]
    (init* db {:write-dos write-dos :write-path read-write-path})))

(defn close [dba]
  (assert (fair-lock/join (:write-lock (meta dba)) 60))
  (if-let [write-dos (:write-dos (meta dba))]
    (io/dos-close write-dos))
  (assert (compare-and-set! dba @dba nil))
  true)

(defn fork [dba]
  (rassert (ephemeral? dba) "cannot fork persistent databases")
  (init* @dba))

(defn snapshot [dba snapshot-path]
  (rassert (ephemeral? dba) "cannot snapshot persistent databases")
  (let [tmp-path  (file/tmp-path "/tmp" "snapshot")]
    (write-to @dba tmp-path)
    (file/mv tmp-path snapshot-path)
    true))

(defn compact [dba]
  (rassert (persistent? dba) "cannot compact ephemeral database")
  (rassert (not (compacting? dba)) "already compacting database")
  (fair-lock/fair-locking (:write-lock (meta dba))
    (let [tmp-path      (file/tmp-path "/tmp" "compact")
          db-comp-start @dba]
      (alter-meta! dba assoc :write-buf (ArrayList.))
      (spawn
        (write-to db-comp-start tmp-path)
        (fair-lock/fair-locking (:write-lock (meta dba))
          (let [dos (io/dos-init tmp-path)]
            (doseq [post-comp-query (:write-buf (meta dba))]
              (io/dos-serialize dos post-comp-query))
            (file/mv tmp-path (:write-path (meta dba)))
            (io/dos-close (:write-dos (meta dba)))
            (alter-meta! dba dissoc :write-buf)
            (alter-meta! dba assoc  :write-dos dos))))
      true)))

(defn query* [dba q]
  (if (core/write-query? q)
    (fair-lock/fair-locking (:write-lock (meta dba))
      (let [old-db          @dba
            [new-db result] (core/query* old-db q)]
        (when-let [write-dos (:write-dos (meta dba))]
          (io/dos-serialize write-dos q)
          (when-let [#^ArrayList write-buf (:write-buf (meta dba))]
            (.add write-buf q)))
        (assert (compare-and-set! dba old-db new-db))
        result))
    (core/query* @dba q)))

(defn query [dba q]
  (rassert (dba? dba) "dba not recognized as a database")
  (lint/lint-query q)
  (query* dba q))
