(ns fleetdb.embedded
  (:use [fleetdb.util :only (def- ?)])
  (:require (fleetdb [core :as core] [exec :as exec] [io :as io])))

(def- write-query-type?
  #{:insert :update :delete
    :create-index :drop-index
    :multi-write :checked-write})

(defn- dba? [dba]
  (? (:write-pipe ^dba)))

(defn- persistent? [dba]
  (? (:write-pipe ^dba)))

(defn- ephemral? [dba]
  (not (persistent? dba)))

(defn- read-from [read-path]
  (let [dis       (io/dis-init read-path)
        header    (io/dis-read! dis)]
    (assert (:root header))
    (reduce #(first (core/query %1 %2)) (core/init) (io/dis-seq dis))))

(defn- write-to [db write-path]
  (let [dos (io/dos-init write-path)]
    (io/dos-write dos {:root true})
    (doseq [records (partition 100 (core/query db [:select]))]
      (io/dos-write dos [:insert {:records (vec records)}]))
    (doseq [ispec (core/query db [:list-indexes])]
      (io/dos-write dos [:create-index {:on ispec}]))
    (io/dos-close dos)))

(defn- init* [db other-meta]
  (atom db :meta (merge {:write-pipe (exec/init-pipe)} other-meta)))

(defn init-ephemral []
  (init* (core/init)))

(defn load-ephemral [read-path]
  (init* (read-from read-path)))

(defn init-persistent [write-path]
  (let [write-dos (io/dos-init write-path)]
    (io/dos-write write-dos {:root true})
    (init* (core/init) {:write-dos write-dos})))

(defn load-persistent [read-write-path]
  (let [db        (read-from read-write-path)
        write-dos (io/dos-init read-write-path)]
    (init* db {:write-dos write-dos})))

(defn fork [dba]
  (assert (dba? dba))
  (assert (ephemral? dba))
  (init* @dba))

(defn snapshot [dba snapshot-path tmp-dir-path]
  (assert (dba? dba))
  (assert (ephemral? dba))
  (let [tmp-path  (io/tmp-path tmp-dir-path "snapshot")]
    (write-to @dba tmp-path)
    (io/rename tmp-path snapshot-path)
    tmp-path))

(defn query [dba [query-type :as q]]
  (assert (dba? dba))
  (if (write-query-type? query-type)
    (exec/execute (:write-pipe ^dba)
      #(let [old-db          @dba
             [new-db result] (core/query old-db q)]
         (if-let [write-dos (:write-dos ^dba)]
           (io/dos-write write-dos q))
         (assert (compare-and-set! dba old-db new-db))
         result))
    (core/query @dba q)))

(defn close [dba]
  (assert (dba? dba))
  (let [write-pipe (:write-pipe ^dba)]
    (exec/shutdown-now (:write-pipe ^dba))
    (exec/await-termination (:write-pipe ^dba) 60))
  (if-let [write-dos (:write-dos ^dba)]
    (io/dos-close write-dos))
  true)
