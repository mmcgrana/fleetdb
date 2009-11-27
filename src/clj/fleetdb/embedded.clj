(ns fleetdb.embedded
  (:use [fleetdb.util :only (def- ?)]
        [clojure.contrib.seq-utils :only (partition-all)])
  (:require (fleetdb [core :as core] [exec :as exec] [io :as io]))
  (:import  (java.util ArrayList)))

(def- write-query-type?
  #{:insert :update :delete
    :create-index :drop-index
    :multi-write :checked-write})

(defn- dba? [dba]
  (? (:write-pipe ^dba)))

(defn- persistent? [dba]
  (? (:write-dos ^dba)))

(defn- ephemral? [dba]
  (not (persistent? dba)))

(defn- compacting? [dba]
  (? (:write-buf ^dba)))

(defn- replay-command [db query]
  (first (core/query db query)))

(defn- read-from [read-path]
  (let [dis       (io/dis-init read-path)
        header    (io/dis-read! dis)
        commands  (io/dis-seq dis)
        empty     (core/init)]
    (reduce replay-command empty commands)))

(defn- write-to [db write-path]
  (let [dos (io/dos-init write-path)]
    (io/dos-write dos {:root true})
    (doseq [coll (core/query db [:list-colls])]
      (doseq [records (partition-all 100 (core/query db [:select coll]))]
        (io/dos-write dos [:insert coll (vec records)]))
      (doseq [ispec (core/query db [:list-indexes coll])]
        (io/dos-write dos [:create-index coll ispec])))
    (io/dos-close dos)))

(defn- init* [db & [other-meta]]
  (atom db :meta (assoc other-meta :write-pipe (exec/init-pipe))))

(defn init-ephemral []
  (init* (core/init)))

(defn load-ephemral [read-path]
  (init* (read-from read-path)))

(defn init-persistent [write-path]
  (let [write-dos (io/dos-init write-path)]
    (io/dos-write write-dos {:root true})
    (init* (core/init) {:write-dos write-dos :write-path write-path})))

(defn load-persistent [read-write-path]
  (let [db        (read-from read-write-path)
        write-dos (io/dos-init read-write-path)]
    (init* db {:write-dos write-dos :write-path read-write-path})))

(defn close [dba]
  (exec/join (:write-pipe ^dba) 60)
  (if-let [write-dos (:write-dos ^dba)]
    (io/dos-close write-dos))
  true)

(defn fork [dba]
  (assert (ephemral? dba))
  (init* @dba))

(defn snapshot [dba snapshot-path]
  (assert (ephemral? dba))
  (let [tmp-path  (io/tmp-path "/tmp" "snapshot")]
    (write-to @dba tmp-path)
    (io/mv tmp-path snapshot-path)
    true))

(defn compact [dba]
  (assert (persistent? dba))
  (assert (not (compacting? dba)))
  (exec/execute (:write-pipe ^dba)
    #(let [tmp-path      (io/tmp-path "/tmp" "compact")
           db-comp-start @dba]
       (alter-meta! dba assoc :write-buf (ArrayList.))
       (exec/async (fn []
         (write-to db-comp-start tmp-path)
         (exec/execute (:write-pipe ^dba)
           (fn []
             (let [dos (io/dos-init tmp-path)]
               (doseq [post-comp-command (:write-buf ^dba)]
                 (io/dos-write dos post-comp-command))
               (io/mv tmp-path (:write-path ^dba))
               (io/dos-close (:write-dos ^dba))
               (alter-meta! dba dissoc :write-buf)
               (alter-meta! dba assoc  :write-dos dos))))))
       true)))

(defn query [dba [query-type :as q]]
  (assert (dba? dba))
  (if (write-query-type? query-type)
    (exec/execute (:write-pipe ^dba)
      #(let [old-db          @dba
             [new-db result] (core/query old-db q)]
         (when-let [write-dos (:write-dos ^dba)]
           (io/dos-write write-dos q)
           (when-let [#^ArrayList write-buf (:write-buf ^dba)]
             (.add write-buf q)))
         (assert (compare-and-set! dba old-db new-db))
         result))
    (core/query @dba q)))
