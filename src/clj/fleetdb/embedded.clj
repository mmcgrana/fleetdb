(ns fleetdb.embedded
  (:use [fleetdb.util :only (def-)])
  (:require (fleetdb [core :as core] [exec :as exec] [io :as io])))

(def- write-query-type?
  #{:insert :update :delete :create-index :drop-index
    :multi-write :checked-write})

(defn- dba? [dba]
  (if (:write-pipe (meta dba)) true))

(defn- persistent? [dba]
  (:write-path (meta dba)))

(defn- header-read-path [header]
  (let [{:keys [compact-prev-path prev-path]} header]
    (if (and compact-prev-path (io/exist? compact-prev-path))
      compact-prev-path
      prev-path)))

(defn- read-from [read-path]
  (let [dis       (io/dis-init read-path)
        header    (io/dis-read! dis)
        prev-path (header-read-path header)
        prev-db   (if prev-path (read-from prev-path) (core/init))]
    (reduce #(first (core/query %1 %2)) prev-db (io/dis-seq dis))))

(defn- write-to [db write-path]
  (let [dos (io/dos-init write-path)]
    (doseq [records (partition 100 (core/query db [:select]))]
      (io/dos-write dos [:insert {:records (vec records)}]))
    (doseq [ispec (core/query db [:list-indexes])]
      (io/dos-write dos [:create-index {:on ispec}]))
    (io/dos-close dos)))

(defn- simplified-write-query [[q-type q-opts :as q]]
  (if (= (q-type :checked-write)) (:write q) q))

(defn init [read-path write-path]
  (let [db         (if read-path (read-from read-path) (core/init))
        write-dos  (if write-path (io/dos-init write-path))
        write-pipe (exec/init-pipe)]
    (if write-dos
      (io/dos-write write-dos {:prev-path read-path}))
    (atom db :meta {:write-pipe write-pipe
                    :write-path write-path :write-dos write-dos})))

(defn snapshot [dba snapshot-path tmp-dir-path]
  (assert (dba? dba))
  (assert (not (persistent? dba)))
  (let [tmp-path ( io/tmp-path tmp-dir-path "snapshot")
        write-dos (:write-dos (meta dba))]
    (write-to @dba tmp-path)
    (io/rename tmp-path snapshot-path)
    tmp-path))

(defn branch [dba new-write-path]
  (assert (dba? dba))
  (assert (persistent? dba))
  (exec/execute (:write-pipe (meta dba))
    #(let [old-write-path (:write-path (meta dba))
           old-write-dos  (:write-dos  (meta dba))
           new-write-dos  (io/dos-init new-write-path)]
       (io/dos-write new-write-dos {:prev-path old-write-path})
       (alter-meta! dba assoc
         :write-path new-write-path :write-dos new-write-dos)
       (io/dos-close old-write-dos)
       old-write-path)))

(defn compact [dba compact-path new-write-path tmp-dir-path]
  (assert (dba? dba))
  (assert (persistent? dba))
  (exec/execute (:write-pipe (meta dba))
    #(let [old-write-path (:write-path (meta dba))
           old-write-dos  (:write-dos  (meta dba))
           tmp-write-path (io/tmp-path tmp-dir-path "compact")
           new-write-dos  (io/dos-init new-write-path)
           db             @dba]
       (io/dos-write new-write-dos
         {:compact-prev-path compact-path :prev-path old-write-path})
       (alter-meta! dba assoc
         :write-path new-write-path :write-dos new-write-dos)
       (io/dos-close old-write-dos)
       (exec/async (fn []
         (write-to db tmp-write-path)
         (io/rename tmp-write-path compact-path)))
       tmp-write-path)))

(defn query [dba [query-type :as q]]
  (assert (dba? dba))
  (if (write-query-type? query-type)
    (exec/execute (:write-pipe (meta dba))
      #(let [old-db          @dba
             [new-db result] (core/query old-db q)]
         (if-let [write-dos (:write-dos (meta dba))]
           (io/dos-write write-dos (simplified-write-query q)))
         (assert (compare-and-set! dba old-db new-db))
         result))
    (core/query @dba q)))

(defn close [dba]
  (let [{:keys [write-pipe write-dos]} (meta dba)]
    (exec/shutdown-now write-pipe)
    (exec/await-termination write-pipe 60)
    (if write-dos (io/dos-close write-dos))))
