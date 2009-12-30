(ns fleetdb.embedded-test
  (:require (fleetdb [embedded :as embedded] [io :as io] [file :as file]))
  (:use (clj-unit core) (fleetdb util)))

(defmacro- with-dba [[name dba-form] & body]
  `(let [~name ~dba-form]
     (try
       ~@body
       (finally
         (embedded/close ~name)))))

(def- snapshot-path "/tmp/fleetdb-snapshot")
(def- log-path      "/tmp/fleetdb-log")

(deftest "ephemeral lifecycle"
  (with-dba [dba-0 (embedded/init-ephemeral)]
    (assert-that dba-0)
    (embedded/query dba-0 ["insert" "elems" [{"id" 1} {"id" 2}]])
    (assert= 2 (embedded/query dba-0 ["count" "elems"]))
    (embedded/query dba-0 ["create-index" "elems" [["name" "asc"]]])
    (embedded/snapshot dba-0 snapshot-path)
    (embedded/query dba-0 ["insert" "elems" {"id" 3}]))
  (with-dba [dba-1 (embedded/load-ephemeral snapshot-path)]
    (assert= 2 (embedded/query dba-1 ["count" "elems"]))
    (assert= [[["name" "asc"]]] (embedded/query dba-1 ["list-indexes" "elems"]))
    (with-dba [dba-2 (embedded/fork dba-1)]
      (embedded/query dba-1 ["insert" "elems" {"id" 3}])
      (assert= 2 (embedded/query dba-2 ["count" "elems"])))))

(deftest "persistent lifecycle"
  (file/rm log-path)
  (with-dba [dba-0 (embedded/init-persistent log-path)]
    (embedded/query dba-0 ["insert" "elems" [{"id" 1} {"id" 2}]])
    (assert= 2 (embedded/query dba-0 ["count" "elems"]))
    (dotimes [i 100]
      (embedded/query dba-0 ["insert" "elems" {"id" (+ 3 i)}])
      (when (odd? i)
        (embedded/query dba-0 ["delete" "elems" {"where" ["=" "id" i]}])))
    (let [pre-compact-size (file/size log-path)]
      (embedded/compact dba-0)
      (Thread/sleep 100)
      (let [post-compact-size (file/size log-path)]
        (assert-that (< post-compact-size pre-compact-size)))))
  (with-dba [dba-1 (embedded/load-persistent log-path)]
    (assert= 52 (embedded/query dba-1 ["count" "elems"]))))

(deftest "catches linting errors"
  (with-dba [dba (embedded/init-ephemeral)]
    (assert-throws #"Malformed query: query not a vector '\"foo\"'"
      (embedded/query dba "foo"))))
