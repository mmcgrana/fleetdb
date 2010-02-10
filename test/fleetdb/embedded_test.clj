(ns fleetdb.embedded-test
  (:require (fleetdb [embedded :as embedded] [file :as file]))
  (:use (clj-unit core) (fleetdb util))
  (:import (java.io File RandomAccessFile)))

(defmacro- with-dba [[name dba-form] & body]
  `(let [~name ~dba-form]
     (try
       ~@body
       (finally
         (embedded/close ~name)))))

(def- snapshot-path "/tmp/fleetdb-snapshot")
(def- log-path      "/tmp/fleetdb-log")

(deftest "embeded: catches linting errors"
  (with-dba [dba (embedded/init-ephemeral)]
    (assert-throws #"Malformed query: query not a vector '\"foo\"'"
      (embedded/query dba "foo"))))

(deftest "ephemeral: init"
  (with-dba [dba-0 (embedded/init-ephemeral)]
    (assert-that dba-0)
    (assert= 0 (embedded/query dba-0 ["count" "elems"]))))

(deftest "ephemeral: in-process state"
  (with-dba [dba-0 (embedded/init-ephemeral)]
    (embedded/query dba-0 ["insert" "elems" [{"id" 1} {"id" 2}]])
    (assert= 2 (embedded/query dba-0 ["count" "elems"]))))

(deftest "ephemeral: snapshot and load"
  (with-dba [dba-0 (embedded/init-ephemeral)]
    (embedded/query dba-0 ["insert" "elems" [{"id" 1} {"id" 2}]])
    (embedded/query dba-0 ["create-index" "elems" [["name" "asc"]]])
    (embedded/snapshot dba-0 snapshot-path)
    (embedded/query dba-0 ["insert" "elems" {"id" 3}]))
    (with-dba [dba-1 (embedded/load-ephemeral snapshot-path)]
      (assert= 2 (embedded/query dba-1 ["count" "elems"]))
      (assert= [[["name" "asc"]]] (embedded/query dba-1 ["list-indexes" "elems"]))))

(deftest "ephemeral: fork"
  (with-dba [dba-0 (embedded/init-ephemeral)]
    (embedded/query dba-0 ["insert" "elems" [{"id" 1} {"id" 2}]])
    (with-dba [dba-1 (embedded/fork dba-0)]
      (embedded/query dba-1 ["insert" "elems" {"id" 3}])
      (assert= 2 (embedded/query dba-0 ["count" "elems"]))
      (assert= 3 (embedded/query dba-1 ["count" "elems"])))))

(deftest "persistent: init"
  (file/rm log-path)
  (with-dba [dba-0 (embedded/init-persistent log-path)]
    (assert= 0 (embedded/query dba-0 ["count" "elems"]))))

(deftest "persitent: in-process state"
  (file/rm log-path)
  (with-dba [dba-0 (embedded/init-persistent log-path)]
    (embedded/query dba-0 ["insert" "elems" [{"id" 1} {"id" 2}]])
    (assert= 2 (embedded/query dba-0 ["count" "elems"]))))

(deftest "persistent: load"
  (file/rm log-path)
  (with-dba [dba-0 (embedded/init-persistent log-path)]
    (embedded/query dba-0 ["insert" "elems" [{"id" 1} {"id" 2}]])
    (with-dba [dba-1 (embedded/load-persistent log-path)])
    (with-dba [dba-2 (embedded/load-persistent log-path)]
      (assert= 2 (embedded/query dba-2 ["count" "elems"]))
      (embedded/query dba-2 ["insert" "elems" {"id" 3}])
      (with-dba [dba-3 (embedded/load-persistent log-path)])
      (with-dba [dba-4 (embedded/load-persistent log-path)]
        (assert= 3 (embedded/query dba-4 ["count" "elems"])))))
  (assert=
    "[\"insert\",\"elems\",[{\"id\":1},{\"id\":2}]]\r\n[\"insert\",\"elems\",{\"id\":3}]\r\n"
    (slurp log-path)))

(deftest "persistent: compact"
  (file/rm log-path)
  (with-dba [dba-0 (embedded/init-persistent log-path)]
    (dotimes [i 100]
      (embedded/query dba-0 ["insert" "elems" {"id" (+ 3 i)}])
      (when (odd? i)
        (embedded/query dba-0 ["delete" "elems" {"where" ["=" "id" i]}])))
    (let [pre-compact-size (file/size log-path)]
      (embedded/compact dba-0)
      (Thread/sleep 100)
      (let [post-compact-size (file/size log-path)]
        (assert-fn #(< % pre-compact-size) post-compact-size)))
  (with-dba [dba-1 (embedded/load-persistent log-path)]
    (assert= 51 (embedded/query dba-1 ["count" "elems"])))))

(deftest "persistent: check"
  (file/rm log-path)
  (with-dba [dba-0 (embedded/init-persistent log-path)]
    (dotimes [i 3]
      (embedded/query dba-0 ["insert" "elems" {"id" (+ 3 i)}])))
  (.setLength (RandomAccessFile. (File. #^String log-path) "rw") 70)
  (with-dba [dba-1 (embedded/load-persistent log-path)]
    (assert= 2 (embedded/query dba-1 ["count" "elems"]))))
