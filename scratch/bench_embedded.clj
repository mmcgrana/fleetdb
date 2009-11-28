(set! *warn-on-reflection* true)

(ns scratch.bench-embedded
  (:require (fleetdb [embedded :as embedded] [io :as io])
            (scratch [bench-util :as bench])))

(defn record-ids [n]
  (map #(+ 10000000 %) (range n)))

(defn records [n]
  (for [i (record-ids n)]
    {:id         i
     :created-at (+ i 10000000)
     :updated-at (+ i 20000000)
     :title      (str i "is the best number ever")}))

(def n 1000000)
(def k 100)
(def db-log-path "/tmp/fleetdb-bench-embedded")

(defn- build [label initialize]
  ;(bench/timed label
  (bench/profiled
    #(let [dba (initialize)]
       (doseq [record (records n)]
         (embedded/query dba [:insert :elems record]))
       (embedded/close dba))))

(defn- chunked-build [label initialize]
  (bench/timed label
    #(let [dba (initialize)]
       (doseq [record-chunk (partition k (records n))]
         (embedded/query dba [:insert :elems (vec record-chunk)]))
       (embedded/close dba))))

(println "-- embedded")
(println "n =" n)
(println "k =" k)

(build         "build ephemral:          "
               embedded/init-ephemral)
(chunked-build "chunked build ephemral:  "
               embedded/init-ephemral)

(io/rm db-log-path)
(build         "build persistent:        "
               #(embedded/init-persistent db-log-path))
(chunked-build "chunked build persistent:"
               #(embedded/init-persistent db-log-path))





