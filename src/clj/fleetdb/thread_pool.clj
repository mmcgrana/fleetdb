(ns fleetdb.thread-pool
  (:import (java.util.concurrent ExecutorService Executors)))

(defn init [size]
  (Executors/newFixedThreadPool size))

(defn submit [#^ExecutorService executor #^Callable f]
  (.submit executor f))
