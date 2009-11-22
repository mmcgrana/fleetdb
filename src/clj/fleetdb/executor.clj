(ns fleetdb.executor
  (:import (java.util.concurrent ExecutorService Executors)))

(defn init []
  (Executors/newSingleThreadExecutor))

(defn shutdown [#^ExecutorService executor]
  (.shutdownNow executor))

(defn execute [#^ExecutorService executor #^Callable f]
  (.get (.submit executor f)))