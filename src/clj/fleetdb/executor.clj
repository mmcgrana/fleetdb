(ns fleetdb.executor
  (java.util.concurrent ExecutorService Executors))

(defn init []
  (Executors/newSingleThreadExecutor))

(defn shutdown [#^ExecutorService executor]
  (.shutdownNow pipeliner))

(defn execute [#^ExecutorService executor #^Callable f]
  (.get (.submit executor f)))