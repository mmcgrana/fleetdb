(ns fleetdb.exec
  (:import (java.util.concurrent ExecutorService Executors TimeUnit)))

(defn init-pipe []
  (Executors/newSingleThreadExecutor))

(defn init-pool [size]
  (Executors/newFixedThreadPool size))

(defn submit [#^ExecutorService executor #^Callable f]
  (.submit executor f))

(defn execute [#^ExecutorService executor #^Callable f]
  (.get (.submit executor f)))

(defn shutdown [#^ExecutorService executor]
  (.shutdown executor))

(defn shutdown-now [#^ExecutorService executor]
  (.shutdownNow executor))

(defn await-termination [#^ExecutorService executor timeout-secs]
  (.awaitTermination executor timeout-secs TimeUnit/SECONDS))

(defn async [#^Runnable f]
  (.start (Thread. f)))
