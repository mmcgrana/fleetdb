(ns fleetdb.fair-lock
  (:import (java.util.concurrent.locks ReentrantLock)
           (java.util.concurrent TimeUnit)))

(defn init []
  (ReentrantLock. true))

(defmacro fair-locking [lock & body]
  `(let [#^ReentrantLock l# ~lock]
     (.lock l#)
     (try
       ~@body
       (finally
         (.unlock l#)))))

(defn join [#^ReentrantLock lock timeout]
  (.tryLock lock timeout TimeUnit/SECONDS))
