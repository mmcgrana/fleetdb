(ns fleetdb.client
  (:require (fleetdb [io :as io]))
  (:import (java.net Socket)))

(defn connect [#^String host #^Integer port]
  (let [socket (Socket. host port)]
    {:parser    (is->parser    (.getInputStream  socket))
     :generator (os->generator (.getOutputStream socket))
     :socket    socket}))

(defn query [client q]
  (io/generate (:generator client) q)
  (let [resp (io/parse (:parser client) io/eof)]
    (if (identical? resp io/eof)
      (throw (Exception. "End of server input."))
      resp)))

(defn close [client]
  (io/parser-close    (:parser    client))
  (io/generator-close (:generator client))
  (.close    #^Socket (:socket    client)))
