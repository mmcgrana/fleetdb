(ns fleetdb.client
  (:use (fleetdb [util :only (def-)]))
  (:require (fleetdb [io :as io]))
  (:import (java.net Socket)
           (java.io DataInputStream  BufferedInputStream
                    DataOutputStream BufferedOutputStream)))

(def- read-fns
  {:binary io/dis-deserialize :bert io/dis-berp-decode})

(def- write-fns
  {:binary io/dos-serialize   :bert io/dos-berp-encode})

(defn connect [#^String host #^Integer port protocol]
  (assert (#{:binary :bert} protocol))
  (let [socket (Socket. host port)]
    {:dis      (DataInputStream.  (BufferedInputStream.  (.getInputStream  socket)))
     :dos      (DataOutputStream. (BufferedOutputStream. (.getOutputStream socket)))
     :read-fn  (read-fns protocol)
     :write-fn (write-fns protocol)
     :socket   socket}))

(defn query [client q]
  ((:write-fn client) (:dos client) q)
  (let [result ((:read-fn client) (:dis client) io/eof)]
    (assert (not= result io/eof))
    result))

(defn close [client]
  (io/dis-close (:dis client))
  (io/dos-close (:dos client))
  (.close #^Socket (:socket client)))
