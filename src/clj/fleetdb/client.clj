(ns fleetdb.client
  (:use (fleetdb [util :only (def-)]))
  (:require (fleetdb [io :as io])
            (clojure.contrib [str-utils :as str-utils]))
  (:import (java.net Socket)
           (java.io DataInputStream  BufferedInputStream
                    DataOutputStream BufferedOutputStream)))

(def- binary-handler
  [(fn [out q]
     (io/dos-serialize out q))
   (fn [in]
     (let [resp (io/dis-deserialize in io/eof)]
       (if (= resp io/eof)
         (throw (Exception. "Server disconnected"))
         (let [[status val] resp]
           (if (zero? status)
             val
             (throw (Exception. #^String val)))))))])

(def- bert-handler
  [(fn [out q]
     (io/dos-berp-encode out q))
   (fn [in]
     (let [resp (io/dis-berp-decode in io/eof)]
       (if (= resp io/eof)
         (throw (Exception. "Server disconnected"))
         (let [[status val] resp]
           (if (zero? status)
             val
             (throw (Exception. #^String val)))))))])

(def- bert-rpc-handler
  [(fn [out q]
     (io/dos-berp-encode out [:call :server :query [q]]))
   (fn [in]
     (let [resp (io/dis-berp-decode in io/eof)]
       (if (= resp io/eof)
         (throw (Exception. "Server disconnected"))
         (let [[status vals] resp]
           (if (= status :reply)
             vals
             (let [[type code class detail stacktrace] vals
                   msg (if (= type :user)
                         (str detail)
                         (str detail (str-utils/str-join "\n" stacktrace)))]
               (throw (Exception. msg))))))))])

(def- protocol-handlers
  {:binary binary-handler :bert bert-handler :bert-rpc bert-rpc-handler})

(defn connect [#^String host #^Integer port & [protocol]]
  (let [protocol (or protocol :bert-rpc)]
    (assert (#{:binary :bert :bert-rpc} protocol))
    (let [socket             (Socket. host port)
          [write-fn read-fn] (protocol-handlers protocol)]
      {:dos      (DataOutputStream. (BufferedOutputStream. (.getOutputStream socket)))
       :dis      (DataInputStream.  (BufferedInputStream.  (.getInputStream  socket)))
       :write-fn write-fn
       :read-fn  read-fn
       :socket   socket})))

(defn query [client q]
  ((:write-fn client) (:dos client) q)
  ((:read-fn client) (:dis client)))

(defn close [client]
  (io/dis-close (:dis client))
  (io/dos-close (:dos client))
  (.close #^Socket (:socket client)))
