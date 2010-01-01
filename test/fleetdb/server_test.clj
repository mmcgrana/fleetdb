(ns fleetdb.server-test
  (:import (java.net Socket)
           (java.io  OutputStreamWriter BufferedWriter
                     InputStreamReader BufferedReader))
  (:use (clj-unit core)))

(defn- client-connect [#^String host #^Integer port]
  (let [socket (Socket. host port)]
    {:writer (BufferedWriter. (OutputStreamWriter. (.getOutputStream  socket)))
     :reader (BufferedReader. (InputStreamReader.  (.getInputStream socket)))
     :socket socket}))

(defn- client-write [client text]
  (let [#^BufferedWriter writer (:writer client)]
    (.write writer text)
    (.flush writer)))

(defn- client-read [client]
  (str (.readLine #^BufferedReader (:reader client)) "\r\n"))

(defn- client-close [client]
  (.close #^BufferedReader (:reader client))
  (.close #^BufferedWriter (:writer client))
  (.close #^Socket         (:socket client)))

(defmacro with-client [name & body]
  `(let [~name (client-connect "127.0.0.1" 3400)]
     (try
       (client-write ~name "[\"delete\", \"elems\"]\r\n")
       (client-read ~name)
       ~@body
       (finally
         (client-close ~name)))))

(deftest "ping"
  (with-client client
    (client-write client "[\"ping\"]\r\n")
    (assert= "[0,\"pong\"]\r\n" (client-read client))))

(deftest "valid query"
  (with-client client
    (client-write client "[\"insert\",\"elems\",{\"id\":1}]\r\n")
    (assert= "[0,1]\r\n" (client-read client))
    (client-write client "[\"select\",\"elems\"]\r\n")
    (assert= "[0,[{\"id\":1}]]\r\n" (client-read client))))

(deftest "malformed query"
  (with-client client
    (client-write client "[\"foo\"]\r\n")
    (assert= "[1,\"Malformed query: unrecognized query type '\\\"foo\\\"'\"]\r\n"
             (client-read client))))

(deftest "malformed json"
  (with-client client
    (client-write client "{]\r\n")
    (assert-match #"Unexpected close marker"
                  (client-read client))))
