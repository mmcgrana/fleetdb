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

(defn- client-write [client #^String text]
  (let [#^BufferedWriter writer (:writer client)]
    (.write writer text)
    (.flush writer)))

(defn- client-read [client]
  (str (.readLine #^BufferedReader (:reader client)) "\r\n"))

(defn- client-close [client]
  (.close #^BufferedReader (:reader client))
  (.close #^BufferedWriter (:writer client))
  (.close #^Socket         (:socket client)))

(defmacro with-client [name port & body]
  `(let [~name (client-connect "127.0.0.1" ~port)]
     (try
       ~@body
       (finally
         (client-close ~name)))))

(defn- test-ping [client]
  (client-write client "[\"ping\"]\r\n")
  (assert= "[0,\"pong\"]\r\n" (client-read client)))

(deftest "ping"
  (with-client client 3400
    (test-ping client)))

(deftest "valid query"
  (with-client client 3400
    (client-write client "[\"insert\",\"elems\",{\"id\":1}]\r\n")
    (assert= "[0,1]\r\n" (client-read client))
    (client-write client "[\"select\",\"elems\"]\r\n")
    (assert= "[0,[{\"id\":1}]]\r\n" (client-read client))
    (client-write client "[\"delete\", \"elems\"]\r\n")
    (assert= (client-read client) "[0,1]\r\n")))

(deftest "malformed query"
  (with-client client 3400
    (client-write client "[\"foo\"]\r\n")
    (assert= "[1,\"Malformed query: unrecognized query type '\\\"foo\\\"'\"]\r\n"
             (client-read client))))

(deftest "malformed json"
  (with-client client 3400
    (client-write client "{]\r\n")
    (assert-match #"Unexpected close marker"
                  (client-read client))))

(deftest "auth on non-protected server"
  (with-client client 3400
    (client-write client "[\"auth\", \"foo\"]\r\n")
    (assert= "[0,\"auth unneeded\"]\r\n" (client-read client))
    (test-ping client)))

(deftest "good auth on protected server"
  (with-client client 3401
    (client-write client "[\"auth\", \"pass\"]\r\n")
    (assert= "[0,\"auth accepted\"]\r\n" (client-read client))
    (client-write client "[\"auth\", \"pass\"]\r\n")
    (assert= "[0,\"auth unneeded\"]\r\n" (client-read client))
    (test-ping client)))

(defn- test-auth-tryop [client]
  (assert-throws #"Connection reset"
    (do
      (client-write client "[\"insert\",\"elems\",{\"id\":1}]\r\n")
      (client-read client))))

(defn- test-auth-noop []
  (with-client client 3401
    (client-write client "[\"auth\", \"pass\"]\r\n")
    (assert= "[0,\"auth accepted\"]\r\n" (client-read client))
    (client-write client "[\"count\",\"elems\"]\r\n")
    (assert= "[0,0]\r\n" (client-read client))))

(deftest "bad auth on protected server"
  (with-client client 3401
    (client-write client "[\"auth\", \"nopass\"]\r\n")
    (assert= "[1,\"auth rejected\"]\r\n" (client-read client))
    (test-auth-tryop client))
  (test-auth-noop))

(deftest "no auth on protected server"
  (with-client client 3401
    (client-write client "[\"insert\",\"elems\",{\"id\":1}]\r\n")
    (assert= "[1,\"auth needed\"]\r\n" (client-read client))
    (test-auth-tryop client))
  (test-auth-noop))