(ns fleetdb.server
  (:use     (fleetdb [util :only (def-)]))
  (:require (fleetdb [embedded :as embedded] [io :as io] [exec :as exec])
            (clj-stacktrace [repl :as stacktrace]))
  (:import  (java.net ServerSocket Socket)
            (java.io PushbackReader BufferedReader InputStreamReader
                     PrintWriter    BufferedWriter OutputStreamWriter
                     DataInputStream  BufferedInputStream
                     DataOutputStream BufferedOutputStream )))

(defn- safe-query [dba query]
  (if (= query [:ping])
    "pong"
    (try
      (let [result (embedded/query dba query)]
        (if (coll? result) (vec result) result))
      (catch Exception e e))))

(defn- text-read-query [#^PushbackReader in eof-val]
  (try
    (read in false eof-val)
    (catch Exception e e)))

(defn- text-write-exception [#^PrintWriter out e]
  (stacktrace/pst-on out false e)
  (.flush out))

(defn- text-write-result [#^PrintWriter out result]
  (.println out (pr-str result))
  (.flush out))

(defn- text-handler [dba #^Socket socket]
  (try
    (with-open [socket socket
                out    (PrintWriter.    (BufferedWriter. (OutputStreamWriter. (.getOutputStream socket))))
                in     (PushbackReader. (BufferedReader. (InputStreamReader.  (.getInputStream  socket))))]
      (loop []
        (let [read-result (text-read-query in io/eof)]
          (when-not (identical? io/eof read-result)
            (if (instance? Exception read-result)
              (text-write-exception out read-result)
              (let [query-result (safe-query dba read-result)]
                (if (instance? Exception query-result)
                  (text-write-exception out query-result)
                  (text-write-result out query-result))))
            (recur)))))
    (catch Exception e
      (stacktrace/pst-on System/err false e)
      (.println System/err))))

(defn- binary-read-query [#^DataInputStream in eof-val]
  (try
    (io/dis-read in eof-val)
    (catch Exception e e)))

(defn- binary-write-exception [#^DataOutputStream out e]
  (io/dos-write out [1 (stacktrace/pst-str e)]))

(defn- binary-write-result [#^DataOutputStream out result]
  (io/dos-write out [0 result]))

(defn- binary-handler [dba #^Socket socket]
  (try
    (with-open [socket socket
                out    (DataOutputStream. (BufferedOutputStream. (.getOutputStream socket)))
                in     (DataInputStream.  (BufferedInputStream.  (.getInputStream  socket)))]
      (.setKeepAlive socket true)
      (loop []
        (let [read-result (binary-read-query in io/eof)]
          (when-not (identical? io/eof read-result)
            (if (instance? Exception read-result)
              (binary-write-exception out read-result)
              (let [query-result (safe-query dba read-result)]
                (if (instance? Exception query-result)
                  (binary-write-exception out query-result)
                  (binary-write-result out query-result))))
            (recur)))))
    (catch Exception e
      (stacktrace/pst-on System/err false e)
      (.println System/err))))

(def- protocol-handlers
  {:binary binary-handler :text text-handler})

(defn run [{:keys [port protocol threads persistent db-path]}]
  (let [server-socket (ServerSocket. port)
        pool          (exec/init-pool threads)
        handler       (protocol-handlers protocol)
        loading       (io/exist? db-path)
        dba           (if persistent
                        (if loading
                          (embedded/load-persistent db-path)
                          (embedded/init-persistent db-path))
                        (if loading
                          (embedded/load-ephemral db-path)
                          (embedded/init-ephemral)))]
    (println "FleetDB serving port" port)
    (loop []
      (let [socket (doto (.accept server-socket))]
        (exec/submit pool #(handler dba socket)))
      (recur))))
