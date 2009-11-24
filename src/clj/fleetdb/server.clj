(ns fleetdb.server
  (:require (fleetdb [embedded :as embedded] [io :as io] [exec :as exec])
            (clj-stacktrace [repl :as stacktrace]))
  (:import  (java.net ServerSocket Socket)
            (java.io PrintWriter BufferedReader InputStreamReader PushbackReader)))

(defn- read-query [#^PushbackReader in eof-val]
  (try
    (read in false eof-val)
    (catch Exception e e)))

(defn- write-exception [#^PrintWriter out e]
  (stacktrace/pst-on out false e)
  (.println out)
  (.flush out))

(defn- write-result [#^PrintWriter out result]
  (.println out (prn-str result))
  (.flush out))

(defn- safe-query [dba query]
  (try
    (let [result (embedded/query dba query)]
      (if (coll? result) (doall result) result))
    (catch Exception e e)))

(defn- text-handler [dba #^Socket socket]
  (try
    (with-open [socket socket
                out    (PrintWriter. (.getOutputStream socket))
                in     (PushbackReader. (BufferedReader. (InputStreamReader. (.getInputStream socket))))]
      (loop []
        (let [read-result (read-query in io/eof)]
          (when-not (identical? io/eof read-result)
            (if (instance? Exception read-result)
              (write-exception out read-result)
              (let [query-result (safe-query dba read-result)]
                (if (instance? Exception query-result)
                  (write-exception out query-result)
                  (write-result out query-result))))
            (recur)))))
    (catch Exception e
      (stacktrace/pst-on System/err true e))))

(defn run [read-path write-path text-port]
  (let [text-ss (ServerSocket. text-port)
        dba     (embedded/init read-path write-path)
        pool    (exec/init-pool 100)]
    (println "FleetDB serving text protocol from port" text-port)
    (loop []
      (let [socket (.accept text-ss)]
        (exec/submit pool #(text-handler dba socket)))
      (recur))))

(use 'clojure.contrib.shell-out)
(sh "rm" "-f" "/Users/mmcgrana/Desktop/log")
(run nil "/Users/mmcgrana/Desktop/log" 4444)
