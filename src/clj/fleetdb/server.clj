(ns fleetdb.server
  (:require (fleetdb [embedded :as embedded] [io :as io])
            (clj-stacktrace [repl :as stacktrace]))
  (:import  (java.net ServerSocket Socket)
            (java.io PrintWriter BufferedReader InputStreamReader PushbackReader)))

(defn- read-query [#^PushbackReader in eof-val]
  (try
    (read in false eof-val)
    (catch Exception e e)))

(defn- write-exception [#^PrintWriter out e]
  (stacktrace/pst-on out false e)
  (.flush out))

(defn- write-result [#^PrintWriter out result]
  (.println out (prn-str result))
  (.flush out))

(defn- safe-query [dba query]
  (try
    (embedded/query dba query)
    (catch Exception e e)))

(defn- handle-text [dba #^Socket socket]
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
          (recur))))))

(defn run [read-path write-path port]
  (let [server-socket (ServerSocket. port)
        dba           (embedded/init read-path write-path)]
    (println "FleetDB serving port" port)
    (loop []
      (handle-text dba (.accept server-socket))
      (recur))))

(use 'clojure.contrib.shell-out)
(sh "rm" "-f" "/Users/mmcgrana/Desktop/log")
(run nil "/Users/mmcgrana/Desktop/log" 4444)
