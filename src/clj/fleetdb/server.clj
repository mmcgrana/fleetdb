(ns fleetdb.server
  (:use     (fleetdb util))
  (:require (fleetdb [file :as file] [thread-pool :as thread-pool]
                     [lint :as lint] [embedded :as embedded])
            fleetdb
            (clj-stacktrace [repl :as stacktrace])
            (clojure.contrib [str-utils :as str-utils])
            [clj-json :as json])
  (:import  (java.net ServerSocket Socket InetAddress)
            (java.io BufferedWriter OutputStreamWriter
                     BufferedReader InputStreamReader)
            (org.codehaus.jackson JsonParseException)
            (fleetdb FleetDBException)
            (joptsimple OptionParser OptionSet OptionException))
  (:gen-class))

(defn- info-map [dba]
  (let [base {"fleetdb-version" fleetdb/version}
        pers (embedded/persistent? dba)]
    (if pers
      (assoc base
        "persistent"   true
        "db-file-size" (file/size (embedded/write-path dba))
        "compacting"   (embedded/compacting? dba))
      (assoc base
        "persistent"   false))))

(defn- process-query [dba q needs-auth? password]
  (lint/lint-query q)
  (if needs-auth?
    (if (= (first q) "auth")
      (if (= (second q) password)
        ["auth accepted" true]
        ["auth rejected" false])
      ["auth needed" false])
    [(condp = (first q)
       "auth"    "auth unneeded"
       "ping"    "pong"
       "compact" (embedded/compact dba)
       "info"    (info-map dba)
       (embedded/query* dba q))
     false]))

(defn- write-response [#^BufferedWriter out resp]
  (.write out #^String (json/generate-string resp))
  (.write out "\n")
  (.flush out))

(defn- recognized-exception? [#^Exception e]
  (or (instance? FleetDBException e)
      (instance? JsonParseException e)))

(defn handler [dba #^Socket socket password]
  (try
    (with-open [socket socket
                in     (BufferedReader. (InputStreamReader.  (.getInputStream socket)))
                out    (BufferedWriter. (OutputStreamWriter. (.getOutputStream socket)))]
      (.setKeepAlive socket true)
      (loop [needs-auth? (? password)]
        (let [got-auth?
                (try
                  (if-let [in-line (.readLine in)]
                    (let [query              (json/parse-string in-line)
                          [result got-auth?] (process-query dba query needs-auth? password)]
                      (write-response out
                        [(if (or (not needs-auth?) got-auth?) 0 1) result])
                      got-auth?))
                  (catch Exception e
                    (write-response out
                      (if (recognized-exception? e)
                        [1 (.getMessage e)]
                        [2 (stacktrace/pst-str e)]))
                    false))]
            (when-not (nil? got-auth?)
              (when (or (not needs-auth?) got-auth?)
                (recur false))))))
    (catch Exception e
      (stacktrace/pst-on System/err false e)
      (.println System/err))))

(defn- report-loading-start []
  (print "Loading database file...")
  (flush))

(defn- report-loading-end []
  (println "done"))

(defn- report-ready [port]
  (println "FleetDB listening on port" port))

(defn run [db-path ephemeral port addr threads password]
  (let [inet          (InetAddress/getByName addr)
        server-socket (ServerSocket. port 10000 inet)
        pool          (thread-pool/init threads)
        loading       (and db-path (file/exist? db-path))]
    (if loading (report-loading-start))
    (let [dba (if ephemeral
                (if loading
                  (embedded/load-ephemeral db-path)
                  (embedded/init-ephemeral))
                (if loading
                  (embedded/load-persistent db-path)
                  (embedded/init-persistent db-path)))]
      (if loading (report-loading-end))
      (report-ready port)
      (loop []
        (let [socket (doto (.accept server-socket))]
          (thread-pool/submit pool #(handler dba socket password)))
        (recur)))))

(defn- print-help []
  (println "FleetDB Server                                             ")
  (println "-f <path>   Path to database log file                      ")
  (println "-e          Ephemeral: do not log changes to disk          ")
  (println "-p <port>   TCP port to listen on (default: 3400)          ")
  (println "-a <addr>   Local address to listen on (default: 127.0.0.1)")
  (println "-t <num>    Maximum number of worker threads (default: 100)")
  (println "-v          Print the FleetDB version and exit             ")
  (println "-h          Print this help and exit.                      "))

(defn- parse-int [s]
  (and s (Integer/decode s)))

(defn -main [& args]
  (let [args-array (into-array String args)
        opt-parser (OptionParser. "f:ep:a:t:x:vh")
        opt-set    (.parse opt-parser args-array)]
    (cond
      (.has opt-set "h")
        (print-help)
      (.has opt-set "v")
        (println fleetdb/version)
      (not (or (.has opt-set "f") (.has opt-set "e")))
        (println "You must specify either -e or -f <path>. Use -h for help.")
      :else
        (let [db-path   (.valueOf opt-set "f")
              ephemeral (.has opt-set "e")
              port      (or (parse-int (.valueOf opt-set "p")) 3400)
              addr      (or (.valueOf opt-set "a") "127.0.0.1")
              threads   (or (parse-int (.valueOf opt-set "t")) 100)
              password  (.valueOf opt-set "x")]
          (run db-path ephemeral port addr threads password)))))
