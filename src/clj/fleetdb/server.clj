(ns fleetdb.server
  (:use     (fleetdb util))
  (:require (fleetdb [embedded :as embedded] [json :as json] [lint :as lint]
                     [file :as file] [thread-pool :as thread-pool])
            (clj-stacktrace [repl :as stacktrace])
            (clojure.contrib [str-utils :as str-utils]))
  (:import  (java.net ServerSocket Socket InetAddress)
            (java.io BufferedWriter OutputStreamWriter
                     BufferedReader InputStreamReader)
            (org.codehaus.jackson JsonParseException)
            (joptsimple OptionParser OptionSet OptionException))
  (:gen-class))

(defn- process-query [dba q]
  (lint/lint-query q)
  (condp = (first q)
    "ping"    "pong"
    "compact" (embedded/compact dba)
    (embedded/query* dba q)))

(defn- write-response [#^BufferedWriter out resp]
  (.write out #^String (json/generate-string resp))
  (.write out "\n")
  (.flush out))

(defn handler [dba #^Socket socket]
  (try
    (with-open [socket socket
                in     (BufferedReader. (InputStreamReader.  (.getInputStream socket)))
                out    (BufferedWriter. (OutputStreamWriter. (.getOutputStream socket)))]
      (.setKeepAlive socket true)
      (loop []
        (if (try
              (if-let [in-line (.readLine in)]
                (let [query  (json/parse-string in-line)
                      result (process-query dba query)]
                  (write-response out [0 result])
                  true))
              (catch Exception e
                (write-response out
                  (if (or (raised? e) (instance? JsonParseException e))
                    [1 (.getMessage e)]
                    [2 (stacktrace/pst-str e)]))
                true))
            (recur))))
    (catch Exception e
      (stacktrace/pst-on System/err false e)
      (.println System/err))))

(defn run [db-path ephemeral port addr threads]
  (let [inet          (InetAddress/getByName addr)
        server-socket (ServerSocket. port 10000 inet)
        pool          (thread-pool/init threads)
        loading       (and db-path (file/exist? db-path))
        dba           (if ephemeral
                        (if loading
                          (embedded/load-ephemeral db-path)
                          (embedded/init-ephemeral))
                        (if loading
                          (embedded/load-persistent db-path)
                          (embedded/init-persistent db-path)))]
    (printf "FleetDB listening on port %d\n" port)
    (flush)
    (loop []
      (let [socket (doto (.accept server-socket))]
        (thread-pool/submit pool #(handler dba socket)))
      (recur))))

(defn- print-help []
  (println "FleetDB Server                                             ")
  (println "-f <path>   Path to database log file                      ")
  (println "-e          Ephemeral: do not log changes to disk          ")
  (println "-p <port>   TCP port to listen on (default: 3400)          ")
  (println "-a <addr>   Local address to listen on (default: 127.0.0.1)")
  (println "-t <num>    Maximum number of worker threads (default: 100)")
  (println "-h          Print this help and exit.                      "))

(defn- parse-int [s]
  (and s (Integer/decode s)))

(defn -main [& args]
  (let [args-array (into-array String args)
        opt-parser (OptionParser. "f:ep:a:t:h")
        opt-set    (.parse opt-parser args-array)]
    (cond
      (not-any? #(.has opt-set #^String %) ["f" "e" "p" "a" "t" "h"])
        (print-help)
      (.has opt-set "h")
        (print-help)
      (not (empty? (.nonOptionArguments opt-set)))
        (do
          (printf "Unrecognized option '%s'. Use -h for help.\n"
                  (first (.nonOptionArguments opt-set)))
          (flush))
      (not (or (.has opt-set "f") (.has opt-set "e")))
        (println "You must specify either -e or -f <path>. Use -h for help.")
      :else
        (let [db-path   (.valueOf opt-set "f")
              ephemeral (.has opt-set "e")
              port      (or (parse-int (.valueOf opt-set "p")) 3400)
              addr      (or (.valueOf opt-set "a") "127.0.0.1")
              threads   (or (parse-int (.valueOf opt-set "t")) 100)]
          (run db-path ephemeral port addr threads)))))
