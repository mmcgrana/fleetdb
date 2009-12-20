(ns fleetdb.server
  (:use     (fleetdb util))
  (:require (fleetdb [embedded :as embedded] [io :as io] [file :as file]
                     [thread-pool :as thread-pool])
            (clj-stacktrace [repl :as stacktrace]))
  (:import  (java.net ServerSocket Socket InetAddress)
            (java.io PushbackReader BufferedReader InputStreamReader
                     PrintWriter    BufferedWriter OutputStreamWriter
                     DataInputStream  BufferedInputStream
                     DataOutputStream BufferedOutputStream)
            (joptsimple OptionParser OptionSet OptionException)))

(defn- embedded-query [dba q]
  (let [[q-type q-opts] q]
    (condp = q-type
      :ping     "pong"
      :compact  (embedded/compact dba)
      :snapshot (embedded/snapshot dba (:path q-opts))
      nil)))

(defn- core-query [dba q]
  (let [result (embedded/query dba q)]
    (if (sequential? result) (vec result) result)))

(defn- process-query [dba q]
  (or (embedded-query dba q)
      (core-query dba q)))

(defn- text-handler [dba #^Socket socket]
  (try
    (with-open [socket socket
                out    (PrintWriter.    (BufferedWriter. (OutputStreamWriter. (.getOutputStream socket))))
                in     (PushbackReader. (BufferedReader. (InputStreamReader.  (.getInputStream  socket))))]
      (.setKeepAlive socket true)
      (loop []
        (try
          (let [query  (read in false io/eof)]
            (if-not (identical? query io/eof)
              (let [result (process-query dba query)]
                (.println out (prn-str result))
                (.flush out))))
          (catch Exception e
            (if (raised? e)
              (.println out (str e))
              (stacktrace/pst-on out false e))
            (.println out)
            (.flush out)))
        (recur)))
    (catch Exception e
      (stacktrace/pst-on System/err false e)
      (.println System/err))))

(defn- binary-handler [dba #^Socket socket]
  (try
    (with-open [socket socket
                out    (DataOutputStream. (BufferedOutputStream. (.getOutputStream socket)))
                in     (DataInputStream.  (BufferedInputStream.  (.getInputStream  socket)))]
      (.setKeepAlive socket true)
      (loop []
        (try
          (let [query  (io/dis-read in io/eof)]
            (if-not (identical? query io/eof)
              (let [result (process-query dba query)]
                (io/dos-write out [0 result]))))
          (catch Exception e
            (io/dos-write out
              [1 (if (raised? e) (str e) (stacktrace/pst-str e))])))
        (recur)))
    (catch Exception e
      (stacktrace/pst-on System/err false e)
      (.println System/err))))

(def- protocol-handlers
  {:text text-handler :binary binary-handler })

(defn run [db-path ephemeral port addr threads protocol]
  (let [inet          (InetAddress/getByName addr)
        server-socket (ServerSocket. port 10000 inet)
        pool          (thread-pool/init threads)
        handler       (protocol-handlers protocol)
        loading       (and db-path (file/exist? db-path))
        dba           (if ephemeral
                        (if loading
                          (embedded/load-ephemeral db-path)
                          (embedded/init-ephemeral))
                        (if loading
                          (embedded/load-persistent db-path)
                          (embedded/init-persistent db-path)))]
    (println "FleetDB serving" (name protocol) "protocol on port" port)
    (loop []
      (let [socket (doto (.accept server-socket))]
        (thread-pool/submit pool #(handler dba socket)))
      (recur))))

(defn- print-help []
  (println "FleetDB Server                                                             ")
  (println "-f <path>   Path to database log file                                      ")
  (println "-e          Ephemeral: do not log changes to disk                          ")
  (println "-p <port>   TCP port to listen on (default: 3400)                          ")
  (println "-a <addr>   Local address to listen on (default: 127.0.0.1)                ")
  (println "-t <num>    Maximum number of worker threads (default: 100)                ")
  (println "-i <name>   Client/server protocol: one of {binary, text} (default: binary)")
  (println "-h          Print this help and exit.                                      "))

(defn- parse-int [s]
  (and s (Integer/decode s)))

(let [opt-parser (OptionParser. "f:ep:a:t:i:h")
      arg-array  (into-array String *command-line-args*)
      opt-set    (.parse opt-parser arg-array)]
  (cond
    (not-any? #(.has opt-set #^String %) ["f" "e" "p" "a" "t" "i" "h"])
      (print-help)
    (.has opt-set "h")
      (print-help)
    (not (or (.has opt-set "f") (.has opt-set "e")))
      (println "You must specify either -e or -f <path>. Use -h for help.")
    :else
      (let [db-path   (.valueOf opt-set "f")
            ephemeral (.has opt-set "e")
            port      (or (parse-int (.valueOf opt-set "p")) 3400)
            addr      (or (.valueOf opt-set "a") "127.0.0.1")
            threads   (or (parse-int (.valueOf opt-set "t")) 100)
            protocol  (or (keyword (or (.valueOf opt-set "i") "binary")))]
        (run db-path ephemeral port addr threads protocol))))
