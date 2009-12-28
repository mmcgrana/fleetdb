(ns fleetdb.server
  (:use     (fleetdb util))
  (:require (fleetdb [embedded :as embedded] [io :as io] [file :as file]
                     [thread-pool :as thread-pool])
            (clj-stacktrace [repl :as stacktrace]))
  (:import  (java.net ServerSocket Socket InetAddress)
            (java.io PushbackReader BufferedReader InputStreamReader
                     PrintWriter    BufferedWriter OutputStreamWriter
                     DataInputStream  BufferedInputStream  InputStream
                     DataOutputStream BufferedOutputStream OutputStream)
            (joptsimple OptionParser OptionSet OptionException))
  (:gen-class))

(defn- embedded-query [dba q]
  (let [[q-type q-opt] q]
    (condp = q-type
      :ping     "pong"
      :compact  (embedded/compact dba)
      :snapshot (embedded/snapshot dba q-opt)
      nil)))

(defn- core-query [dba q]
  (let [result (embedded/query dba q)]
    (if (sequential? result) (vec result) result)))

(defn- process-query [dba q]
  (or (embedded-query dba q)
      (core-query dba q)))

(defn generic-handler [init-out init-in read-query write-response]
  (fn [dba #^Socket socket]
    (try
      (with-open [socket socket
                  out    (init-out (.getOutputStream socket))
                  in     (init-in  (.getInputStream socket))]
        (.setKeepAlive socket true)
        (loop []
          (if (try
                (let [query (read-query in io/eof)]
                  (if (identical? query io/eof)
                    false
                    (let [result (process-query dba query)]
                      (write-response out [0 result])
                      true)))
                (catch Exception e
                  (write-response out
                    (if (raised? e)
                      [1 (str e)]
                      [2 (stacktrace/pst-str e)]))
                  true))
              (recur))))
      (catch Exception e
        (stacktrace/pst-on System/err false e)
        (.println System/err)))))

(def- text-handler
  (generic-handler
    (fn [#^OutputStream os] (PrintWriter. (BufferedWriter. (OutputStreamWriter. os))))
    (fn [#^InputStream is] (PushbackReader. (BufferedReader. (InputStreamReader. is))))
    (fn [#^PushbackReader in eof-val] (read in false eof-val))
    (fn [#^PrintWriter out [resp-code resp-val :as resp]]
      (if (#{0 1} resp-code)
        (.print out (prn-str resp-val))
        (.print out resp-val))
      (.println out)
      (.flush out))))

(def- binary-handler
  (generic-handler
    (fn [#^OutputStream os] (DataOutputStream. (BufferedOutputStream. os)))
    (fn [#^InputStream is] (DataInputStream.  (BufferedInputStream. is)))
    (fn [#^DataInputStream in eof-val] (io/dis-deserialize in io/eof))
    (fn [#^DataOutputStream out resp]  (io/dos-serialize out resp))))

(def- bert-handler
  (generic-handler
    (fn [#^OutputStream os] (DataOutputStream. (BufferedOutputStream. os)))
    (fn [#^InputStream is] (DataInputStream.  (BufferedInputStream.  is)))
    (fn [#^DataInputStream in eof-val] (io/dis-berp-decode in io/eof))
    (fn [#^DataOutputStream out resp]  (io/dos-berp-encode out resp))))

(def- protocol-handlers
  {:text text-handler :binary binary-handler :bert bert-handler})

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
  (println "FleetDB Server                                                                 ")
  (println "-f <path>   Path to database log file                                          ")
  (println "-e          Ephemeral: do not log changes to disk                              ")
  (println "-p <port>   TCP port to listen on (default: 3400)                              ")
  (println "-a <addr>   Local address to listen on (default: 127.0.0.1)                    ")
  (println "-t <num>    Maximum number of worker threads (default: 100)                    ")
  (println "-i <name>   Client/server protocol: one of {text,binary,bert} (default: binary)")
  (println "-h          Print this help and exit.                                          "))

(defn- parse-int [s]
  (and s (Integer/decode s)))

(defn -main [& args]
  (let [args-array  (into-array String args)
        opt-parser (OptionParser. "f:ep:a:t:i:h")
        opt-set    (.parse opt-parser args-array)]
    (cond
      (not-any? #(.has opt-set #^String %) ["f" "e" "p" "a" "t" "i" "h"])
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
              threads   (or (parse-int (.valueOf opt-set "t")) 100)
              protocol  (or (keyword (or (.valueOf opt-set "i") "binary")))]
          (run db-path ephemeral port addr threads protocol)))))
