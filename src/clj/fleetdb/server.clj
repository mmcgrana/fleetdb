(ns fleetdb.server
  (:use     (fleetdb util))
  (:require (fleetdb [embedded :as embedded] [io :as io]
                     [thread-pool :as thread-pool])
            (clj-stacktrace [repl :as stacktrace])
            (clojure.contrib [command-line :as cli]))
  (:import  (java.net ServerSocket Socket InetAddress)
            (java.io PushbackReader BufferedReader InputStreamReader
                     PrintWriter    BufferedWriter OutputStreamWriter
                     DataInputStream  BufferedInputStream
                     DataOutputStream BufferedOutputStream)))

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
            (io/dos-write out [1 (stacktrace/pst-str e)])))
        (recur)))
    (catch Exception e
      (stacktrace/pst-on System/err false e)
      (.println System/err))))

(def- protocol-handlers
  {:binary binary-handler :text text-handler})

(defn run [db-path ephemeral port interface threads protocol]
  (let [inet          (InetAddress/getByName interface)
        server-socket (ServerSocket. port 10000 inet)
        pool          (thread-pool/init threads)
        handler       (protocol-handlers protocol)
        loading       (and db-path (io/exist? db-path))
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

(cli/with-command-line *command-line-args*
  "FleetDB Server"
  [[f  "Full path to database log."]
   [e? "Ephemeral: do not log database changes to disk." false]
   [p  "TCP port number to listen on." "3400"]
   [a  "Local address to listen on." "127.0.0.1"]
   [t  "Maximum number of worker threads." "100"]
   [i  "Client/server protocol: one of binary or text." "binary"]]
  (run f e? (Integer/decode p) a (Integer/decode t) (keyword i)))
