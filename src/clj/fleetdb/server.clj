(ns fleetdb.server
  (:use     (fleetdb [util :only (def-)]))
  (:require (fleetdb [embedded :as embedded] [io :as io] [exec :as exec])
            (clj-stacktrace [repl :as stacktrace])
            (clojure.contrib [command-line :as cli]))
  (:import  (java.net ServerSocket Socket InetAddress)
            (java.io PushbackReader BufferedReader InputStreamReader
                     PrintWriter    BufferedWriter OutputStreamWriter
                     DataInputStream  BufferedInputStream
                     DataOutputStream BufferedOutputStream)))

(defn- safe-command [dba command]
  (try
    (let [[c-type c-opts] command]
      (cond
        (= c-type :ping)
          "pong"
        (= c-type :compact)
          (embedded/compact dba)
        (= c-type :snapshot)
          (embedded/snapshot dba (:path c-opts))
        :query
          (let [result (embedded/query dba command)]
            (if (sequential? result) (vec result) result))))
      (catch Exception e e)))

(defn- text-read-command [#^PushbackReader in eof-val]
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
        (let [read-result (text-read-command in io/eof)]
          (when-not (identical? io/eof read-result)
            (if (instance? Exception read-result)
              (text-write-exception out read-result)
              (let [command-result (safe-command dba read-result)]
                (if (instance? Exception command-result)
                  (text-write-exception out command-result)
                  (text-write-result out command-result))))
            (recur)))))
    (catch Exception e
      (stacktrace/pst-on System/err false e)
      (.println System/err))))

(defn- binary-read-command [#^DataInputStream in eof-val]
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
        (let [read-result (binary-read-command in io/eof)]
          (when-not (identical? io/eof read-result)
            (if (instance? Exception read-result)
              (binary-write-exception out read-result)
              (let [command-result (safe-command dba read-result)]
                (if (instance? Exception command-result)
                  (binary-write-exception out command-result)
                  (binary-write-result out command-result))))
            (recur)))))
    (catch Exception e
      (stacktrace/pst-on System/err false e)
      (.println System/err))))

(def- protocol-handlers
  {:binary binary-handler :text text-handler})

(defn run [db-path ephemeral port interface threads protocol]
  (let [inet          (InetAddress/getByName interface)
        server-socket (ServerSocket. port 10000 inet)
        pool          (exec/init-pool threads)
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
        (exec/submit pool #(handler dba socket)))
      (recur))))

(cli/with-command-line *command-line-args*
  "FleetDB Server"
  [[l  "Full path to database log"]
   [e? "Do not log database changes to disk" false]
   [p  "TCP port number to listen on" "3400"]
   [a  "Local address to listen on" "127.0.0.1"]
   [c  "Maximum number of threads/concurrent connections" "100"]
   [i  "Client/server interface: one of binary or text" "binary"]]
  (run l e? (Integer/decode p) a (Integer/decode c) (keyword i)))
