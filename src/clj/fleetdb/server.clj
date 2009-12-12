(ns fleetdb.server
  (:use     (fleetdb util))
  (:require (fleetdb [embedded :as embedded] [io :as io] [exec :as exec])
            (clj-stacktrace [repl :as stacktrace])
            (clojure.contrib [command-line :as cli]))
  (:import  (java.net ServerSocket Socket InetAddress)
            (java.io PushbackReader BufferedReader InputStreamReader
                     PrintWriter    BufferedWriter OutputStreamWriter
                     DataInputStream  BufferedInputStream
                     DataOutputStream BufferedOutputStream)))

(defmacro- safe [& body]
  `(try ~@body (catch Exception e# e#)))

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
  (safe
    (or (embedded-query dba q)
        (core-query dba q))))

(defn- text-read-query [#^PushbackReader in eof-val]
  (safe
    (binding [*read-eval* false]
      (read in false eof-val))))

(defn- text-write-exception [#^PrintWriter out e]
  (if (raised? e)
    (.println out (str e))
    (stacktrace/pst-on out false e))
  (.println out)
  (.flush out))

(defn- text-write-result [#^PrintWriter out result]
  (.println out (prn-str result))
  (.flush out))

(defn- text-handler [dba #^Socket socket]
  (try
    (with-open [socket socket
                out    (PrintWriter.    (BufferedWriter. (OutputStreamWriter. (.getOutputStream socket))))
                in     (PushbackReader. (BufferedReader. (InputStreamReader.  (.getInputStream  socket))))]
      (.setKeepAlive socket true)
      (loop []
        (let [read-result (text-read-query in io/eof)]
          (when-not (identical? io/eof read-result)
            (if (instance? Exception read-result)
              (text-write-exception out read-result)
              (let [query-result (process-query dba read-result)]
                (if (instance? Exception query-result)
                  (text-write-exception out query-result)
                  (text-write-result out query-result))))
            (recur)))))
    (catch Exception e
      (stacktrace/pst-on System/err false e)
      (.println System/err))))

(defn- binary-read-query [#^DataInputStream in eof-val]
  (safe
    (io/dis-read in eof-val)))

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
              (let [query-result (process-query dba read-result)]
                (if (instance? Exception query-result)
                  (binary-write-exception out query-result)
                  (binary-write-result out query-result))))
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
  [[f  "Full path to database log."]
   [e? "Ephemeral: do not log database changes to disk." false]
   [p  "TCP port number to listen on." "3400"]
   [a  "Local address to listen on." "127.0.0.1"]
   [t  "Maximum number of worker threads." "100"]
   [i  "Client/server protocol: one of binary or text." "binary"]]
  (run f e? (Integer/decode p) a (Integer/decode t) (keyword i)))
