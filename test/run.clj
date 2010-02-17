(use 'clj-unit.core)
(use '(fleetdb [util :only (spawn)]))

(def base-tests
  ['fleetdb.compare-test
   'fleetdb.core-test
   'fleetdb.embedded-test])

(def server-tests
  ['fleetdb.server-test])

(defn- exec [& commands]
  (.exec (Runtime/getRuntime) #^"[Ljava.lang.String;" (into-array commands)))

(defn boot-servers []
  (println "Booting servers...")
  (let [procs
          [(exec "java""-cp" "fleetdb-standalone.jar" "fleetdb.server" "-e" "-p" "3400")
           (exec "java""-cp" "fleetdb-standalone.jar" "fleetdb.server" "-e" "-p" "3401" "-x" "pass")]]
    (Thread/sleep 2000)
    procs))

(defn kill-servers [procs]
  (doseq [#^Process proc procs]
    (.destroy proc)))

(condp = (first *command-line-args*)
  "--no-server"
    (apply require-and-run-tests base-tests)
  nil
    (let [threads (boot-servers)]
      (try
        (apply require-and-run-tests (concat base-tests server-tests))
        (finally
          (kill-servers threads)))))
