(set! *warn-on-reflection* true)

(require '(fleetdb [embedded :as embedded] [core :as core]))
(import '(com.yourkit.api Controller ProfilingModes))

(defn records [n]
  (for [x (range n)]
    {:id         (+ x 10000000)
     :created-at (+ x 20000000)
     :updated-at (+ x 30000000)
     :title      (str x "is the best number ever")}))

(defn record-ids [n]
  (map #(+ 10000000 %) (range n)))

(defn nano-time []
  (System/nanoTime))

(defn timed [f]
  (f)
  (let [t-start (nano-time)
        res     (f)
        t-end   (nano-time)]
   (double (/ (- t-end t-start) 1000000000))))

(defn profiled [body]
  (body)
  (let [profiler (Controller.)]
    (.startCPUProfiling profiler
      ProfilingModes/CPU_TRACING
      Controller/DEFAULT_FILTERS
      Controller/DEFAULT_WALLTIME_SPEC)
    (body)
    (.captureSnapshot profiler ProfilingModes/SNAPSHOT_WITHOUT_HEAP)
    (.stopCPUProfiling profiler)
    :profiled))

(def n 1000000)

(println "-- embedded")
(println "n =" n)

(println "1-by-1 build:  "
  (timed
    #(let [dba (embedded/init)]
       (doseq [r-seq (partition 1 (records n))]
         (embedded/query dba [:insert {:records r-seq}]))
       (embedded/close dba))))

(println "get sequential:"
  (let [dba (embedded/init)]
    (embedded/query dba [:insert {:records (records n)}])
    (timed
      #(do
         (doseq [id (record-ids n)]
           (doall (query dba [:select {:where [:= :id id]}])))
         (embedded/close dba)))))