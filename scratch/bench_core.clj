(set! *warn-on-reflection* true)

(use '(fleetdb core))

(import '(com.yourkit.api Controller ProfilingModes))

(defn nano-time []
  (System/nanoTime))

(defn timed [f]
  (f)
  (let [t-start (nano-time)
        res     (f)
        t-end   (nano-time)]
   [res (double (/ (- t-end t-start) 1000000000))]))

(defn profile [body]
  (let [profiler (Controller.)]
   (body)
    (.startCPUProfiling profiler
      ProfilingModes/CPU_TRACING
      Controller/DEFAULT_FILTERS
      Controller/DEFAULT_WALLTIME_SPEC)
    (body)
    (.captureSnapshot profiler ProfilingModes/SNAPSHOT_WITHOUT_HEAP)
    (.stopCPUProfiling profiler)))

(defn records [n]
  (for [x (range n)]
    {:id         (+ x 10000000)
     :created-at (+ x 20000000)
     :updated-at (+ x 30000000)
     :title      (str x "is the best number ever")}))

(defn record-ids [n]
  (map #(+ 10000000 %) (range n)))

(defn bigint-records [n]
  (for [x (range n)]
    {:id         (+ x 5787935876087761561957338577287896768)
     :created-at (+ x 20000000)
     :updated-at (+ x 30000000)
     :title      (str x "is the best number ever")}))

(def n 1000000)
(def k 100)

(defn build-time [n record-seqs prof & [db-base]]
  (let [db-base    (or db-base (init))
        build-fn  #(reduce
                     (fn [db-int record-seq]
                       (first (query db-int [:insert {:records record-seq}])))
                     db-base
                     record-seqs)]
    (if prof
      (do (profile build-fn) :prof)
      (let [[built t] (timed build-fn)]
        (assert (= n (query built [:count])))
        t))))

(println "-- core")
(println "n =" n)
(println "k =" k)

(println "records:              "
  (second (timed #(dorun (records n)))))

(println "bigint records:       "
  (second (timed #(dorun (bigint-records n)))))

(println "bulk build:           "
  (build-time n (list (records n)) false))

(println "bulk bigint build:    "
  (build-time n (list (bigint-records n)) false))

(println "chuncked build:       "
  (build-time n (partition k (records n)) false))

(println "1-by-1 build:         "
  (build-time n (partition 1 (records n)) false))

(println "offline index:        "
  (second (timed
    #(query
       (first (query (init) [:insert {:records (records n)}]))
       [:create-index {:on [[:created-at :asc]]}]))))

(println "offline bigint build: "
  (second (timed
    #(query
       (first (query (init) [:insert {:records (bigint-records n)}]))
       [:create-index {:on [[:created-at :asc]]}]))))

(let [indexed (first (query (init) [:create-index {:on [[:created-at :asc]]}]))]
  (println "online bulk index:    "
    (build-time n (list (records n)) false indexed))

   (println "online chuncked index:"
     (build-time n (partition k (records n)) false indexed))

  (println "online 1-by-1 index:  "
    (build-time n (partition 1 (records n)) false indexed)))

(let [inserted (first (query (init) [:insert {:records (records n)}]))
      built    (first (query inserted [:create-index {:on [[:created-at :asc]]}]))]
  (println "get sequential:       "
    (second (timed
      #(doseq [id (record-ids n)]
         (doall (query built [:select {:where [:= :id id]}]))))))

  (println "get roundrobin:       "
    (second (timed
      #(doseq [id (take n (cycle (iterate (partial + 100) 0)))]
         (doall (query built [:select {:where [:= :id id]}]))))))

  (println "multiget sequential:  "
    (second (timed
      #(doseq [id (record-ids n)]
         (doall (query built [:select {:where [:in :id (take 10 (iterate inc id))]}]))))))

  (println "multiget roundrobin:  "
    (second (timed
      #(doseq [id (record-ids n)]
         (doall (query built [:select {:where [:in :id (take 10 (iterate (partial + 100) id))]}]))))))

   (println "query at:             "
     (second (timed
       #(doseq [id (record-ids n)]
          (doall (query built [:select {:where [:= :created-at (+ 20000000 id)]}]))))))

  (println "query range:          "
    (second (timed
      #(doseq [id (record-ids n)]
         (doall (query built [:select {:where [:>=<= :created-at [(+ 20000000 id) (+ 20000000 id 10)]]}])))))))
