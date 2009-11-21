(set! *warn-on-reflection* true)

(defn records [n]
  (for [x (range n)]
    {:id         (+ x 10000000)
     :created_at (+ x 20000000)
     :updated_at (+ x 30000000)
     :title      (str x "is the best number ever")}))

(defn query-ids [n]
  (take n (cycle (range 0 n 100))))

(defn build-seq [n]
  (doall (records n)))

(defn build-hashmap [n]
  (let [hashmap (java.util.HashMap.)]
    (doseq [r (records n)]
      (.put hashmap (:id r) r))
    hashmap))

(defn build-treemap [n]
  (let [treemap (java.util.TreeMap.)]
    (doseq [r (records n)]
      (.put treemap (:id r) r))
    treemap))

(defn query-jmap [#^java.util.Map jm n]
  (doseq [id (query-ids n)]
    (.get jm id)))

(defn build-map [n]
  (reduce
    (fn [m r] (assoc m (:id r) r))
    {}
    (records n)))

(defn build-transmap [n]
  (let [tm (transient {})]
    (doseq [r (records n)]
      (assoc! tm (:id r) r))
    (persistent! tm)))

(defn build-sortedmap [n]
  (reduce
    (fn [m r] (assoc m (:id r) r))
    (sorted-map)
    (records n)))

(defn query-cmap [cm n]
  (doseq [id (query-ids n)]
    (cm id)))

(defn total-mem []
  (.. Runtime getRuntime totalMemory))

(defn free-mem []
  (.. Runtime getRuntime freeMemory))

(defn used-mem []
  (- (total-mem) (free-mem)))

(defn gc []
  (dotimes [_ 20] (System/gc)))

(defn nano-time []
  (System/nanoTime))

(defn mem-times [build query]
  (if query (query (build)) (build))
  (gc)
  (let [tb-start (nano-time)
        m-start  (used-mem)
        built    (build)
        tb-end   (nano-time)
        m-end    (used-mem)
        m        (int (/ (- m-end m-start) 1048576))
        tb       (double (/ (- tb-end tb-start) 1000000000))]
    (if-not query
      [m tb]
      (let [tq-start (nano-time)
            _        (query built)
            tq-end   (nano-time)
            tq       (double (/ (- tq-end tq-start) 1000000000))]
        [m tb tq]))))

(defn bench [label build query n]
  (println label)
  (let [[m tb tq] (mem-times #(build n) (if query #(query % n)))]
    (println "memory usage (MB):" m)
    (println "build time (sec):  " tb)
    (if query
      (println "query time (sec):  " tq))
    (prn)))

(def tests
  [["seq"       build-seq       nil]
   ["hashmap"   build-hashmap   query-jmap]
   ["treemap"   build-treemap   query-jmap]
   ["map"       build-map       query-cmap]
   ["transmap"  build-transmap  query-cmap]
   ["sortedmap" build-sortedmap query-cmap]])

(def n 2000000)

(println "n =" n)
(println)
(doseq [[label build-name query-name] tests]
  (bench label build-name query-name n))
