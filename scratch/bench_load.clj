(set! *warn-on-reflection* true)

(use '(fleetdb core serializer)
     '(clojure.contrib shell-out))

(defn nano-time []
  (System/nanoTime))

(defn timed [f]
  (f)
  (let [t-start (nano-time)
        res     (f)
        t-end   (nano-time)]
   (double (/ (- t-end t-start) 1000000000))))

(defn records [n]
  (for [x (range n)]
    {:id         (+ x 10000000)
     :created_at (+ x 20000000)
     :updated_at (+ x 30000000)
     :title      (str x "is the best number ever")}))

(def log-path "/Users/mmcgrana/Desktop/load.out")

(defn rm-log []
  (sh "rm" "-f" log-path))

(def n 10000)
(def k 100)

(def db-empty (init))

(def db-empty-indexed
  (first (query db-empty [:create-index {:on [[:created-at :asc]]}])))

(defn dis-worker [dis eof-val]
  #(dis-read dis eof-val))

(defn dis-seq [dis eof-val]
  (lazy-seq
    (let [elem (dis-read dis eof-val)]
      (if-not (identical? elem eof-val)
        (cons elem (dis-seq dis eof-val))))))

(rm-log)

(println "n =" n)
(println "k =" k)
(println)

(let [dos (dos-init log-path)]
  (doseq [r-seq (partition k (records n))]
    (dos-write dos [:insert {:records (vec r-seq)}])))

(defn bench-load [label db-base]
  (println label
    (timed
      #(let [dis (dis-init log-path)]
         (loop [db-int db-base]
           (let [q (dis-read dis eof)]
             (if-not (identical? eof q)
               (recur (first (query db-int q)))
               db-int)))))))

(defn bench-seq-load [label db-base]
  (println label
    (timed
      #(reduce
         (fn [db-int q] (first (query db-int q)))
         db-base
         (dis-seq (dis-init log-path) eof)))))

(defn bench-seque-load [label db-base]
  (println label
    (timed
      #(reduce
         (fn [db-int q] (first (query db-int q)))
         db-base
         (seque (dis-seq (dis-init log-path) eof))))))

(defn bench-worker-load [label db-base]
  (println label
    (timed
      #(let [worker (dis-worker (dis-init log-path) eof)]
         (loop [db-int db-base]
           (let [q (worker)]
             (if-not (identical? eof q)
               (recur (first (query db-int q)))
               db-int)))))))

(bench-load        "unindexed chunked simple load: " db-empty)
(bench-load        "indexed chunked simple load:   " db-empty-indexed)

(bench-seq-load    "unindexed chunked seq'ed load: " db-empty)
(bench-seq-load    "indexed chunked seq'ed load:   " db-empty-indexed)

(bench-seque-load  "unindexed chunked seque'd load:" db-empty)
(bench-seque-load    "indexed chunked seque'd load:  " db-empty-indexed)

(bench-worker-load "unindexed chunked worker load: " db-empty)
(bench-worker-load "indexed chunked worker load:   " db-empty-indexed)

(shutdown-agents)
(rm-log)
