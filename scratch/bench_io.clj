(set! *warn-on-reflection* true)

(import
  '(com.yourkit.api Controller ProfilingModes))

(use
  '(fleetdb serializer)
  '(clojure.contrib shell-out))

(defn profile [body]
  (let [profiler (Controller.)]
   (body 1)
    (.startCPUProfiling profiler
      ProfilingModes/CPU_TRACING
      Controller/DEFAULT_FILTERS
      Controller/DEFAULT_WALLTIME_SPEC)
    (body 2)
    (.captureSnapshot profiler ProfilingModes/SNAPSHOT_WITHOUT_HEAP)
    (.stopCPUProfiling profiler)))

(defn snapshot []
  (let [profiler (Controller.)]
    (.captureMemorySnapshot profiler)))

(defn nano-time []
  (System/nanoTime))

(defn timed [f]
  (f 1)
  (let [t-start (nano-time)
        res     (f 2)
        t-end   (nano-time)]
   [res (double (/ (- t-end t-start) 1000000000))]))

(defn records [n]
  (for [x (range n)]
    {:id         (+ x 10000000)
     :created_at (+ x 20000000)
     :updated_at (+ x 30000000)
     :title      (str x "is the best number ever")}))

(def n 1000000)
(def k 100)
(defn one-path [i] (str "/Users/mmcgrana/Desktop/one." i))
(defn chk-path [i] (str "/Users/mmcgrana/Desktop/chk." i))

(defn clean-files []
  (sh "rm" "-f" (one-path 1) (one-path 2) (chk-path 1) (chk-path 2)))

(clean-files)
(println "n = " n)
(println "k = " k)
(println)

(println "write 1-by-1:   "
  (second (timed
    #(let [dos-one (dos-init (one-path %))]
       (doseq [r (records n)]
         (dos-write dos-one r))))))

(println "write chuncked: "
  (second (timed
    #(let [dos-chk (dos-init (chk-path %))]
       (doseq [r-seq (partition k (records n))]
         (dos-write dos-chk (vec r-seq)))))))

(println "read 1-by-1:    "
  (second (timed
    #(let [dis-one (dis-init (one-path %))]
       (loop [] (if (not (identical? eof (dis-read dis-one eof))) (recur)))))))

(println "read chuncked:  "
  (second (timed
    #(let [dis-chk (dis-init (chk-path %))]
       (loop [] (if (not (identical? eof (dis-read dis-chk eof))) (recur)))))))

;(profile
;  #(let [dis-chk (dis-init (chk-path %))]
;     (loop [] (if (not (identical? eof (dis-read dis-chk eof))) (recur)))))

(clean-files)