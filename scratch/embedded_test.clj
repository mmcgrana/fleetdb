(set! *warn-on-reflection* true)

(use '(fleetdb [embedded :as embedded] [exec :as exec]))

(def dba (embedded/init))

(prn (embedded/query dba [:select]))
(embedded/query dba [:insert {:records [{:id 1 :name "mark"} {:id 2 :name "matt"}]}])
(prn (embedded/query dba [:select]))
(try
  (embedded/query dba [:nonsense])
  (catch Exception e
    (println "read error:" e)))
(try
  (embedded/query dba [:insert :nonsense])
  (catch Exception e
    (println "write error:" e)))

(embedded/query dba [:insert {:records (for [n (range 1000)] {:id (+ 3 n)})}])

(def n 100000)
(time
  (dotimes [_ n]
    (dorun (embedded/query dba [:select]))))

(time
  (dotimes [_ n]
    (dorun (embedded/query dba [:select {:where [:= :id 50]}]))))

(let [pool (exec/init-pool 2)]
  (time
    (do
      (dotimes [_ n]
        (exec/submit pool #(dorun (embedded/query dba [:select]))))
      (exec/shutdown pool)
      (exec/await-termination pool 60))))

(let [pool (exec/init-pool 2)]
  (time
    (do
      (dotimes [_ n]
        (exec/submit pool #(dorun (embedded/query dba [:select {:where [:= :id 50]}]))))
      (exec/shutdown pool)
      (exec/await-termination pool 60))))

(embedded/close dba)