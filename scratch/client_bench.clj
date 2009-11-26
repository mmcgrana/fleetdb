(set! *warn-on-reflection* true)

(require '(fleetdb [client :as client] [exec :as exec]))

(def n 200000)
(def c 10)
(def t 100)

(defn hit [k]
  (let [client (client/connect "localhost" 4444)]
    (dotimes [_ k]
      (client/query client [:select {:where [:= :id 1]}]))))

(time (hit n))

(time
  (let [executor (exec/init-pool c)]
    (dotimes [_ c]
      (exec/submit executor #(hit (/ n c))))
    (assert (exec/join executor t))))
