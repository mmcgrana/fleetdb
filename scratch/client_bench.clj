(set! *warn-on-reflection* true)

(require '(fleetdb [client :as client] [exec :as exec]))

(def n 100000)
(def c 10)
(def t 100)

(defn clear []
  (let [client (client/connect "localhost" 4444)]
    (client/query client [:delete])
    (client/close client)))

(defn hit [k o]
  (let [client (client/connect "localhost" 4444)]
    (dotimes [i k]
      (client/query client [:insert {:id  (+ o i)}]))
    (client/close client)))

(defn check []
  (let [client (client/connect "localhost" 4444)
        count  (client/query client [:count])]
    (client/close client)
    count))

(clear)
(time (hit n 0))
(prn (check))

(clear)
(time
  (let [executor (exec/init-pool c)]
    (dotimes [i c]
      (exec/submit executor #(hit (/ n c) (* i (/ n c)))))
    (assert (exec/join executor t))))
(prn (check))
