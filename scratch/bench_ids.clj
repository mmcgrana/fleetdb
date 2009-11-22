(set! *warn-on-reflection* true)

(import '(java.util UUID))

(defn nano-time []
  (System/nanoTime))

(defn timed [f]
  (f)
  (let [t-start (nano-time)
        res     (f)
        t-end   (nano-time)]
   (double (/ (- t-end t-start) 1000000000))))

(def n 1000000)

(defn uuids [n]
  (for [_ (range n)]
    (UUID/randomUUID)))

(println "n =" n)
(println)

(println "gen random UUIDs:"
  (timed
    #(dorun (uuids n))))
