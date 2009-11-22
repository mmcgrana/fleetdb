(set! *warn-on-reflection* true)

(import '(java.util UUID Random))

(defn nano-time []
  (System/nanoTime))

(defn timed [f]
  (f)
  (let [t-start (nano-time)
        res     (f)
        t-end   (nano-time)]
   (double (/ (- t-end t-start) 1000000000))))

(def n 1000000)

(println "-- ids")
(println "n =" n)

(println "gen random UUIDs:      "
  (timed
    #(dotimes [_ n] (UUID/randomUUID))))

(println "gen random BigIntegers:"
  (let [r (Random.)]
    (timed
      #(dotimes [_ n]
         (let [bytes (make-array Byte/TYPE 16)]
           (.nextBytes r bytes)
           (BigInteger. bytes))))))
