(set! *warn-on-reflection* true)

(use '(fleetdb serializer))

(import '(fleetdb Serializer))

(defn wall-time [f]
  (let [start  (System/nanoTime)
        result (f)
        end    (System/nanoTime)]
    (/ (- end start) 1000000000.0)))

(defn bench [label f]
  (f)
  (println label (wall-time f)))

(def int-object
  {:id 1000 :created_at 1001 :updated_at 1002
   :author_id 1003 :version 1005 :posted_at 1006
   :title "3 is the best number ever"
   :slug "3-is-the-best-number-ever"
   :body "3 is the best number ever. I say so so its true. That is all."})

(def bigint-object
  (assoc int-object
    :id        33159784908718306650975755317368237673
    :author_id 5787935876087761561957338577287896768))

(def n 1000000)

(println "n =" n)
(println)

(let [int-obj      int-object
      bigint-obj   bigint-object
      int-bytes    (serialize int-obj)
      bigint-bytes (serialize bigint-obj)]

  (bench "serialize int object:     "
    #(dotimes [_ n] (serialize int-obj)))
  (bench "deserialize int object:   "
    #(dotimes [_ n] (deserialize int-bytes eof)))

  (bench "serialize bigint object:  "
    #(dotimes [_ n] (serialize bigint-obj)))
  (bench "deserialize bigint object:"
    #(dotimes [_ n] (deserialize bigint-bytes eof))))