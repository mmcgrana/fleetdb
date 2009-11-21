(set! *warn-on-reflection* true)

(import
  '(fleetdb Serializer)
  '(java.io ByteArrayOutputStream DataOutputStream
            ByteArrayInputStream  DataInputStream))

(defn serialize [obj]
  (let [baos (ByteArrayOutputStream.)
        dos  (DataOutputStream. baos)]
    (Serializer/serialize dos obj)
    (.toByteArray baos)))

(defn deserialize [bytes eof]
  (let [bais  (ByteArrayInputStream. bytes)
        dis   (DataInputStream. bais)]
    (Serializer/deserialize dis eof)))

(defn wall-time [f]
  (let [start  (System/nanoTime)
        result (f)
        end    (System/nanoTime)]
    (/ (- end start) 1000000000.0)))

(defn bench [label f]
  (f)
  (println label (wall-time f)))

(def object
  {:id 1000 :created_at 1001 :updated_at 1002
   :author_id 1003 :version 1005 :posted_at 1006
   :title "3 is the best number ever"
   :slug "3-is-the-best-number-ever"
   :body "3 is the best number ever. I say so so its true. That is all."})

(let [obj   object
      eof   (Object.)
      n     1000000
      bytes (serialize obj)]
  (println "n =" n)
  (println)
  (bench "serialize:  "
    #(dotimes [_ n] (serialize obj)))
  (bench "deserialize:"
    #(dotimes [_ n] (deserialize bytes eof))))