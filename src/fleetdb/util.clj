(ns fleetdb.util
  (use (clojure.contrib def)))

(defn and? [coll]
  (cond
    (empty? coll) true
    (first coll)  (recur (next coll))
    :else         false))

(defn or? [coll]
  (cond
    (empty? coll) false
    (first coll)  true
    :else         (recur (next coll))))

(def != (complement =))

(defn raise [msg]
  (throw (Exception. msg)))

(defalias def- defvar-)

(defmacro defmulti-
  [name & decls]
  (list* `defmulti (vary-meta name assoc :private true) decls))

(defn merge-compact [m1 m2]
  (reduce
    (fn [m-int [k v]]
      (if (nil? v) (dissoc m-int k) (assoc m-int k v)))
    m1 m2))

(defn update [m k f & args]
  (assoc m k (apply f (get m k) args)))

(defn union-stream
  ([colls]
   (union-stream (apply concat colls) #{}))
  ([unstreamed streamed]
   (lazy-seq
     (let [pruned (drop-while streamed unstreamed)]
       (if-let [e (first pruned)]
         (cons e (union-stream (rest pruned) (conj streamed e))))))))

(defn vec-pad [v n e]
  (let [d (- n (count v))]
    (if (zero? d) v (apply conj v (repeat d e)))))

(defn high [comp coll]
  (if (seq coll)
    (reduce
      (fn [h e] (if (> 0 (comp h e)) e h))
      coll)))
