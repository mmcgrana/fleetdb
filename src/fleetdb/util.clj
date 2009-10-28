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

(defn merge-compact [m1 m2]
  (reduce
    (fn [m-int [k v]]
      (if (nil? v) (dissoc m-int k) (assoc m-int k v)))
    m1 m2))

(defn update [m k f & args]
  (assoc m k (apply f (get m k) args)))

(defn greatest
  ([coll]
     (greatest compare coll))
  ([compfn coll]
     (first (sort #(compfn %2 %1) coll))))

(defn least-by [keyfn coll]
  (first (sort-by keyfn coll)))