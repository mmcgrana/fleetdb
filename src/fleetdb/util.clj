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

(defn greatest [coll]
  (if (seq coll)
    (apply max coll)))

(defn least-by [key-fn coll]
  (if (seq coll)
    (reduce
      (fn [mem elem]
        (if (< (key-fn elem) (key-fn mem)) elem mem))
      coll)))
