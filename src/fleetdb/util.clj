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

(def non-nil? (comp not nil?))

(defn raise [msg]
  (throw (Exception. msg)))

(defalias def- defvar-)

(defn merge-compact [m1 m2]
  (reduce
    (fn [m-int [k v]]
      (if (nil? v) (dissoc m-int k) (assoc m-int k v)))
    m1 m2))

(defmacro if-let? [[test sym exp] & body]
  `(let [~sym ~exp]
     (if (~test ~sym)
       ~@body)))
