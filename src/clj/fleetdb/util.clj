(ns fleetdb.util
  (:require (clojure.contrib [def :as def])))

(def/defalias def- def/defvar-)
(def/defalias defmacro- def/defmacro-)

(defmacro defmulti-
  [name & decls]
  (list* `defmulti (vary-meta name assoc :private true) decls))

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

(defn ? [val]
  (if val true false))

(defn- raise-excp [#^String msg]
  (proxy [Exception] [msg]
    (toString [] msg)))

(def- raise-excp-class
  (class (raise-excp "")))

(defn raise [& msg-elems]
  (throw (raise-excp (apply str msg-elems))))

(defn raised? [e]
  (= (class e) raise-excp-class))

(defn update [m k f & args]
  (assoc m k (apply f (get m k) args)))

(defn merge-compact [m1 m2]
  (reduce
    (fn [m-int [k v]]
      (if (nil? v) (dissoc m-int k) (assoc m-int k v)))
    m1 m2))

(defn uniq [coll]
  (lazy-seq
    (when-let [s (seq coll)]
      (let [f (first s) r (rest s)]
        (cons f (uniq (drop-while #(= f %) r)))))))

(defn compact [coll]
  (filter #(not (nil? %)) coll))

(defn vec-pad [v n e]
  (let [d (- n (count v))]
    (if (zero? d) v (apply conj v (repeat d e)))))

(defn vec-map [f coll]
  (into [] (map f coll)))

(defn high [comp coll]
  (if (seq coll)
    (reduce
      (fn [h e] (if (> 0 (comp h e)) e h))
      coll)))

(defn mash [f m]
  (reduce (fn [int-m [k v]] (assoc int-m k (f k v))) {} m))

(defmacro spawn [& body]
  `(doto (Thread. (fn [] ~@body)) (.start)))
