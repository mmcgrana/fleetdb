(ns fleetdb.util
  (:require (clojure.contrib [def :as def]))
  (:require (clj-stacktrace [repl :as stacktrace]))
  (:import  (fleetdb FleetDBException)))

(def/defalias def- def/defvar-)
(def/defalias defmacro- def/defmacro-)

(defmacro defmulti-
  [name & decls]
  (list* `defmulti (vary-meta name assoc :private true) decls))

(defmacro cond! [& clauses]
  `(cond
     ~@clauses
     :else
       (throw (Exception. "No cond matches."))))

(defmacro condv [val-form & clauses]
  (let [val-sym (gensym "v")]
    `(let [~val-sym ~val-form]
        (cond
          ~@(apply concat
              (map (fn [[pred-form then-form]]
                     [(list pred-form val-sym) then-form])
                   (partition 2 clauses)))
          :else
            (throw (IllegalArgumentException.
                     (str "No matching clause: " ~val-sym)))))))

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

(defn boolean? [x]
  (instance? Boolean x))

(defn raise [& msg-elems]
  (throw (FleetDBException. (apply str msg-elems))))

(defmacro rassert [test & msg-elems]
  `(if-not ~test (raise ~@msg-elems)))

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

(defn domap [f coll]
  (dorun (map f coll)))

(defn vec-map [f coll]
  (vec (map f coll)))

(defn high [comp coll]
  (if (seq coll)
    (reduce
      (fn [h e] (if (> 0 (comp h e)) e h))
      coll)))

(defn mash [f m]
  (reduce (fn [int-m [k v]] (assoc int-m k (f k v))) {} m))

(defmacro spawn [& body]
  `(doto (Thread.
           (fn []
             (try
               ~@body
               (catch Exception e#
                 (stacktrace/pst-on System/err false e#)
                 (.println System/err)))))
        (.start)))
