(ns fleetdb.executor
  (:use (fleetdb shared util)))

(defn- )
(defn- where-pred [[op & wrest]]
  (cond
    (conj-op? op)
      (let [subpreds (map #(where-pred %) wrest)
            conj-op-fn  (conj-op-fns op)]
        (fn [record]
          (conj-op-fn (map #(% record) subpreds))))
    (sing-op? op)
      (let [[attr aval] wrest
            sing-op-fn  (sing-op-fns op)]
        (fn [record]
          (sing-op-fn (attr record) aval)))
    (doub-op? op)
      (let [[attr [aval1 aval2]] wrest
            [doub-op-fn1 doub-op-fn2]      (doub-op-fns op)]
        (fn [record]
          (let [record-aval (attr record)]
            (and (doub-op-fn1 record-aval aval1)
                 (doub-op-fn2 record-aval aval2)))))
    (= op :in)
      (let [[attr aval-vec] wrest
            aval-set        (set aval-vec)]
        (fn [record]
          (contains? aval-set (attr record))))
    :else
      (raise (str "where op " op " not recognized"))))

(defmulti- exec-plan (fn [db plan] (nth plan 0)))

(defmethod exec-plan :filter [db [_ where source]]
  (filter (where-pred where) (exec-plan db source)))

(defmethod exec-plan :sort [db [_ order source]]
  (sort (order-compare order) (exec-plan db source)))

(defmethod exec-plan :offset [db [_ offset source]]
  (drop offset (exec-plan db source)))

(defmethod exec-plan :limit [db [_ limit source]]
  (take limit (exec-plan db source)))

(defmethod exec-plan :only [db [_ only source]]
  (map #(select-keys % only) (exec-plan db source)))

(defmethod exec-plan :union [db [_ order sources]]
  (uniq
    (sort (order-compare order)
      (apply concat (map #(exec-plan db %) sources)))))

(defn- index-flatten1 [indexed]
  (cond (nil? indexed) nil
        (set? indexed) indexed
        :single        (list indexed)))

(defn- index-flatten [indexeds]
  (lazy-seq
    (when-let [iseq (seq indexeds)]
      (let [f (first iseq) r (rest iseq)]
        (cond
          (nil? f) r
          (set? f) (concat f r)
          :single  (cons f r))))))

(defmethod exec-plan :rmap-lookup [db [_ id _]]
  (get-in db [:rmap id]))

(defmethod exec-plan :rmap-multilookup [db [_ ids _]]
  (let [rmap (:rmap db)]
    (compact (map #(get rmap %) ids))))

(defmethod exec-plan :rmap-scan [db _]
  (vals (:rmap db)))

(defmethod exec-plan :index-lookup [db [_ [ispec val]]]
  (index-flatten1 (get-in db [:imap ispec val])))

(defmethod exec-plan :index-seq
  [db [_ [ispec sdir left-val left-inc right-val right-inc]]]
    (let [index    (get-in db [:imap ispec])
          indexeds
      (if (= sdir :left-right)
        (let [base    (.seqFrom index left-val true)
              base-l  (if (or left-inc (!= (key (first base)) left-val))
                        base
                        (rest base))
              base-lr (if right-inc
                        (take-while #(<= 0 (compare* (key %) right-val)) base-l)
                        (take-while #(<  0 (compare* (key %) right-val)) base-l))]
          (vals base-lr))
        (let [base    (.seqFrom index right-val false)
              base-r  (if (or right-inc (!= (key (first base)) right-val))
                        base
                        (rest base))
              base-rl (if left-inc
                        (take-while #(>= 0 (compare* (key %) left-val)) base-r)
                        (take-while #(>  0 (compare* (key %) left-val)) base-r))]
          (vals base-rl)))]
      (index-flatten indexeds)))
