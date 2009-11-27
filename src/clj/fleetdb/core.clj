(ns fleetdb.core
  (:import (clojure.lang Numbers Sorted) (fleetdb Compare))
  (:use (fleetdb util)))

;; General ordering

(def- neg-inf :neg-inf)
(def- pos-inf :pos-inf)

(defn- record-compare [order]
  (let [[[attr dir] & rorder] order]
    (if (not rorder)
      (cond
        (= dir :asc)
          #(Compare/compare (attr %1) (attr %2))
        (= dir :dsc)
          #(Compare/compare (attr %2) (attr %1))
        :else
          (raise ("invalid order " order)))
      (let [rcompare (record-compare rorder)]
        (cond
          (= dir :asc)
            #(let [c (Compare/compare (attr %1) (attr %2))] (if (zero? c) (rcompare %1 %2) c))
          (= dir :dsc)
            #(let [c (Compare/compare (attr %2) (attr %1))] (if (zero? c) (rcompare %1 %2) c))
          :else
            (raise "invalid order " order))))))

(defn- attr-compare [order]
  (let [[[attr dir] & rorder] order]
    (if (not rorder)
      (cond
        (= dir :asc)
          #(Compare/compare %1 %2)
        (= dir :dsc)
          #(Compare/compare %2 %1)
        :else
          (raise (str "invalid order " order)))
      (let [rcompare (attr-compare rorder)]
        (cond
          (= dir :asc)
            #(let [c (Compare/compare (first %1) (first %2))]
               (if (zero? c)
                 (rcompare (rest %1) (rest %2))
                 c))
          (= dir :dsc)
            #(let [c (Compare/compare (first %1) (first %2))]
               (if (zero? c)
                 (rcompare (rest %1) (rest %2))
                   c))
          :else
            (raise "invalid order " order))))))

;; Find planning

(defn- filter-plan [source where]
  (if where [:filter where source] source))

(defn- sort-plan [source order]
  (if order [:sort order source] source))

(defn- rmap-scan-plan [where order]
  (-> [:rmap-scan]
    (filter-plan where)
    (sort-plan order)))

(defn- index-order-prefix? [ispec order]
  (= (take (count order) ispec)
     order))

(def- flip-idir
  {:asc :dsc :dsc :asc})

(defn- flip-order [order]
  (map (fn [[attr dir]] [attr (flip-idir dir)]) order))

(defn- val-pad [left-v right-v ispec]
  (loop [left-vp left-v right-vp right-v rispec (drop (count left-v) ispec)]
    (if-let [[icattr icdir] (first rispec)]
      (if (= icdir :asc)
        (recur (conj left-vp neg-inf) (conj right-vp pos-inf) (rest rispec))
        (recur (conj left-vp pos-inf) (conj right-vp neg-inf) (rest rispec)))
      [left-vp right-vp])))

(defn- build-where-left [eq-left ineq-left other]
  (let [eq-conds   (map second (vals eq-left))
        ineq-conds (map second (vals ineq-left))
        conds      (concat eq-conds ineq-conds other)]
    (cond
      (empty? conds)      nil
      (= 1 (count conds)) (first conds)
      :multi-cond         (vec (cons :and conds)))))

(defn- conds-order-ipplan [ispec eq ineq other where order]
  (let [[rispec left-val left-inc right-val right-inc where-count where-left]
    (loop [rispec ispec left-val [] right-val [] where-count 0 eq-left eq]
      (if-not rispec
        [nil left-val true right-val true where-count (build-where-left eq-left ineq other)]
        (let [[icattr icdir] (first rispec)
              rrispec        (next rispec)]
          (if-let [[v _] (get eq-left icattr)]
            (recur rrispec (conj left-val v) (conj right-val v)
                   (inc where-count) (dissoc eq-left icattr))
            (if-let [[[low-v low-i high-v high-i] _] (get ineq icattr)]
              (let [w-left (build-where-left eq-left (dissoc ineq icattr) other)]
                (if (= icdir :asc)
                  [rrispec (conj left-val low-v)  low-i  (conj right-val high-v) high-i (inc where-count) w-left]
                  [rrispec (conj left-val high-v) high-i (conj right-val low-v)  low-i  (inc where-count) w-left]))
              [rispec left-val true right-val true where-count (build-where-left eq-left ineq other)])))))]
    (let [[order-left order-count sdir]
      (cond
        (empty? order)
          [nil 0 :left-right]
        (index-order-prefix? rispec order)
          [nil (count order) :left-right]
        (index-order-prefix? rispec (flip-order order))
          [nil (count order) :right-left]
        :else
          [order 0 :left-right])]
      (let [[left-val right-val] (val-pad left-val right-val ispec)]
        {:ispec       ispec
         :where-count where-count
         :order-count order-count
         :left-val    left-val
         :left-inc    left-inc
         :right-val   right-val
         :right-inc   right-inc
         :sdir        sdir
         :where-left  where-left
         :order-left  order-left}))))

(defn- flatten-where [where]
  (cond
    (not where)
      nil
    (= :and (first where))
      (rest where)
    :single-op
      (list where)))

(def- eq-op?
  #{:=})

(def- ineq-op?
  #{:< :<= :> :>= :>< :>=< :><= :>=<=})

(def- other-op?
  #{:!= :in :or})

(defn- cond-low-high [op v]
  (condp = op
    :<  [neg-inf true  v       false]
    :<= [neg-inf true  v       true ]
    :>  [v       false pos-inf true ]
    :>= [v       true  pos-inf true ]
    (let [[v1 v2] v]
      (condp = op
        :><   [v1 false v2 false]
        :>=<  [v1 true  v2 false]
        :><=  [v1 false v2 true ]
        :>=<= [v1 true  v2 true ]))))

(defn- partition-conds [conds]
  (reduce
    (fn [[eq ineq other] acond]
      (let [[cop cattr cval] acond]
        (cond
          (eq-op? cop)
            (if (contains? eq cattr)
              (raise (str "duplicate equality on " cattr))
              [(assoc eq cattr [cval acond]) ineq other])
          (ineq-op? cop)
            (if (contains? ineq cattr)
              (raise (str "duplicate inequality on " cattr))
              [eq (assoc ineq cattr [(cond-low-high cop cval) acond]) other])
          (other-op? cop)
            [eq ineq (conj other acond)]
          :else
            (raise (str "invalid where " acond)))))
    [{} {} []]
    conds))

(defn- where-order-ipplans [ispecs where order]
  (let [[eq ineq other] (-> where flatten-where partition-conds)]
    (map #(conds-order-ipplan % eq ineq other where order) ispecs)))

(defn- ipplan-compare [a b]
  (compare [(:where-count a) (:order-count a)] [(:where-count b) (:order-count b)]))

(defn- ipplan-useful? [ipplan]
  (pos? (+ (:where-count ipplan) (:order-count ipplan))))

(defn- ipplan-plan [{:keys [ispec sdir left-val left-inc right-val right-inc
                            where-left order-left]}]
  (let [left-val-t  (if (= (count left-val)  1) (first left-val)  left-val)
        right-val-t (if (= (count right-val) 1) (first right-val) right-val)]
    (-> (if (= left-val-t right-val-t)
          [:index-lookup [ispec left-val-t]]
          [:index-seq    [ispec sdir left-val-t left-inc right-val-t right-inc]])
      (filter-plan where-left)
      (sort-plan   order-left))))

(defn- where-order-plan* [ispecs where order]
  (let [ipplans (where-order-ipplans ispecs where order)
        ipplan  (high ipplan-compare ipplans)]
    (if (and ipplan (ipplan-useful? ipplan))
      (ipplan-plan ipplan)
      (rmap-scan-plan where order))))

(defn- where-order-plan [ispecs where order]
  (cond
    (and (= (get where 0) :=) (= (get where 1) :id))
      [:rmap-lookup (get where 2)]

    (and (= (get where 0) :in) (= (get where 1) :id))
      (-> [:rmap-multilookup (get where 2)]
        (sort-plan order))

    (= (get where 0) :or)
      [:union order (map #(where-order-plan ispecs % order) (next where))]

    :else
      (where-order-plan* ispecs where order)))

(defn- offset-plan [source offset]
  (if offset [:offset offset source] source))

(defn- limit-plan [source limit]
  (if limit [:limit limit source] source))

(defn- only-plan [source only]
  (if only [:only only source] source))

(defn find-plan [ispecs where order offset limit only]
  (-> (where-order-plan ispecs where order)
   (offset-plan offset)
   (limit-plan  limit)
   (only-plan   only)))


;; Find execution

(def- conj-op-fns
  {:and and? :or or?})

(def- sing-op-fns
  {:= = :!= != :< < :<= <= :> > :>= >=})

(def- doub-op-fns
  {:>< [> <] :>=< [>= <] :><= [> <=] :>=<= [>= <=]})

(defn- where-pred [[op & wrest]]
  (cond
    (conj-op-fns op)
      (let [subpreds (map #(where-pred %) wrest)
            conj-op-fn  (conj-op-fns op)]
        (fn [record]
          (conj-op-fn (map #(% record) subpreds))))
    (sing-op-fns op)
      (let [[attr aval] wrest
            sing-op-fn  (sing-op-fns op)]
        (fn [record]
          (sing-op-fn (attr record) aval)))
    (doub-op-fns op)
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

(defmulti- exec-plan (fn [db [plan-type _]] plan-type))

(defmethod exec-plan :filter [db [_ where source]]
  (filter (where-pred where) (exec-plan db source)))

(defmethod exec-plan :sort [db [_ order source]]
  (sort (record-compare order) (exec-plan db source)))

(defmethod exec-plan :offset [db [_ offset source]]
  (drop offset (exec-plan db source)))

(defmethod exec-plan :limit [db [_ limit source]]
  (take limit (exec-plan db source)))

(defmethod exec-plan :only [db [_ only source]]
  (map #(select-keys % only) (exec-plan db source)))

(defmethod exec-plan :union [db [_ order sources]]
  (uniq
    (sort (record-compare order)
      (apply concat (map #(exec-plan db %) sources)))))

(defmethod exec-plan :rmap-lookup [db [_ id _]]
  (let [record (get-in db [:rmap id])]
    (when record (list record))))

(defmethod exec-plan :rmap-multilookup [db [_ ids _]]
  (if-let [rmap (:rmap db)]
    (compact (map #(rmap %) ids))))

(defmethod exec-plan :rmap-scan [db _]
  (vals (:rmap db)))

(defn- indexed-flatten1 [indexed]
  (cond (nil? indexed) nil
        (set? indexed) indexed
        :single        (list indexed)))

(defn- indexed-flatten [indexeds]
  (lazy-seq
    (when-let [iseq (seq indexeds)]
      (let [f (first iseq) r (rest iseq)]
        (cond
          (nil? f) r
          (set? f) (concat f r)
          :single  (cons f r))))))

(defmethod exec-plan :index-lookup [db [_ [ispec val]]]
  (indexed-flatten1 (get-in db [:imap ispec val])))

(defmethod exec-plan :index-seq
  [db [_ [ispec sdir left-val left-inc right-val right-inc]]]
    (let [#^Sorted index (get-in db [:imap ispec])
          indexeds
      (if (= sdir :left-right)
        (let [base    (.seqFrom index left-val true)
              base-l  (if (or left-inc (!= (key (first base)) left-val))
                        base
                        (rest base))
              base-lr (if right-inc
                        (take-while #(<= (Compare/compare (key %) right-val) 0) base-l)
                        (take-while #(<  (Compare/compare (key %) right-val) 0) base-l))]
          (vals base-lr))
        (let [base    (.seqFrom index right-val false)
              base-r  (if (or right-inc (!= (key (first base)) right-val))
                        base
                        (rest base))
              base-rl (if left-inc
                        (take-while #(>= (Compare/compare (key %) left-val) 0) base-r)
                        (take-while #(>  (Compare/compare (key %) left-val) 0) base-r))]
          (vals base-rl)))]
      (indexed-flatten indexeds)))

(defn- find-records [db {:keys [where order offset limit only]}]
  (exec-plan db
    (find-plan (keys (:imap db)) where order offset limit only)))


;; RMap and IMap manipulation

(defn- rmap-insert [rmap record]
  (let [id (:id record)]
    (assert id)
    (assert (not (contains? rmap id)))
    (assoc rmap (:id record) record)))

(defn- rmap-update [rmap old-record new-record]
  (assoc rmap (:id old-record) new-record))

(defn- rmap-delete [rmap old-record]
  (dissoc rmap (:id old-record)))

(defn- ispec-on-fn [ispec]
  (let [attrs (map first ispec)]
    (cond
      (empty? attrs)      (raise (str "empty ispec: " ispec))
      (= 1 (count attrs)) (first attrs)
      :multi-attr         #(vec (map % attrs)))))

(defn- index-insert [index on-fn record]
  (update index (on-fn record)
    (fn [indexed]
      (cond
        (nil? indexed) record
        (set? indexed) (conj indexed record)
        :single-record (hash-set indexed record)))))

(defn- index-delete [index on-fn record]
  (let [aval    (on-fn record)
        indexed (aval index)]
    (update index aval
      (fn [indexed]
        (cond
          (nil? indexed) (raise "missing record")
          (set? indexed) (do
                           (assert (contains? indexed record))
                           (disj indexed record))
          :single-record (do (assert (= indexed record))
                           nil))))))

(defn- index-build [records ispec]
  (let [on-fn (ispec-on-fn ispec)]
    (reduce
      (fn [i r] (index-insert i on-fn r))
      (sorted-map-by (attr-compare ispec))
      records)))

(defn- imap-apply [imap apply-fn]
  (mash (fn [ispec index] (apply-fn (ispec-on-fn ispec) index)) imap))

(defn- imap-insert [imap record]
  (imap-apply imap
    (fn [on-fn index]
      (index-insert index on-fn record))))

(defn- imap-update [imap old-record new-record]
  (imap-apply imap
    (fn [on-fn index]
      (-> index
        (index-delete on-fn old-record)
        (index-insert on-fn new-record)))))

(defn- imap-delete [imap record]
  (imap-apply imap
    (fn [on-fn index] (index-delete index on-fn record))))


;; Query implementations

(defmulti query (fn [db [query-type opts]] query-type))

(defmethod query :default [_ [query-type _]]
  (raise (str "invalid query type: " query-type)))

(defmethod query :select [db [_ opts]]
  (find-records db opts))

(defmethod query :get [db [_ id-s]]
  (if-let [rmap (:rmap db)]
    (if (vector? id-s)
      (compact (map #(rmap %) id-s))
      (rmap id-s))))

(defmethod query :count [db [_ opts]]
  (count (find-records db opts)))

(defn- db-apply1 [{old-rmap :rmap old-imap :imap :as db} record rmap-fn imap-fn]
  (let [new-rmap (rmap-fn old-rmap record)
        new-imap (imap-fn old-imap record)]
    [(assoc db :rmap new-rmap :imap new-imap) 1]))

(defn- db-apply [{old-rmap :rmap old-imap :imap :as db} records apply-fn]
  (let [[new-rmap new-imap] (reduce apply-fn [old-rmap old-imap] records)]
    [(assoc db :rmap new-rmap :imap new-imap) (count records)]))

(defmethod query :insert [db [_ record-s]]
  (assert record-s)
  (if (map? record-s)
   (db-apply1 db record-s rmap-insert imap-insert)
   (db-apply db record-s
     (fn [[int-rmap int-imap] record]
       [(rmap-insert int-rmap record)
        (imap-insert int-imap record)]))))

(defmethod query :update [db [_ {:keys [with] :as opts}]]
  (assert with)
  (db-apply db (find-records db opts)
    (fn [[int-rmap int-imap] old-record]
      (let [new-record (merge-compact old-record with)]
        [(rmap-update int-rmap old-record new-record)
         (imap-update int-imap old-record new-record)]))))

(defmethod query :delete [db [_ opts]]
  (db-apply db (find-records db opts)
    (fn [[int-rmap int-imap] old-record]
      [(rmap-delete int-rmap old-record)
       (imap-delete int-imap old-record)])))

(defmethod query :explain [db [_ [query-type opts]]]
  (assert (= query-type :select))
  (let [{:keys [where order offset limit only]} opts]
    (find-plan (keys (:imap db)) where order offset limit only)))

(defmethod query :create-index [db [_ {ispec :on}]]
  (if (get-in db [:imap ispec])
    [db 0]
    (let [records (vals (:rmap db))
          index   (index-build records ispec)]
      [(update db :imap assoc ispec index) 1])))

(defmethod query :drop-index [db [_ {ispec :on}]]
  (if (get-in db [:imap ispec])
    [db 0]
    [(update db :imap dissoc ispec) 1]))

(defmethod query :list-indexes [db [_ _]]
  (keys (:imap db)))

(defmethod query :multi-read [db [_ {:keys [queries]}]]
  (assert queries)
  (vec (map #(query db %) queries)))

(defmethod query :multi-write [db [_ {:keys [queries]}]]
  (assert queries)
  (reduce
    (fn [[int-db int-results] q]
      (let [[aug-db result] (query int-db query)]
        [aug-db (conj int-results result)]))
    [db []]
    queries))

(defmethod query :checked-write [db [_ {:keys [check expected write]}]]
  (let [actual (query db check)]
    (if (= actual expected)
      (let [[new-db result] (query db write)]
        [new-db [true result]])
      [db [false actual]])))

(defn init []
  {:rmap (sorted-map)
   :imap {}})
