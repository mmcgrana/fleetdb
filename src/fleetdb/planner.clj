(ns fleetdb.planner
  (use (fleetdb util shared)))

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

(defn- conds-order-ipplan [ispec eq ineq where order]
  (let [[rispec left-val left-inc right-val right-inc where-count]
    (loop [rispec ispec left-val [] right-val [] where-count 0]
      (if-not rispec
        [nil left-val true right-val true where-count]
        (let [[icattr icdir] (first rispec)
              rrispec        (next rispec)]
          (if-let [v (get eq icattr)]
            (recur rrispec (conj left-val v) (conj right-val v) (inc where-count))
            (if-let [[low-v low-i high-v high-i] (get ineq icattr)]
              (if (= icdir :asc)
                [rrispec (conj left-val low-v)  low-i  (conj right-val high-v) high-i (inc where-count)]
                [rrispec (conj left-val high-v) high-i (conj right-val low-v)  low-i  (inc where-count)])
              [rispec left-val true right-val true where-count])))))]
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
         :where-left  where
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
    (fn [[eq ineq] acond]
      (let [[cop cattr cval] acond]
        (cond
          (eq-op? cop)
            (if (contains? eq cattr)
              (raise (str "duplicate equality on " cattr))
              [(assoc eq cattr cval) ineq])
          (ineq-op? cop)
            (if (contains? ineq cattr)
              (raise (str "duplicate inequality on " cattr))
              [eq (assoc ineq cattr (cond-low-high cop cval))])
          (other-op? cop)
            [eq ineq]
          :else
            (raise (str "invalid where " acond)))))
    [{} {}]
    conds))

(defn- where-order-ipplans [ispecs where order]
  (let [[eq ineq] (-> where flatten-where partition-conds)]
    (map #(conds-order-ipplan % eq ineq where order) ispecs)))

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
