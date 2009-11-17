(ns fleetdb.planner
  (use (fleetdb util shared)))

(defn- index-order-prefix? [ispec order]
  (= (take (count order) ispec)
     order))

(def- flip-dir
  {:asc :dsc :dsc :asc})

(defn- flip-order [order]
  (map (fn [[attr dir]] [attr (flip-dir dir)]) order))

(defn- conds-order-ipplan [ispec eq ineq where order]
  (let [[rispec low-val low-inc high-val high-inc where-count]
    (loop [rispec ispec low-val [] high-val [] where-count 0]
      (if-not rispec
        [nil low-val true high-val true where-count]
        (let [[iattr idir] (first rispec)
              rrispec      (next rispec)]
          (if-let [v (get eq iattr)]
            (recur rrispec (conj low-val v) (conj high-val v) (inc where-count))
            (if-let [[low-v low-i high-v high-i] (get ineq iattr)]
              [rrispec (conj low-val low-v) low-i (conj high-val high-v) high-i (inc where-count)]
              [rispec low-val true high-val true where-count])))))]
    (let [[order-left order-count dir]
      (cond
        (empty? order)
          [nil 0 :asc]
        (index-order-prefix? rispec order)
          [nil (count order) :asc]
        (index-order-prefix? rispec (flip-order order))
          [nil (count order) :dsc]
        :else
          [order 0 :asc])]
      (let [low-val  (vec-pad low-val  (count ispec) neg-inf)
            high-val (vec-pad high-val (count ispec) pos-inf)]
        {:ispec       ispec
         :where-count where-count
         :order-count order-count
         :low-val     low-val
         :low-inc     low-inc
         :high-val    high-val
         :high-inc    high-inc
         :dir         dir
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
    :<  [neg-inf   true  v       false]
    :<= [neg-inf   true  v       true ]
    :>  [v         false pos-inf true ]
    :>= [v         true  pos-inf true ]
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

(defn- sort-plan [source order]
  (if order [:sort order source] source))

(defn- filter-plan [source where]
  (if where [:filter where source] source))

(defn- where-order-plan* [ispecs where order]
  (let [ipplans (where-order-ipplans ispecs where order)
        ipplan  (high ipplan-compare ipplans)]
    (if (and ipplan (pos? (+ (:where-count ipplan) (:order-count ipplan))))
      (-> [:index-seq (select-keys ipplan
                        [:ispec :low-val :high-val :low-inc :high-inc :dir])]
        (filter-plan (:where-left ipplan))
        (sort-plan   (:order-left ipplan)))
      (-> [:rmap-scan]
        (filter-plan where)
        (sort-plan order)))))

(defn- where-order-plan [ispecs where order]
  (cond
    (and (= (get where 0) :=) (= (get where 1) :id))
      [:rmap-lookup (get where 2)]

    (and (= (get where 0) :in) (= (get where 1) :id))
      (-> [:rmap-multilookup (get where 2)]
        (sort-plan order))

    (= (get where 0) :or)
      [:union nil (map #(where-order-plan ispecs % order) (next where))]

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
