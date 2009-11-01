(ns fleetdb.core
  (use (fleetdb util)))

(defn init []
  {:rmap (sorted-map)
   :imap (sorted-map)})

(defn- rmap-insert [rmap record]
  (let [id (:id record)]
    (assert id)
    (assert (not (contains? rmap id)))
    (assoc rmap (:id record) record)))

(defn- rmap-update [rmap old-record new-record]
  (assoc rmap (:id old-record) new-record))

(defn- rmap-delete [rmap old-record]
  (dissoc rmap (:id old-record)))

(defn- index-on [db attr]
  (attr (:imap db)))

(defn- index-insert [index on record]
  (let [aval    (on record)
        indexed (get index aval)]
    (update index aval
      (fn [indexed]
        (cond
          (nil? indexed) record
          (set? indexed) (conj indexed record)
          :single-record (hash-set indexed record))))))

(defn- index-delete [index on record]
  (let [aval    (on record)
        indexed (aval index)]
    (update index aval
      (fn [indexed]
        (if (and (set? indexed) (> 1 (count indexed)))
          (do
            (assert (contains? indexed record))
            (disj indexed record))
          (do
            (assert (= #{record} indexed))
            nil))))))

(defn- index-build [records on]
  (reduce
    (fn [int-index record] (index-insert int-index on record))
    (sorted-map)
    records))

(defn- index-flatten1 [indexed]
  (if (and indexed (not (set? indexed)))
    (list indexed)))

(defn- index-flatten [indexeds]
  (lazy-seq
    (when-let [iseq (seq indexeds)]
      (let [f (first iseq) r (rest iseq)]
        (cond
          (nil? f) r
          (set? f) (concat f r)
          :single  (cons f r))))))

(defn- imap-apply [imap apply-fn]
  (reduce
    (fn [int-imap [on index]]
      (assoc int-imap (apply-fn on index)))
    {}
    imap))

(defn- imap-insert [imap record]
  (imap-apply imap
    (fn [on index]
      (index-insert index on record))))

(defn- imap-update [imap old-record new-record]
  (imap-apply imap
    (fn [on index]
      (-> index
        (index-delete on old-record)
        (index-insert  on new-record)))))

(defn- imap-delete [imap record]
  (imap-apply imap
    (fn [on index] (index-delete index on record))))

(def- conj-op?
  #{:and :or})

(def- conj-op-fns
  {:and and? :or or?})

(def- sing-op?
  #{:= :!= :< :> :<= :> :>=})

(def- sing-op-fns
  {:= = :!= != :< < :<= <= :> > :>= >=})

(def- doub-op?
  #{:>< :>=< :><= :>=<=})

(def- doub-op-fns
  {:>< [> <] :>=< [>= <] :><= [> <=] :>=<= [>= <=]})

(defn- where-pred [[op & wrest]]
  (cond
    (conj-op? op)
      (let [subpreds (map #(where-pred %) wrest)
            conj-op-fn  (conj-op-fns op)]
        (fn [record]
          (conj-op-fn (map #(% record) subpreds))))
    (sing-op? op)
      (let [[attr aval]  wrest
            sing-op-fn (sing-op-fns op)]
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

(defn- order-keyfn [[attr dir]]
  (assert (#{:asc :dsc} dir))
  (if (= dir :asc)
    (fn [record-a record-b]
      (compare (attr record-a) (attr record-b)))
    (fn [record-a record-b]
      (compare (attr record-b) (attr record-a)))))

(defn- root-plan [db [attr dir :as order]]
  (if (and order (index-on db attr))
    {:action  :index-scan
     :attr    attr
     :dir     dir
     :ordered true}
    {:action  :db-scan
     :ordered false}))

(defn- filter-plan [source where]
  (if-not where
    source
    {:action  :filter
     :where   where
     :ordered (:ordered source)
     :source  source}))

(defn- root-filter-plan [db where order]
  (-> (root-plan db order) (filter-plan where)))

(def- range-op?
  #{:< :<= :> :>= :>< :>=< :><= :>=<=})

(defn- where-plan [db where order]
  (let [[op & wrest] where]
    (cond
      ; only choice -> optimal
      (not op)
        (root-plan db order)

      ; only simple choice -> probably optimal
      (= :!= op)
        (root-filter-plan db where order)

      ; when no index, use root-filter-plan -> optimal
      ; when reasonable index and no order -> probably optimal
      ; when reasonable index and order -> may be suboptimal, but reasonable
      (= := op)
        (let [[attr aval] wrest]
          (if (index-on db attr)
            {:action  :index-lookup
             :attr    attr
             :aval    aval
             :ordered false}
            (root-filter-plan db where order)))

      ; as above
      (= :in op)
        (let [[attr in] wrest]
          (if (index-on db attr)
            {:action  :index-multilookup
             :attr    attr
             :in      in
             :ordered false}
            (root-filter-plan db where order)))

      (range-op? op)
        (let [[attr aval] wrest]
          (if (index-on db attr)
            (let [[oattr odir] order
                  ordered      (= attr oattr)]
              {:action  :index-range
               :attr    attr
               :dir     (if ordered oattr)
               :op      op
               :aval    aval
               :ordered ordered})
            (root-filter-plan db where order)))

      ; when no indexes use root-filter-plan -> optimal
      ; when some reasonable, most selective first index -> probably optimal
      ; when non-index or not-most selective first -> suboptimal
      (= :and op)
        (let [main-plan     (where-plan db (first wrest) order)
              filter-wheres (next wrest)]
          {:action  :filter
           :where   (cons :and filter-wheres)
           :ordered (:ordered main-plan)
           :source  main-plan})

      ; meh -> fair but not great
      (= :or op)
        (let [sub-plans (map #(where-plan db % order) wrest)]
          {:action  :union
           :ordered false
           :source  sub-plans})

      :else
        (raise "where op " op " not recognized"))))

(defn- order-plan [source order]
  (if (or (not order) (:ordered source))
    source
    {:action  :sort
     :orde    order
     :ordered true
     :source  source}))

(defn- offset-plan [source offset]
  (if-not offset
    source
    {:action  :offset
     :offset  offset
     :ordered (:ordered source)
     :source  source}))

(defn- limit-plan [source limit]
  (if-not limit
    source
    {:action  :limit
     :limit   limit
     :ordered (:ordered source)
     :source  source}))

(defn- only-plan [source only]
  (if-not only
    source
    {:action  :only
     :only    only
     :ordered (:ordered source)
     :source  source}))

(defn- find-plan [db where order offset limit only]
  (-> (where-plan db where order)
    (order-plan order)
    (offset-plan offset)
    (limit-plan limit)
    (only-plan only)))

(defmulti- exec (fn [db plan] (:action plan)))

(defmethod exec :db-scan [{:keys [rmap]} plan]
  (vals rmap))

(defmethod exec :index-scan [db {:keys [attr dir]}]
  (let [index    (index-on db attr)
        seq-fn   (if (= dir :asc) seq rseq)
        pairs    (seq-fn index)
        indexeds (map val pairs)]
    (index-flatten indexeds)))

(defmethod exec :index-lookup [db {:keys [attr aval]}]
  (let [index   (index-on db attr)
        indexed (index aval)]
    (index-flatten1 indexed)))

(defmethod exec :index-multilookup [db {:keys [attr in]}]
  (let [index    (index-on db attr)
        indexeds (map index in)]
    (index-flatten indexeds)))

(def- one-sided-op-fns
  {:< < :<= <= :> > :>= >=})

(def- two-sided-op-fns
  {:>< [> <] :>=< [>= <] :><= [> <=] :>=<= [>= <=]})

(defmethod exec :index-range [db {:keys [attr dir op aval]}]
  (let [index     (index-on db attr)
        subseq-fn (if (= dir :asc) subseq rsubseq)
        pairs     (if-let [op-fn (one-sided-op-fns op)]
                    (subseq-fn index op-fn aval)
                    (let [[op-fn1 op-fn2] (two-sided-op-fns op)
                          [aval1  aval2]   aval]
                      (subseq-fn index op-fn1 aval1 op-fn2 aval2)))
        indexeds  (map val pairs)]
    (index-flatten indexeds)))

(defmethod exec :union [db {:keys [source]}]
  (union-stream (map #(exec db %) source)))

(defmethod exec :filter [db {:keys [where source]}]
  (filter (where-pred where) (exec db source)))

(defmethod exec :sort [db {:keys [order source]}]
  (sort (order-keyfn order) (exec db source)))

(defmethod exec :offset [db {:keys [offset source]}]
  (drop offset (exec db source)))

(defmethod exec :limit [db {:keys [limit source]}]
  (take limit (exec db source)))

(defmethod exec :only [db {:keys [only source]}]
  (map #(select-keys % only) (exec db source)))

(defn- find-records [db {:keys [where order offset limit only]}]
  (exec db
    (find-plan db where order offset limit only)))

(defn- q-select [db opts]
  (find-records db opts))

(defn- q-count [db opts]
  (count (find-records db opts)))

(defn- db-apply [db records apply-fn]
  (let [{old-rmap :rmap old-imap :imap} db
        [new-rmap new-imap] (reduce apply-fn [old-rmap old-imap] records)]
    [(assoc db :rmap new-rmap :imap new-imap) (count records)]))

(defn- q-insert [db {:keys [records]}]
  (assert records)
  (db-apply db records
    (fn [[int-rmap int-imap] record]
      [(rmap-insert int-rmap record)
       (imap-insert int-imap record)])))

(defn- q-update [db {:keys [with] :as opts}]
  (assert with)
    (db-apply db (find-records db opts)
      (fn [[int-rmap int-imap] old-record]
        (let [new-record (merge-compact old-record with)]
          [(rmap-update int-rmap old-record new-record)
           (imap-update int-imap old-record new-record)]))))

(defn- q-delete [db opts]
  (db-apply db (find-records db opts)
    (fn [[int-rmap int-imap] old-record]
      [(rmap-delete int-rmap old-record)
       (imap-delete int-imap old-record)])))

(defn- q-explain [db {[query-type opts] :query}]
  (assert (= query-type :select))
  (let [{:keys [where order offset limit only]} opts]
    (find-plan db where order offset limit only)))

(defn- q-create-index [db {:keys [on where]}]
  (assert (not (on (:imap db))))
  (let [records (vals (:rmap db))
        index   (index-build records on)]
    [(update db :imap assoc on index) 1]))

(defn- q-drop-index [db {:keys [on]}]
  (let [index (on (:imap db))]
    (assert index)
    [(update db :imap dissoc on) 1]))

(defn- q-list-indexes [db opts]
  (vec (keys (:imap db))))

(declare query)

(defn- q-multi-read [db {:keys [queries]}]
  (vec (map #(query db %) queries)))

(defn- q-multi-write [db {:keys [queries]}]
  (reduce
    (fn [[int-db int-results] q]
      (let [[aug-db result] (query int-db query)]
        [aug-db (conj int-results result)]))
    [db []]
    queries))

(defn- q-checked-write [db {:keys [check expect write]}]
  (if (= (query db check) expect)
    (query db write)))

(def- query-fns
  {:select        q-select
   :count         q-count
   :insert        q-insert
   :update        q-update
   :delete        q-delete
   :explain       q-explain
   :create-index  q-create-index
   :drop-index    q-drop-index
   :list-indexes  q-list-indexes
   :multi-read    q-multi-read
   :multi-write   q-multi-write
   :checked-write q-checked-write})

(defn query [db [query-type opts]]
  (if-let [queryfn (query-fns query-type)]
    (queryfn db opts)
    (raise (str "query type " query-type " not recognized"))))
