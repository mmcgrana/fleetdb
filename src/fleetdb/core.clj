(ns fleetdb.core
  (use (fleetdb util)))

(defn init []
  {:rmap {}
   :imap {}})

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

(defn- rmap-insert [rmap record]
  (let [id (:id record)]
    (assert id)
    (assert (not (contains? rmap id)))
    (assoc rmap (:id record) record)))

(defn- rmap-update [rmap old-record new-record]
  (assoc rmap (:id old-record) new-record))

(defn- rmap-delete [rmap old-record]
  (dissoc rmap (:id old-record)))

(defn- index-get [db spec]
  (get (:imap db) spec))

(defn- index-insert [index {:keys [on where]} record]
  (if (and where (not ((where-pred where) record)))
    index
    (update index (on record)
      (fn [indexed]
        (cond
          (nil? indexed) record
          (set? indexed) (conj indexed record)
          :single-record (hash-set indexed record))))))

(defn- index-delete [index {:keys [on where]} record]
  (if (and where (not ((where-pred where) record)))
    index
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
              nil)))))))

(defn- index-build [records spec]
  (reduce
    (fn [int-index record] (index-insert int-index spec record))
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
    (fn [int-imap [spec index]]
      (assoc int-imap (apply-fn spec index)))
    {}
    imap))

(defn- imap-insert [imap record]
  (imap-apply imap
    (fn [spec index]
      (index-insert index spec record))))

(defn- imap-update [imap old-record new-record]
  (imap-apply imap
    (fn [spec index]
      (-> index
        (index-delete spec old-record)
        (index-insert spec new-record)))))

(defn- imap-delete [imap record]
  (imap-apply imap
    (fn [spec index] (index-delete index spec record))))

(defn- root-plan [db [attr dir :as order]]
  (if (and order (index-get db {:on attr}))
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
          (if (index-get db {:on attr})
            {:action  :index-lookup
             :attr    attr
             :aval    aval
             :ordered false}
            (root-filter-plan db where order)))

      ; as above
      (= :in op)
        (let [[attr in] wrest]
          (if (index-get db {:on attr})
            {:action  :index-multilookup
             :attr    attr
             :in      in
             :ordered false}
            (root-filter-plan db where order)))

      ; hmm
      (range-op? op)
        (let [[attr aval] wrest]
          (if (index-get db {:on attr})
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
  (let [index    (index-get db {:on attr})
        seq-fn   (if (= dir :asc) seq rseq)
        pairs    (seq-fn index)
        indexeds (map val pairs)]
    (index-flatten indexeds)))

(defmethod exec :index-lookup [db {:keys [attr aval]}]
  (let [index   (index-get db {:on attr})
        indexed (index aval)]
    (index-flatten1 indexed)))

(defmethod exec :index-multilookup [db {:keys [attr in]}]
  (let [index    (index-get db {:on attr})
        indexeds (map index in)]
    (index-flatten indexeds)))

(def- one-sided-op-fns
  {:< < :<= <= :> > :>= >=})

(def- two-sided-op-fns
  {:>< [> <] :>=< [>= <] :><= [> <=] :>=<= [>= <=]})

(defmethod exec :index-range [db {:keys [attr dir op aval]}]
  (let [index     (index-get db {:on attr})
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

(defn- q-create-index [db spec]
  (assert (not (index-get db spec)))
  (let [records (vals (:rmap db))
        index   (index-build records spec)]
    [(update db :imap assoc spec index) 1]))

(defn- q-drop-index [db spec]
  (let [index (index-get db spec)]
    (assert index)
    [(update db :imap dissoc spec) 1]))

(defn- q-list-indexes [db opts]
  (keys (:imap db)))

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
