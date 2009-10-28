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

(defn- index-insert [index on record]
  (let [val     (on record)
        indexed (get index val)]
    (update index val
      (fn [indexed]
        (cond
          (nil? indexed) record
          (set? indexed) (conj indexed record)
          :single-id     (hash-set indexed record))))))

(defn- index-delete [index on record]
  (let [val     (on record)
        indexed (val index)]
    (update index val
      (fn [indexed]
        (if (and (set? indexed) (> 1 (count indexed)))
          (do
            (assert (contains? indexed record))
            (disj indexed record))
          (do
            (assert (= #{record} indexed))
            nil))))))

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

(defn- index-build [records on]
  (reduce
    (fn [int-index record] (index-insert int-index on record))
    (sorted-map)
    records))

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
      (let [[attr val]  wrest
            sing-op-fn (sing-op-fns op)]
        (fn [record]
          (sing-op-fn (attr record) val)))
    (doub-op? op)
      (let [[attr [val1 val2]] wrest
            [doub-op-fn1 doub-op-fn2]      (doub-op-fns op)]
        (fn [record]
          (let [record-val (attr record)]
            (and (doub-op-fn1 record-val val1)
                 (doub-op-fn2 record-val val2)))))
    (= op :in)
      (let [[attr val-vec] wrest
            val-set        (set val-vec)]
        (fn [record]
          (contains? val-set (attr record))))
    (nil? op)
      (constantly true)
    :else
      (raise (str "where op " op " not recognized"))))

(defn- order-keyfn [[attr dir]]
  (assert (#{:asc :dsc} dir))
  (if (= dir :asc)
    (fn [record-a record-b]
      (compare (attr record-a) (attr record-b)))
    (fn [record-a record-b]
      (compare (attr record-b) (attr record-a)))))

(defn- filter-plan [source where]
  (if-not where
    source
    {:action  :filter
     :where   where
     :ordered (:ordered source)
     :size    (:size source)
     :cost    [:scan (:size source)]
     :source  source}))

(defn- where-plans [db where]
  [(filter-plan
      {:action  :db-scan
       :ordered [:id :asc]
       :size    :db
       :cost    [:scan :db]}
      where)])

(defn- order-plan [source order]
  (if (or (not order) (= order (:ordered source)))
    source
    {:action  :sort
     :order   order
     :ordered order
     :size    (:size source)
     :cost    [:sort (:size source)]
     :source  source}))

(defn- offset-plan [source offset]
  (if-not offset
    source
    {:action  :offset
     :offset  offset
     :ordered (:ordered source)
     :size    (:size source)
     :cost    [:scan :range]
     :source  source}))

(defn- limit-plan [source limit]
  (if-not limit
    source
    {:action  :limit
     :limit   limit
     :ordered (:ordered source)
     :size    (:size source)
     :cost    [:scan :range]
     :source  source}))

(defn- only-plan [source only]
  (if-not only
    source
    {:action  :only
     :only    only
     :ordered (:ordered source)
     :size    (:size source)
     :cost    [:scan (:size source)]
     :source  source}))

(defn symbolic-costs [{:keys [source] :as plan}]
  (cons (:cost plan)
    (cond
      (nil? source) nil
      (map? source) (symbolic-costs source)
      :else         (apply concat (map symbolic-costs source)))))

(def- numeric-costs
  {[:sort :db]     9
   [:set  :db]     8
   [:scan :db]     7
   [:sort :range]  6
   [:set  :range]  5
   [:scan :range]  4
   [:sort :bucket] 3
   [:set  :bucket] 2
   [:scan :bucket] 1})

(defn- quantify-cost [plan]
  (let [s-costs (symbolic-costs plan)
        n-costs (map numeric-costs s-costs)]
    (greatest n-costs)))

(defn- wo-plan [db where order]
  (let [w-plans (where-plans db where)
        o-plans (map #(order-plan % order) w-plans)]
    (least-by quantify-cost o-plans)))

(defn- find-plan [db where order offset limit only]
  (-> (wo-plan db where order)
    (offset-plan offset)
    (limit-plan limit)
    (only-plan only)))

(defmulti- exec (fn [db plan] (:action plan)))

(defn- exec-source [db plan]
  (exec db (:source plan)))

(defmethod exec :db-scan [db plan]
  (vals (:rmap db)))

(defmethod exec :filter [db plan]
  (filter (where-pred (:where plan)) (exec-source db plan)))

(defmethod exec :sort [db plan]
  (sort (order-keyfn (:order plan)) (exec-source db plan)))

(defmethod exec :offset [db plan]
  (drop (:offset plan) (exec-source db plan)))

(defmethod exec :limit [db plan]
  (take (:limit plan) (exec-source db plan)))

(defmethod exec :only [db plan]
  (let [only (:only plan)]
    (map #(select-keys % only) (exec-source db plan))))

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

(defn- q-explain [db {[query-type opts] :query :as explain-opts}]
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
