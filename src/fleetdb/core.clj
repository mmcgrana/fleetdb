(ns fleetdb.core
  (use (fleetdb util)))

(defn init []
  {:rmap (sorted-map)
   :imap (sorted-map)})

(defn- index-insert [index on record]
  (if-let? [non-nil? val (on record)]
    (assoc index val (:id record))))

(defn- index-delete [index on record]
  (if-let? [non-nil? val (on record)]
    (dissoc index val)))

(defn- indexes-update [imap record update-fn]
  (reduce
    (fn [int-imap [on index]]
      (if-let [new-index (update-fn index on record)]
        (assoc int-imap on new-index)
        int-imap))
    imap imap))

(defn- indexes-insert [imap record]
  (indexes-update imap record index-insert))

(defn- indexes-delete [imap record]
  (indexes-update imap record index-delete))

(defn- q-insert [db {:keys [records]}]
  (assert records)
  (let [{old-rmap :rmap old-imap :imap} db
        [new-rmap new-imap]
          (reduce
            (fn [[int-rmap int-imap] record]
              (let [id (:id record)]
                (assert id)
                (assert (not (contains? int-rmap id)))
                [(assoc int-rmap id record)
                 (indexes-insert int-imap record)]))
            [old-rmap old-imap]
            records)]
    [(assoc db :rmap new-rmap :imap new-imap) (count records)]))

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

(defn- order-keyfn [attr dir]
  (assert (#{:asc :dsc} dir))
  (if (= dir :asc)
    (fn [record-a record-b]
      (compare (attr record-a) (attr record-b)))
    (fn [record-a record-b]
      (compare (attr record-b) (attr record-a)))))

(defn- where-plans [db where order]
  [{:action  :db-scan
    :options nil
    :source  nil
    :order   [:id :asc]
    :size    :db
    :cost    [:scan :db]}])

(defn- filter-plan [source where]
  (if-not where
    source
    {:action  :filter
     :options {:where where}
     :source  source
     :order   (:order source)
     :size    (:size source)
     :cost    [:scan (:size source)]}))

(defn- order-plan [source order]
  (if (or (not order) (= order (:order source)))
    source
    {:action  :sort
     :options {:order order}
     :source  source
     :order   order
     :size    (:size source)
     :cost    [:sort (:size source)]}))

(defn- offset-plan [source offset]
  (if-not offset
    source
    {:action  :offset
     :options {:offset offset}
     :source  source
     :order   (:order source)
     :size    (:size source)
     :cost    [:scan :range]}))

(defn- limit-plan [source limit]
  (if-not limit
    source
    {:action  :limit
     :options {:limit limit}
     :source  source
     :order   (:order source)
     :size    (:size source)
     :cost    [:scan :range]}))

(defn- only-plan [source only]
  (if-not only
    source
    {:action  :only
     :options {:only only}
     :source  source
     :order   (:order source)
     :size    (:size source)
     :cost    [:scan (:size source)]}))

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
  (let [w-plans (where-plans db where order)
        o-plans (map #(order-plan % order) w-plans)]
    (least-by quantify-cost o-plans)))

(defn- select-plan [db where order offset limit only]
  (-> (wo-plan db where order)
    (offset-plan offset)
    (limit-plan limit)
    (only-plan only)))

(defn- execute-plan [db plan]
  (let [{:keys [action options source]} plan]
    (condp = action
      :db-scan
        (vals (:rmap db))

      :filter
        (let [{:keys [where]} options]
          (filter (where-pred where) (execute-plan db source)))

      :sort
        (let [{[attr dir] :order} options]
          (sort (order-keyfn attr dir) (execute-plan db source)))

      :offset
        (let [{:keys [offset]} options]
          (drop offset (execute-plan db source)))

      :limit
        (let [{:keys [limit]} options]
          (take limit (execute-plan db source)))

      :only
        (let [{:keys [only]} options]
          (map #(select-keys % only) (execute-plan db source))))))

(defn- q-select [db {:keys [where order offset limit only]}]
  (execute-plan db
    (select-plan db where order offset limit only)))

(defn- q-count [db opts]
  (count (q-select db opts)))

(defn- q-update [db {:keys [with] :as opts}]
  (assert with)
  (let [{old-rmap :rmap old-imap :imap} db
        old-records     (q-select db opts)
        num-old-records (count old-records)
        [new-rmap new-imap]
          (reduce
            (fn [[int-rmap int-imap] old-record]
              (let [new-record (merge-compact old-record with)
                    aug-rmap   (assoc int-rmap (:id old-record) new-record)
                    aug-imap   (-> int-imap
                                 (indexes-delete old-record)
                                 (indexes-insert new-record))]
                [aug-rmap aug-imap]))
            [old-rmap old-imap]
            old-records)]
    [(assoc db :rmap new-rmap :imap new-imap) num-old-records]))

(defn- q-delete [db opts]
  (let [{old-rmap :rmap old-imap :imap} db
        old-records (q-select db opts)
        num-old-records (count old-records)
        [new-rmap new-imap]
          (reduce
            (fn [[int-rmap int-imap] old-record]
              (let [aug-rmap (dissoc int-rmap (:id old-record))
                    aug-imap (indexes-delete int-imap old-record)]
                [aug-rmap aug-imap]))
            [old-rmap old-imap]
            old-records)]
    [(assoc db :rmap new-rmap :imap new-imap) num-old-records]))

(defn- q-create-index [db {:keys [on where]}]
  (let [records (vals (:rmap db))
        index
          (reduce
            (fn [int-index record]
              (if-let [val (on record)]
                (assoc int-index val (:id record))
                int-index))
            (sorted-map)
            records)]
    [(assoc-in db [:imap on] index) (count index)]))

(defn- q-drop-index [db {:keys [on]}]
  (let [index (get-in db [:imap on])]
    [(update-in db [:imap] dissoc on) (count index)]))

(defn- q-list-indexes [db opts]
  (vec (keys (:imap db))))

(declare exec)

(defn- q-multi-read [db {:keys [queries]}]
  (vec
    (map (fn [query] (exec db query))
         queries)))

(defn- q-multi-write [db {:keys [queries]}]
  (reduce
    (fn [[int-db int-results] query]
      (let [[aug-db result] (exec int-db query)]
        [aug-db (conj int-results result)]))
    [db []]
    queries))

(defn- q-checked-write [db {:keys [check expect write]}]
  (if (= (exec db check) expect)
    (exec db write)))

(def- query-fns
  {:select        q-select
   :count         q-count
   :insert        q-insert
   :update        q-update
   :delete        q-delete
   :create-index  q-create-index
   :drop-index    q-drop-index
   :list-indexes  q-list-indexes
   :multi-read    q-multi-read
   :multi-write   q-multi-write
   :checked-write q-checked-write})

(defn exec [db [query-type opts]]
  (if-let [queryfn (query-fns query-type)]
    (queryfn db opts)
    (raise (str "query type " query-type " not recognized"))))
