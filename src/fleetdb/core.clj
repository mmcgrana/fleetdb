(ns fleetdb.core
  (use (fleetdb util)))

(defn init []
  {:rmap (sorted-map)
   :imap (sorted-map)})

(defn- index-insert [index on record]
  (let [val (on record)]
    (if (not (nil? val))
      (assoc index val (:id record)))))

(defn- indexes-insert [imap record]
  (reduce
    (fn [int-imap [on index]]
      (if-let [new-index (index-insert index on record)]
        (assoc int-imap on new-index)
        int-imap))
    imap imap))

(defn- index-delete [index on record]
  (let [val (on record)]
    (if (not (nil? val))
      (dissoc index val))))

(defn- indexes-delete [imap record]
  (reduce
    (fn [int-imap [on index]]
      (if-let [new-index (index-delete index on record)]
        (assoc int-imap on new-index)
        int-imap))
    imap imap))

(defn- q-insert [db {:keys [record]}]
  (assert record)
  (let [id   (:id record)
        {old-rmap :rmap old-imap :imap} db]
    (assert id)
    (assert (not (contains? old-rmap id)))
    (let [new-rmap (assoc old-rmap id record)
          new-imap (indexes-insert old-imap record)]
      (assoc db :rmap new-rmap :imap new-imap))))

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
      (raise "where op not recognized")))

(defn- order-keyfn [attr dir]
  (assert (#{:asc :dsc} dir))
  (if (= dir :asc)
    (fn [record-a record-b]
      (compare (attr record-a) (attr record-b)))
    (fn [record-a record-b]
      (compare (attr record-b) (attr record-a)))))

(defn- apply-order [records order]
  (if-let [[attr dir] order]
    (sort (order-keyfn attr dir) records)
    records))

(defn- apply-offset [records offset]
  (if offset (drop offset records) records))

(defn- apply-limit [records limit]
  (if limit (take limit records) records))

(defn- find-records [records {:keys [where order offset limit]}]
  (-> (filter (where-pred where) records)
    (apply-order order)
    (apply-offset offset)
    (apply-limit limit)))

(defn- apply-only [records only]
  (if only
    (map #(select-keys % only) records))
    records)

(defn- q-select [db {:keys [only] :as opts}]
  (-> (find-records (vals (:rmap db)) opts)
    (apply-only only)))

(defn- q-count [db opts]
  (count (find-records (vals (:rmap db)) opts)))

(defn- q-update [db {:keys [with] :as opts}]
  (assert with)
  (let [{old-rmap :rmap old-imap :imap} db
        old-records     (find-records (vals old-rmap) opts)
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
        old-records (find-records (vals old-rmap) opts)
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

(defn- q-index [db {:keys [on where]}]
  (assoc-in db [:imap on]
    (reduce
      (fn [int-index record]
        (if-let [val (on record)]
          (assoc int-index val (:id record))
          int-index))
      (sorted-map)
      (vals (:rmap db)))))

(def- query-fns
  {:select q-select
   :count  q-count
   :insert q-insert
   :update q-update
   :delete q-delete
   :index  q-index})

(defn exec [db [query-type opts]]
  (if-let [queryfn (query-fns query-type)]
    (queryfn db opts)
    (raise "command not recognized")))
