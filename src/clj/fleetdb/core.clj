(ns fleetdb.core
  (:import (clojure.lang Sorted RT)
           (fleetdb Compare))
  (:require (clojure.contrib [core :as core])
            (fleetdb [lint :as lint] [types :as types]))
  (:use (fleetdb util)))

;; General ordering

(def- neg-inf :neg-inf)
(def- pos-inf :pos-inf)

(defn- normalize-order [order]
  (if (string? (first order)) [order] order))

(defn- normalize-ispec [ispec]
  (if (string? ispec)
    [[ispec "asc"]]
    (vec-map
      (fn [ispec-comp]
        (if (string? ispec-comp) [ispec-comp "asc"] ispec-comp))
      ispec)))

(defn- record-compare [order]
  (let [norder (normalize-order order)]
    (if (= 1 (count norder))
      (let [[attr dir] (first norder)]
        (condp = dir
          "asc"  #(Compare/compare (%1 attr) (%2 attr))
          "desc" #(Compare/compare (%2 attr) (%1 attr))))
      (let [[[attr dir] & rorder] norder
            rcompare              (record-compare rorder)]
        (condp = dir
          "asc"  #(let [c (Compare/compare (%1 attr) (%2 attr))]
                    (if (zero? c) (rcompare %1 %2) c))
          "desc" #(let [c (Compare/compare (%2 attr) (%1 attr))]
                    (if (zero? c) (rcompare %1 %2) c)))))))

(defn- attr-compare [ispec]
  (let [nispec (normalize-ispec ispec)]
    (if (= 1 (count nispec))
      (let [[attr dir] (first nispec)]
        (condp = dir
          "asc"  #(Compare/compare %1 %2)
          "desc" #(Compare/compare %2 %1)))
      (let [[[attr dir] & rispec] nispec
            rcompare (attr-compare rispec)
            next-fn  (if (= 1 (count rispec)) #(first (rest %)) rest)]
        (condp = dir
          "asc"  #(let [c (Compare/compare (first %1) (first %2))]
                    (if (zero? c)
                      (rcompare (next-fn %1) (next-fn %2))
                      c))
          "desc" #(let [c (Compare/compare (first %1) (first %2))]
                    (if (zero? c)
                      (rcompare (next-fn %1) (next-fn %2))
                      c)))))))


;; Find planning

(defn- filter-plan [source where]
  (if where ["filter" where source] source))

(defn- sort-plan [source order]
  (if order ["sort" order source] source))

(defn- rmap-scan-plan [coll where order]
  (-> ["collection-scan" coll]
    (filter-plan where)
    (sort-plan order)))

(defn- index-order-prefix? [nispec order]
  (= (take (count order) nispec) order))

(def- flip-idir
  {"asc" "desc" "desc" "asc"})

(defn- flip-order [order]
  (map (fn [[attr dir]] [attr (flip-idir dir)]) order))

(defn- val-pad [left-v right-v ispec]
  (loop [left-vp left-v right-vp right-v rispec (drop (count left-v) ispec)]
    (if-let [[icattr icdir] (first rispec)]
      (if (= icdir "asc")
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
      :multi-cond         (vec (cons "and" conds)))))

(defn- conds-order-ipplan [ispec eq ineq other where order]
  (let [nispec (normalize-ispec ispec)
        [rispec left-val left-inc right-val right-inc where-count where-left]
    (loop [rispec nispec left-val [] right-val [] where-count 0 eq-left eq]
      (if-not rispec
        [nil left-val true right-val true where-count (build-where-left eq-left ineq other)]
        (let [[icattr icdir] (first rispec)
              rrispec        (next rispec)]
          (if-let [[v _] (get eq-left icattr)]
            (recur rrispec (conj left-val v) (conj right-val v)
                   (inc where-count) (dissoc eq-left icattr))
            (if-let [[[low-v low-i high-v high-i] _] (get ineq icattr)]
              (let [w-left (build-where-left eq-left (dissoc ineq icattr) other)]
                (if (= icdir "asc")
                  [rrispec (conj left-val low-v)  low-i  (conj right-val high-v) high-i (inc where-count) w-left]
                  [rrispec (conj left-val high-v) high-i (conj right-val low-v)  low-i  (inc where-count) w-left]))
              [rispec left-val true right-val true where-count (build-where-left eq-left ineq other)])))))]
    (let [norder (normalize-order order)
          [order-left order-count sdir]
      (cond
        (empty? norder)
          [nil 0 "left-right"]
        (index-order-prefix? rispec norder)
          [nil (count norder) "left-right"]
        (index-order-prefix? rispec (flip-order norder))
          [nil (count norder) "right-left"]
        :else
          [order 0 "left-right"])]
      (let [[left-val right-val] (val-pad left-val right-val nispec)]
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
    (= "and" (first where))
      (rest where)
    :single-op
      (list where)))

(def- eq-op?
  #{"="})

(def- ineq-op?
  #{"<" "<=" ">" ">=" "><" ">=<" "><=" ">=<="})

(def- other-op?
  #{"!=" "in" "not in" "or"})

(defn- cond-low-high [op v]
  (condp = op
    "<"  [neg-inf true  v       false]
    "<=" [neg-inf true  v       true ]
    ">"  [v       false pos-inf true ]
    ">=" [v       true  pos-inf true ]
    (let [[v1 v2] v]
      (condp = op
        "><"   [v1 false v2 false]
        ">=<"  [v1 true  v2 false]
        "><="  [v1 false v2 true ]
        ">=<=" [v1 true  v2 true ]))))

(defn- partition-conds [conds]
  (reduce
    (fn [[eq ineq other] acond]
      (let [[cop cattr cval] acond]
        (condv cop
          eq-op?
            (if (contains? eq cattr)
              (raise "duplicate equality on " cattr)
              [(assoc eq cattr [cval acond]) ineq other])
          ineq-op?
            (if (contains? ineq cattr)
              (raise "duplicate inequality on " cattr)
              [eq (assoc ineq cattr [(cond-low-high cop cval) acond]) other])
          other-op?
            [eq ineq (conj other acond)])))
    [{} {} []]
    conds))

(defn- where-order-ipplans [ispecs where order]
  (let [[eq ineq other] (-> where flatten-where partition-conds)]
    (map #(conds-order-ipplan % eq ineq other where order) ispecs)))

(defn- ipplan-compare [a b]
  (compare [(:where-count a) (:order-count a)]
           [(:where-count b) (:order-count b)]))

(defn- ipplan-useful? [ipplan]
  (pos? (+ (:where-count ipplan) (:order-count ipplan))))

(defn- ipplan-plan [coll {:keys [ispec sdir left-val left-inc
                                 right-val right-inc where-left order-left]}]
  (let [left-val-t  (if (= (count left-val)  1) (first left-val)  left-val)
        right-val-t (if (= (count right-val) 1) (first right-val) right-val)]
    (-> (if (= left-val-t right-val-t)
          ["index-lookup" [coll ispec left-val-t]]
          ["index-range"  [coll ispec sdir left-val-t left-inc right-val-t right-inc]])
      (filter-plan where-left)
      (sort-plan   order-left))))

(defn- where-order-plan* [coll ispecs where order]
  (let [ipplans (where-order-ipplans ispecs where order)
        ipplan  (high ipplan-compare ipplans)]
    (if (and ipplan (ipplan-useful? ipplan))
      (ipplan-plan coll ipplan)
      (rmap-scan-plan coll where order))))

(defn- where-order-plan [coll ispecs where order]
  (cond
    (and (= (get where 0) "=") (= (get where 1) "id"))
      ["collection-lookup" [coll (get where 2)]]

    (and (= (get where 0) "in") (= (get where 1) "id"))
      (-> ["collection-multilookup" [coll (get where 2)]]
        (sort-plan order))

    (= (get where 0) "or")
      ["union" order (map #(where-order-plan coll ispecs % order) (next where))]

    :else
      (where-order-plan* coll ispecs where order)))

(defn- offset-plan [source offset]
  (if offset ["offset" offset source] source))

(defn- limit-plan [source limit]
  (if limit ["limit" limit source] source))

(defn- only-plan [source only]
  (if only ["only" only source] source))

(defn- distinct-plan [source distinct]
  (if distinct ["distinct" source] source))

(defn- make-plan [coll ispecs opts]
  (let [{where "where" order "order" offset   "offset"
         limit "limit" only  "only"  distinct "distinct"} opts]
    (-> (where-order-plan coll ispecs where order)
      (offset-plan offset)
      (limit-plan  limit)
      (only-plan   only)
      (distinct-plan distinct))))


;; Find execution

(def- conj-op-fns
  {"and" and? "or" or?})

(def- sing-op-fns
  {"="  #(Compare/eq  %1 %2)
   "!=" #(Compare/neq %1 %2)
   "<"  #(Compare/lt  %1 %2)
   "<=" #(Compare/lte %1 %2)
   ">"  #(Compare/gt  %1 %2)
   ">=" #(Compare/gte %1 %2)})

(def- doub-op-fns
  {"><"   [(sing-op-fns ">" ) (sing-op-fns "<" )]
   ">=<"  [(sing-op-fns ">=") (sing-op-fns "<" )]
   "><="  [(sing-op-fns ">" ) (sing-op-fns "<=")]
   ">=<=" [(sing-op-fns ">=") (sing-op-fns "<=")]})

(defn- where-pred [[op & wrest]]
  (cond!
    (conj-op-fns op)
      (let [subpreds (map #(where-pred %) wrest)
            conj-op-fn  (conj-op-fns op)]
        (fn [record]
          (conj-op-fn (map #(% record) subpreds))))
    (sing-op-fns op)
      (let [[attr aval] wrest
            sing-op-fn  (sing-op-fns op)]
        (fn [record]
          (sing-op-fn (record attr) aval)))
    (doub-op-fns op)
      (let [[attr [aval1 aval2]] wrest
            [doub-op-fn1 doub-op-fn2]      (doub-op-fns op)]
        (fn [record]
          (let [record-aval (record attr)]
            (and (doub-op-fn1 record-aval aval1)
                 (doub-op-fn2 record-aval aval2)))))
    (= op "in")
      (let [[attr aval-vec] wrest
            aval-set        (set aval-vec)]
        (fn [record]
          (contains? aval-set (record attr))))
		(= op "not in")
		  (let [[attr aval-vec] wrest
		        aval-set        (set aval-vec)]
		    (fn [record]
		      (not (contains? aval-set (record attr)))))))

(defmulti- find-plan
  (fn [db [plan-type _]] plan-type))

(defmulti- count-plan
  (fn [db [plan-type _]] plan-type))

(defmethod find-plan  "filter" [db [_ where source]]
  (filter (where-pred where) (find-plan db source)))

(defmethod count-plan "filter" [db plan]
  (count (find-plan db plan)))

(defmethod find-plan  "sort" [db [_ order source]]
  (sort (record-compare order) (find-plan db source)))

(defmethod count-plan "sort" [db [_ order source]]
  (count-plan db source))

(defmethod find-plan  "offset" [db [_ offset source]]
  (drop offset (find-plan db source)))

(defmethod count-plan "offset" [db [_ offset source]]
  (max 0 (- (count-plan db source) offset)))

(defmethod find-plan  "limit" [db [_ limit source]]
  (take limit (find-plan db source)))

(defmethod count-plan "limit" [db [_ limit source]]
  (min limit (count-plan db source)))

(defmethod find-plan  "only" [db [_ only source]]
  (condv only
    vector?
      (map (fn [r] (map #(r %) only)) (find-plan db source))
    string?
      (map (fn [r] (r only)) (find-plan db source))))

(defmethod find-plan  "distinct" [db [_ source]]
  (distinct (find-plan db source)))

(defmethod count-plan "distinct" [db plan]
  (count (find-plan db plan)))

(defmethod find-plan  "union" [db [_ order sources]]
  (uniq
    (sort (record-compare (or order ["id" "asc"]))
          (apply concat (map #(find-plan db %) sources)))))

(defmethod count-plan "union" [db plan]
  (count (find-plan db plan)))

(defmethod find-plan  "collection-lookup" [db [_ [coll id]]]
  (if-let [record (get-in db [coll :rmap id])]
    (list record)))

(defmethod count-plan "collection-lookup" [db [_ [coll id]]]
  (if (get-in db [coll :rmap id]) 1 0))

(defmethod find-plan  "collection-multilookup" [db [_ [coll ids]]]
  (if-let [rmap (get-in db [coll :rmap])]
    (compact (map #(rmap %) ids))))

(defmethod count-plan "collection-multilookup" [db [_ [coll ids]]]
  (let [rmap (get-in db [coll :rmap])]
    (reduce (fn [c id] (if (contains? rmap id) (inc c) c)) 0 ids)))

(defmethod find-plan  "collection-scan" [db [_ coll]]
  (vals (get-in db [coll :rmap])))

(defmethod count-plan "collection-scan" [db [_ coll]]
  (count (get-in db [coll :rmap])))

(defn- indexed-flatten1 [indexed]
  (cond (nil? indexed) nil
        (set? indexed) (seq indexed)
        :single        (list indexed)))

(defn- indexed-count1 [indexed]
  (cond (nil? indexed) 0
        (set? indexed) (count indexed)
        :single        1))

(defn- indexed-flatten [indexeds]
  (lazy-seq
    (when-let [iseq (seq indexeds)]
      (let [f (first iseq) r (rest iseq)]
        (cond
          (nil? f) (indexed-flatten r)
          (set? f) (concat f (indexed-flatten r))
          :single  (cons f (indexed-flatten r)))))))

(defn- index-lookup [db [coll ispec val]]
  (get-in db [coll :imap ispec val]))

(defn- index-range [db [coll ispec sdir left-val left-inc right-val right-inc]]
  (let [#^Sorted index (get-in db [coll :imap ispec])]
    (if (= sdir "left-right")
      (let [base    (.seqFrom index left-val true)
            base-l  (if (or left-inc (not= (key (first base)) left-val))
                      base
                      (rest base))
            base-lr (if right-inc
                      (take-while #(Compare/lte (key %) right-val) base-l)
                      (take-while #(Compare/lt  (key %) right-val) base-l))]
        (vals base-lr))
      (let [base    (.seqFrom index right-val false)
            base-r  (if (or right-inc (not= (key (first base)) right-val))
                      base
                      (rest base))
            base-rl (if left-inc
                      (take-while #(Compare/gte (key %) left-val) base-r)
                      (take-while #(Compare/gt  (key %) left-val) base-r))]
        (vals base-rl)))))

(defmethod find-plan  "index-lookup" [db [_ lookup]]
  (indexed-flatten1 (index-lookup db lookup)))

(defmethod count-plan "index-lookup" [db [_ lookup]]
  (indexed-count1 (index-lookup db lookup)))

(defmethod find-plan  "index-range" [db [_ range]]
  (indexed-flatten (index-range db range)))

(defmethod count-plan "index-range" [db [_ range]]
  (reduce + (map indexed-count1 (index-range db range))))

(defn- coll-ispecs [db coll]
  (keys (get-in db [coll :imap])))

(defn- find-records [db coll opts]
  (or (find-plan db (make-plan coll (coll-ispecs db coll) opts))
      (list)))

(defn- count-records [db coll opts]
  (count-plan db (make-plan coll (coll-ispecs db coll) opts)))


;; RMap and IMap manipulation

(defn- rmap-insert [rmap record]
  (let [id (record "id")]
    (if-not id
      (raise "Record does not have an id: " record))
    (if (contains? rmap id)
      (raise "Duplicated id: " id))
    (assoc rmap (record "id") record)))

(defn- rmap-update [rmap old-record new-record]
  (assert (= (old-record "id") (new-record "id")))
  (assoc rmap (old-record "id") new-record))

(defn- rmap-delete [rmap old-record]
  (assert (contains? rmap (old-record "id")))
  (dissoc rmap (old-record "id")))

(defn- ispec-on-fn [ispec]
  (let [nispec (normalize-ispec ispec)
        attrs  (map first nispec)
        nattrs (count nispec)]
    (cond!
      (= nattrs 1) (let [attr (first attrs)] #(% attr))
      (> nattrs 1) #(vec-map (fn [attr] (% attr)) attrs))))

(defn- index-insert [index on-fn record]
  (update index (on-fn record)
    (fn [indexed]
      (cond
        (nil? indexed) record
        (set? indexed) (do (assert (not (contains? indexed record)))
                           (conj indexed record))
        :single-record (do (assert (not (= indexed record)))
                           (hash-set indexed record))))))

(defn- index-delete [index on-fn record]
  (update index (on-fn record)
    (fn [indexed]
      (condv indexed
        set? (do (assert (contains? indexed record))
                 (disj indexed record))
        map? (do (assert (= indexed record))
                 nil)))))

(defn- empty-index [ispec]
  (sorted-map-by (attr-compare ispec)))

(defn- index-build [records ispec]
  (let [on-fn (ispec-on-fn ispec)]
    (reduce
      (fn [i r] (index-insert i on-fn r))
      (empty-index ispec)
      records)))

(defn- imap-apply [imap apply-fn]
  (mash (fn [ispec index]
          (apply-fn (ispec-on-fn ispec) index))
        imap))

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
    (fn [on-fn index]
      (index-delete index on-fn record))))


;; Query implementations

(defn init []
  {})

(defmulti query* (fn [db q] (first q)))

(defmethod query* "select" [db [_ coll opts]]
  (find-records db coll opts))

(defmethod query* "count" [db [_ coll opts]]
  (count-records db coll opts))

(defn- db-apply [db coll records apply-fn]
  (let [old-coll (get db coll)
        old-rmap (:rmap old-coll)
        old-imap (:imap old-coll)
        [new-rmap new-imap] (reduce apply-fn [old-rmap old-imap] records)]
    [(assoc db coll {:rmap new-rmap :imap new-imap})
     (count records)]))

(defmethod query* "insert" [db [_ coll record-s]]
  (db-apply db coll (if (map? record-s) (list record-s) record-s)
    (fn [[int-rmap int-imap] record]
      [(rmap-insert int-rmap record)
       (imap-insert int-imap record)])))

(defn update-merge [m1 m2]
  (reduce
    (fn [m-int [k v]]
      (cond
        (nil? v)     (dissoc m-int k)
        (= "$inc" k) (let [[attr by] (first v)]
                       (update m-int attr #(+ by %)))
        :new-val     (assoc m-int k v)))
    m1 m2))

(defmethod query* "update" [db [_ coll with opts]]
  (db-apply db coll (find-records db coll opts)
    (fn [[int-rmap int-imap] old-record]
      (let [new-record (update-merge old-record with)]
        [(rmap-update int-rmap old-record new-record)
         (imap-update int-imap old-record new-record)]))))

(defmethod query* "delete" [db [_ coll opts]]
  (if (empty? opts)
    [(assoc db coll
       {:rmap {}
        :imap (reduce
                (fn [int-imap ispec]
                  (assoc int-imap ispec (empty-index ispec)))
                {}
                (coll-ispecs db coll))})
     (count (get-in db [coll :rmap]))]
    (db-apply db coll (find-records db coll opts)
      (fn [[int-rmap int-imap] old-record]
        [(rmap-delete int-rmap old-record)
         (imap-delete int-imap old-record)]))))

(defmethod query* "explain" [db [_ [query-type coll e3 e4]]]
  (make-plan coll (coll-ispecs db coll)
    (if (= "update" query-type) e4 e3)))

(defmethod query* "list-collections" [db _]
  (or (map first (filter
                   (fn [[coll {rmap :rmap imap :imap}]]
                     (or (not (empty? rmap)) (not (empty? imap))))
                   db))
      (list)))

(defmethod query* "drop-collection" [db [_ coll]]
  (if (get db coll)
    [(dissoc db coll) 1]
    [db 0]))

(defmethod query* "create-index" [db [_ coll ispec]]
  (if (get-in db [coll :imap ispec])
    [db 0]
    (let [records (vals (get-in db [coll :rmap]))
          index   (index-build records ispec)]
      [(assoc-in db [coll :imap ispec] index) 1])))

(defmethod query* "drop-index" [db [_ coll ispec]]
  (if-not (get-in db [coll :imap ispec])
    [db 0]
    [(core/dissoc-in db [coll :imap ispec]) 1]))

(defmethod query* "list-indexes" [db [_ coll]]
  (or (keys (get-in db [coll :imap])) (list)))

(defmethod query* "multi-read" [db [_ queries]]
  (map #(query* db %) queries))

(defmethod query* "multi-write" [db [_ queries]]
  (reduce
    (fn [[int-db int-results] q]
      (if (types/read-queries (first q))
        [int-db (conj int-results (query* int-db q))]
        (let [[aug-db result] (query* int-db q)]
          [aug-db (conj int-results result)])))
    [db []]
    queries))

(defmethod query* "checked-write" [db [_ check expected write]]
  (let [actual (query* db check)]
    (if (= actual expected)
      (let [[new-db result] (query* db write)]
        [new-db [true result]])
      [db [false actual]])))

(defmethod query* "clear" [db [_ coll ispec]]
  [(init) (count (query* db ["list-collections"]))])

(defn query [db q]
  (lint/lint-query q)
  (query* db q))
