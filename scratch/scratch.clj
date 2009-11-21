(defn compact [m]
  (reduce
    (fn [m-int [k v]]
      (if (nil? v) (dissoc m-int k) m-int))
    m m))

{:action  :limit
 :options {:limit 5}
 :order   [:age :asc]
 :size    :db
 :cost    [:scan :range],
 :source
   {:action  :offset
    :options {:offset 10}
    :order   [:age :asc]
    :size    :db
    :cost    [:scan :range]
    :source
      {:action  :sort
       :options {:order [:age :asc]}
       :order   [:age :asc]
       :size    :db
       :cost    [:sort :db]
       :source
         {:action :db-scan
          :order  [:id :asc]
          :size   :db
          :cost   [:scan :db]}}}}

(defn symbolic-costs [{:keys [source] :as plan}]
  (cons (:cost plan)
    (cond
      (nil? source) nil
      (map? source) (symbolic-costs source)
      :else         (apply concat (map symbolic-costs source)))))

(or (count-records db opts)
    (count (find-records db opts)))

(defn greatest [coll]
  (if (seq coll)
    (apply max coll)))

(defn least-by [key-fn coll]
  (if (seq coll)
    (reduce
      (fn [mem elem]
        (if (< (key-fn elem) (key-fn mem)) elem mem))
      coll)))

;(let [a (make-array Object 7)]
  ;(aset a 0 (+ x 10000000))
  ;(aset a 1 :created_at)
  ;(aset a 2 (+ x 20000000))
  ;(aset a 3 :updated_at)
  ;(aset a 4 (+ x 30000000))
  ;(aset a 5 :title)
  ;;(aset a 6 (str x "is the best number ever"))
  ;a)))

(defn mem-usage [f]
  (f)
  (used-mem)
  (gc)
  (let [m0 (used-mem)
        e  (f)]
    (gc)
    (- (used-mem) m0)))

    (let [baos (ByteArrayOutputStream.)
          dos  (DataOutputStream. baos)]
      (Serializer/serialize dos :foo)
      (let [bytes (.toByteArray baos)
            bais  (ByteArrayInputStream. bytes)
            dis   (DataInputStream. bais)]
        (prn (Serializer/deserialize dis :eof))
        (prn (Serializer/deserialize dis :eof))))

        obj {"where" ["and" ["=" "name" "mark"] [">" "age" 20]] "order" [["a" "asc"] ["b" "dsc"]] "limit" 3 "only" ["a" "b" "c"]}

        ;obj {:where [:and [:= :name "mark"] [:> :age 20]] :order [[:a :asc] [:b :dsc]] :limit 3 :only [:a :b :c]}

(prn (alength (serialize object)))


{:id => 1000, :created_at => 1001, :updated_at => 1002,
 :author_id => 1003, :version => 1005, :posted_at => 1006,
 :title => "3 is the best number ever",
 :slug => "3-is-the-best-number-ever",
 :body => "3 is the best number ever. I say so so its true. That is all."})

(reduce
   (fn [int-imap [ispec index]] (assoc int-imap ispec (apply-fn (ispec-on-fn ispec) index)))
   {}
   imap))

(prn
  (:imap (first
    (query
      (first (query (init) [:insert {:records (records 2)}]))
      [:create-index {:on [[:created-at :asc]]}]))))

(quit)

(defn prn-plan
  ([plan]
   (prn-plan plan 2)
   (printf "\n"))
  ([[action opts source] indent]
     (printf "[%s" action)
     (when opts
       (printf " %s" (pr-str opts)))
     (cond
        ;(= :union action)
        ;  (do (printf "\n")
        ;      (printf (apply str (repeat indent " ")))
        ;      (printf "[")
        ;      (doseq [subplan source]
        ;        (printf "\n")
        ;        (printf (apply str (repeat indent " ")))
        ;        (prn-plan subplan (+ indent 3)))
        ;      (printf "]"))
        action
          (do (printf "\n")
              (printf (apply str (repeat indent " ")))
              (when source
                (prn-plan source (+ indent 2)))))
     (printf "]")))

     (prn (query* built [:explain {:query [:select {:where [:>=<= :created-at [20000010 20000020]]}]}]))
      (prn (query* built [:select {:where [:>=<= :created-at [20000010 20000020]]}]))


      (use 'fleetdb.core)

      (def db (init))

      (def records
        (for [x (range 100)]
          {:id x :age (+ 23 x)}))

      (prn "building records")
      (dorun records)

      ; (prn "folding records")
      ; (dotimes [n 10]
      ;   (time
      ;     (reduce
      ;       #(assoc %1 (:id %2) %2)
      ;       (sorted-map)
      ;       records)))

      (prn "building db")
      (def db (first (query db [:insert {:records records}])))

      ; (prn "selecting")
      ; (prn (query db [:select {:order [:age :asc] :limit 5 :offset 10}]))

      ; (prn "explaining")
      ; (prn (query db [:explain {:query [:select {:order [:age :asc] :limit 5 :offset 10}]}]))

      ;(prn "inserting")
      ; (dotimes [n 10]
      ;   (time
      ;     (reduce
      ;       #(first (query%1 [:insert {:records [%2]}]))
      ;       db
      ;       records)))

      ; (prn "building db")
      ; (def db (first (query db [:insert {:records records}])))

      ; (prn "creating index")
      ; (def db (first (query db [:create-index {:on :name}])))

      ; (prn "dropping index")
      ; (def db (first (query db [:drop-index {:on :name}])))

      ; (prn "listing indexes")
      ; (prn (query db [:list-indexes]))
      ; 
      ; (prn)
      ; (prn db)

      ; (def db (first (query db [:update {:where [:= :id 2] :with {:age 25}}])))
      ; (def db (first (query db [:update {:where [:= :id 3] :with {:name "Mark the Shark"}}])))
      ; (def db (first (query db [:delete {:where [:= :id 4]}])))
      ; (prn db)

      ;(prn "finding")
      ; (dotimes [n 5]
      ;   (time (dotimes [n 10000] (dorun (query db [:select {:where [:= :age 1000]}])))))

      (prn "selecting")
      (prn (query db [:explain {:query [:select {:where [:= :id 70]}]}]))
      (prn (query db [:select {:where [:= :id 70]}]))
      (prn)

      (prn (query db [:explain {:query [:select {:where [:in :id [70 80 90]]}]}]))
      (prn (query db [:select {:where [:in :id [70 80 90]]}]))
      (prn)

      (prn (query db [:explain {:query [:select {:where [:> :id 70]}]}]))
      (prn (query db [:select {:where [:> :id 70]}]))
      (prn)

      (prn (query db [:explain {:query [:select {:where [:> :age 100] :order [:id :asc]}]}]))
      (prn (query db [:select {:where [:< :age 100] :order [:id :asc]}]))


      ; (prn (query db [:select {:where [:> :age 110]}]))
      ; (prn (query db [:count {:where [:> :age 110]}]))
      ;
      ;(dotimes [n 5]
      ;  (time
      ;    (dotimes [n 50000] (dorun (query db [:select {:where [:> :age 110]}])))))
      ;
      ; (prn "creating index")
      ; (def db (first (query db [:create-index {:on :age}])))
      ;
      ; (prn "selecting")
      ; (prn (query db [:explain {:query [:select {:where [:> :age 110]}]}]))
      ; (prn (query db [:select {:where [:> :age 110]}]))
      ; (prn (query db [:count {:where [:> :age 110]}]))
      ;
      ;(dotimes [n 5]
      ;  (time
      ;    (dotimes [n 50000] (dorun (query db [:select {:where [:> :age 110]}])))))


      ; (prn "selecting")
      ; (dotimes [n 10]
      ;   (time
      ;     (dotimes [n 200000]
      ;       (dorun (query db [:select {:order [:age :dsc] :offset 2 :limit 2}])))))

      ;(prn "multi-reading")
      ;(prn (query db [:multi-read [[:select {:order [:age :asc]}]
      ;                            [:count {:where [:> :id 2]}]]]))

      ; (prn "multi-writing")
      ; (prn (query db [:multi-write [[:insert {:records [{:id 5 :foo :bar}]}]
      ;                              [:insert {:records [{:id 6 :biz :bat}]}]]]))

      ; (prn "checked multi-writing")
      ; (prn (query db [:checked-write {:check  [:count]
      ;                                :expect 6
      ;                                :write  [:insert {:records [{:id 5 :foo :bar}]}]}]))
      ; (prn)
      ; (prn (query db [:checked-write {:check  [:count]
      ;                                :expect 5
      ;                                :write  [:insert {:records [{:id 5 :foo :bar}]}]}]))
      ; (prn "counting")
      ; (dotimes [n 10]
      ;   (time
      ;     (prn (query db [:count {:where [:= :id 100]}]))))

      ; (prn "updating")
      ; (def db (query db [:update {:where [:= :id 5] :with {:name "Frank" :grandpop true}}]))
      ; (prn db)

      ; (prn "deleting")
      ; (def db (query db [:delete {:where [:in :id [0 2 4 6 8]]}]))
      ; (prn db)

      {:order [[:a :asc] [:b :dsc]] :limit 3 :only [:a :b :c]}

      [:only [:a :b :c]
        [:limit 3
          [:sort [[:a :asc] [:b :dsc]]
            [:rmap-scan]]]]

      (map #(select-keys % [:a :b :c])
        (take 3
          (sort (index-compare [[:a :asc]])
            (vals rmap))))


      (def index1
        (sorted-map-by #(compare* %1 %2) 19 :a 20 :b 21 :c 22 :d 23 :e))

      (def index2
        (sorted-map-by #(compare* %2 %1) 19 :a 20 :b 21 :c 22 :d 23 :e))

      [[:age :asc]] {:where [:> :age 21]}
      [_ :left-right 21 false pos-inf true]

      [[:age :asc]] {:where [:< :age 21]}
      [_ :left-right neg-inf true 21 false]

      [[:age :asc]] {:where [:>=< :age [21 30]]}
      [_ :left-right 21 true 30 false]


      [[:age :dsc]] {:where [:> :age 21]}
      [_ :right-left pos-inf true 21 false]

      [[:age :dsc]] {:where [:< :age 21]}
      [_ :left-right 21 false neg-inf true]

      [[:age :dsc]] {:where [:>=< :age [21 30]]}
      [_ :right-left 30 false 21 true]