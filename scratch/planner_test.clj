(set! *warn-on-reflection* true)

(use 'fleetdb.core)

(defn assert-plan [ispecs opts expected-plan]
  (let [{:keys [where order offset limit only]} opts
        found-plan (find-plan ispecs where order offset limit only)]
    (if (= found-plan expected-plan)
      (prn :pass)
      (do
        (prn)
        (prn :fail)
        (prn ispecs)
        (prn opts)
        (prn found-plan)
        (prn expected-plan)
        (prn)))))

(assert-plan
  nil
  nil
  [:rmap-scan])

(assert-plan
  nil
  {:where [:= :name "mark"]}
  [:filter [:= :name "mark"]
    [:rmap-scan]])

(assert-plan
  nil
  {:where [:= :name "mark"] :order [[:age :asc]]
   :limit 3 :offset 6 :only [:name :age]}
   [:only [:name :age]
     [:limit 3
       [:offset 6
         [:sort [[:age :asc]]
           [:filter [:= :name "mark"]
             [:rmap-scan]]]]]])

(assert-plan
  nil
  {:where [:= :id "abc"]}
  [:rmap-lookup "abc"])

(assert-plan
  nil
  {:where [:in :id ["abc" "def" "ghi"]] :order [[:age :asc]]}
  [:sort [[:age :asc]]
    [:rmap-multilookup ["abc" "def" "ghi"]]])

(assert-plan
  nil
  {:where [:or [:= :name "mark"] [:= :id "abc"]]}
  [:union nil
    [[:filter [:= :name "mark"]
       [:rmap-scan]]
     [:rmap-lookup "abc"]]])

(assert-plan
  `([[:name :asc]])
  {:where [:= :age 23]}
  [:filter [:= :age 23]
    [:rmap-scan]])

(assert-plan
  `([[:name :asc]])
  {:where [:= :name "mark"]}
  [:index-lookup [[[:name :asc]] "mark"]])

(assert-plan
  `([[:age :asc]] [[:name :asc]])
  {:where [:= :name "mark"]}
  [:index-lookup [[[:name :asc]] "mark"]])

(assert-plan
  `([[:age :asc]])
  {:where [:> :age 23]}
  [:index-seq [[[:age :asc]] :left-right 23 false :pos-inf true]])

(assert-plan
  `([[:age :dsc]])
  {:where [:> :age 23]}
  [:index-seq [[[:age :dsc]] :left-right :pos-inf true 23 false]])

(assert-plan
  '([[:age :asc]] [[:name :asc] [:age :asc]])
  {:where [:and [:= :name "mark"] [:> :age 23]]}
  [:index-seq [[[:name :asc] [:age :asc]] :left-right ["mark" 23] false ["mark" :pos-inf] true]])

(assert-plan
  '([[:age :asc]])
  {:order [[:age :asc]]}
  [:index-seq [[[:age :asc]] :left-right :neg-inf true :pos-inf true]])

(assert-plan
  '([[:age :dsc]])
  {:order [[:age :asc]]}
  [:index-seq [[[:age :dsc]] :right-left :pos-inf true :neg-inf true]])

(assert-plan
  `([[:name :asc] [:age :asc]])
  {:where [:= :age 23]}
  [:filter [:= :age 23]
    [:rmap-scan]])

(assert-plan
  `([[:name :asc] [:age :asc]])
  {:where [:= :age 23] :order [[:name :asc]]}
  [:filter [:= :age 23]
    [:index-seq [[[:name :asc] [:age :asc]] :left-right [:neg-inf :neg-inf] true [:pos-inf :pos-inf] true]]])

(assert-plan
  `([[:name :asc] [:age :asc]])
  {:where [:= :name "mark"] :order [[:age :asc]]}
  [:index-seq [[[:name :asc] [:age :asc]] :left-right ["mark" :neg-inf] true ["mark" :pos-inf] true]])

(assert-plan
  '([[:age :asc]] [[:name :asc] [:age :asc]] [[:name :asc] [:age :asc] [:height :dsc]])
  {:where [:and [:= :name "mark"] [:= :age 23]] :order [[:height :asc]]}
  [:index-seq [[[:name :asc] [:age :asc] [:height :dsc]] :right-left ["mark" 23 :pos-inf] true ["mark" 23 :neg-inf] true]])

(assert-plan
  `([[:age :asc]])
  {:where [:and [:= :age 23] [:> :height 70] [:!= :gender :male]]}
  [:filter [:and [:> :height 70] [:!= :gender :male]]
    [:index-lookup [[[:age :asc]] 23]]])

(defn bench1 [n]
  (let [ispecs '([[:age :asc]] [[:name :asc] [:age :asc]] [[:name :asc] [:age :asc] [:height :dsc]])
        where  [:and [:= :name "mark"] [:= :age 23]]
        order  [[:height :asc]]
        offset 4
        limit  2
        only   [:name :age :height]]
  (dotimes [i n]
    (find-plan ispecs where order offset limit only))))

(defn bench2 [n]
  (let [where [:= :id "abc"]]
    (dotimes [i n]
      (find-plan nil where nil nil nil nil))))

(when false
  (bench1 100000)
  (time (bench1 100000))
  (bench2 100000)
  (time (bench2 100000)))
