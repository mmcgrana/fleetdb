(ns fleetdb.planner-test
  (:require (fleetdb [core :as core]))
  (:use (clj-unit core) (clojure.contrib [def :only (defmacro-)])))

(defn- assert-plan [ispecs opts expected-plan]
  (let [db         (reduce
                     #(first (core/query %1 [:create-index :elems %2]))
                     (core/init)
                     ispecs)
        found-plan (core/query db [:explain [:select :elems opts]])]
    (assert= expected-plan found-plan)))

(defmacro- defplantest [label ispecs opts expected]
  `(deftest ~label (assert-plan ~ispecs ~opts ~expected)))

(defplantest "empty"
  nil
  nil
  [:rmap-scan :elems])

(defplantest "filter scanned"
  nil
  {:where [:= :name "mark"]}
  [:filter [:= :name "mark"]
    [:rmap-scan :elems]])

(defplantest "many opts"
  nil
  {:where [:= :name "mark"] :order [[:age :asc]]
   :limit 3 :offset 6 :only [:name :age]}
   [:only [:name :age]
     [:limit 3
       [:offset 6
         [:sort [[:age :asc]]
           [:filter [:= :name "mark"]
             [:rmap-scan :elems]]]]]])

(defplantest "lookup fastpath"
  nil
  {:where [:= :id "abc"]}
  [:rmap-lookup [:elems "abc"]])

(defplantest "multilookup fastpath"
  nil
  {:where [:in :id ["abc" "def" "ghi"]] :order [[:age :asc]]}
  [:sort [[:age :asc]]
    [:rmap-multilookup [:elems ["abc" "def" "ghi"]]]])

(defplantest "union"
  nil
  {:where [:or [:= :name "mark"] [:= :id "abc"]]}
  [:union nil
    [[:filter [:= :name "mark"]
       [:rmap-scan :elems]]
     [:rmap-lookup [:elems "abc"]]]])

(defplantest "index on wrong attr"
  `([[:name :asc]])
  {:where [:= :age 23]}
  [:filter [:= :age 23]
    [:rmap-scan :elems]])

(defplantest "index lookup"
  `([[:age :asc]] [[:name :asc]])
  {:where [:= :name "mark"]}
  [:index-lookup [:elems [[:name :asc]] "mark"]])

(defplantest "index range low to high"
  `([[:age :asc]])
  {:where [:> :age 23]}
  [:index-seq [:elems [[:age :asc]] :left-right 23 false :pos-inf true]])

(defplantest "index range high to low"
  `([[:age :desc]])
  {:where [:> :age 23]}
  [:index-seq [:elems [[:age :desc]] :left-right :pos-inf true 23 false]])

(defplantest "index range multiple attrs"
  '([[:age :asc]] [[:name :asc] [:age :asc]])
  {:where [:and [:= :name "mark"] [:> :age 23]]}
  [:index-seq [:elems [[:name :asc] [:age :asc]] :left-right ["mark" 23] false ["mark" :pos-inf] true]])

(defplantest "index order"
  '([[:age :asc]])
  {:order [[:age :asc]]}
  [:index-seq [:elems [[:age :asc]] :left-right :neg-inf true :pos-inf true]])

(defplantest "index order right-left"
  '([[:age :desc]])
  {:order [[:age :asc]]}
  [:index-seq [:elems [[:age :desc]] :right-left :pos-inf true :neg-inf true]])

(defplantest "index on obscured attr"
  `([[:name :asc] [:age :asc]])
  {:where [:= :age 23]}
  [:filter [:= :age 23]
    [:rmap-scan :elems]])

(defplantest "index order with trailing attrs"
  `([[:name :asc] [:age :asc]])
  {:where [:= :age 23] :order [[:name :asc]]}
  [:filter [:= :age 23]
    [:index-seq [:elems [[:name :asc] [:age :asc]] :left-right [:neg-inf :neg-inf] true [:pos-inf :pos-inf] true]]])

(defplantest "index lookup and order"
  `([[:name :asc] [:age :asc]])
  {:where [:= :name "mark"] :order [[:age :asc]]}
  [:index-seq [:elems [[:name :asc] [:age :asc]] :left-right ["mark" :neg-inf] true ["mark" :pos-inf] true]])

(defplantest "index most useful"
  '([[:age :asc]] [[:name :asc] [:age :asc]] [[:name :asc] [:age :asc] [:height :desc]])
  {:where [:and [:= :name "mark"] [:= :age 23]] :order [[:height :asc]]}
  [:index-seq [:elems [[:name :asc] [:age :asc] [:height :desc]] :right-left ["mark" 23 :pos-inf] true ["mark" 23 :neg-inf] true]])

(defplantest "index lookup with remnant filter"
  `([[:age :asc]])
  {:where [:and [:= :age 23] [:> :height 70] [:!= :gender :male]]}
  [:filter [:and [:> :height 70] [:!= :gender :male]]
    [:index-lookup [:elems [[:age :asc]] 23]]])
