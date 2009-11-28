(ns fleetdb.core-test
  (:require (fleetdb [core :as core]))
  (:use (clj-unit core) (fleetdb test-util [util :only (def-)])))

(def- db1
  (let [coll    :elems
        records [{:id 1 :lt "a"} {:id 2 :lt "c"} {:id 3 :lt "b"}
                 {:id 4 :lt "e"} {:id 5 :lt "f"} {:id 6 :lt "d"}]
        empty   (core/init)]
    (first (core/query empty [:insert coll records]))))

(def- db2
  (let [coll    :elems
        records [{:id 1 :lt "a" :num 1 :tp :a}
                 {:id 2 :lt "c" :num 4 :tp :a}
                 {:id 3 :lt "a" :num 2 :tp :a}
                 {:id 4 :lt "d" :num 2 :tp :b}]
        ispecs  [[[:lt :asc]] [[:num :asc] [:tp :asc]]]
        empty   (core/init)
        with-rs (first (core/query empty [:insert coll records]))
        with-is (reduce #(first (core/query %1 [:create-index coll %2]))
                        with-rs
                        ispecs)]
    with-is))

(deftest "init"
  (assert= {:rmaps {} :imaps {}} (core/init)))

(deftest "select: no coll"
  (assert-nil (core/query db1 [:select :foos])))

(deftest "select: full coll"
  (assert= (set (range 1 7))
           (set (map :id (core/query db1 [:select :elems])))))

(deftest "select: ad-hoc sort"
  (assert= [1 3 2 6 4 5]
           (map :id (core/query db1 [:select :elems {:order [[:lt :asc]]}]))))

(deftest "select: sort, offset, and limit"
  (assert= [2 6 4]
           (map :id (core/query db1
              [:select :elems {:order [[:lt :asc]] :offset 2 :limit 3}]))))

(deftest "select: by id"
  (assert= [{:id 4 :lt "e"}]
           (core/query db1 [:select :elems {:where [:= :id 4]}])))

(deftest "select: by ids"
  (assert= [{:id 4 :lt "e"} {:id 2 :lt "c"}]
           (core/query db1 [:select :elems {:where [:in :id [4 100 2]]}])))

(deftest "select: by simple ad-hoc pred"
  (assert= [{:id 4 :lt "e"}]
           (core/query db1 [:select :elems {:where [:= :lt "e"]}])))

(deftest "select: only"
  (assert= [{:lt "e"} {:lt "c"}]
           (core/query db1
             [:select :elems {:where [:in :id [4 2]] :only [:lt]}])))

(deftest "select: simple index get"
  (assert-set= [1 3] (map :id (core/query db2
                                [:select :elems {:where [:= :lt "a"]}]))))

(deftest "select: simple index range"
  (assert-set= [2 4] (map :id (core/query db2
                                [:select :elems {:where [:>= :lt "c"]}]))))

(deftest "select: compound index get"
  (assert-set= [3]
               (map :id (core/query db2
                 [:select :elems {:where [:and [:= :num 2] [:= :tp :a]]}]))))

(deftest "select: compound index range"
  (assert-set= [3 4]
               (map :id (core/query db2
                 [:select :elems {:where [:and [:= :num 2] [:>= :tp :a]]}]))))

(deftest "get: no coll"
  (assert-nil (core/query db1 [:get :foos 7])))

(deftest "get: present"
  (assert-set= [1 3] (map :id (core/query db1 [:get :elems [1 3]]))))

(deftest "get: not present"
  (assert-nil (core/query db1 [:get :elems 100])))

(deftest "count: empty"
  (assert= 0 (core/query db1 [:count :foos])))

(deftest "count: nonempty"
  (assert= 6 (core/query db1 [:count :elems])))

(deftest "count: qualified"
  (assert= 3 (core/query db1 [:count :elems {:limit 3}])))

(deftest "insert: one"
  (let [elem        {:id 7 :lt "g"}
        [new-db1 c] (core/query db1 [:insert :elems elem])]
    (assert= 1 c)
    (assert= elem (core/query new-db1 [:get :elems 7]))))

(deftest "insert: multiple"
  (let [elems       [{:id 7 :lt "g"} {:id 8 :lt "h"}]
        [new-db1 c] (core/query db1 [:insert :elems elems])]
    (assert= 2 c)
    (assert= (first elems) (core/query new-db1 [:get :elems 7]))))

(deftest "insert: duplicate id"
  (assert-throws #"id"
    (core/query db1 [:insert :elems {:id 2 :lt "f"}])))

(deftest "insert: new coll"
  (let [foo         {:id 7 :lt "f"}
        [new-db1 c] (core/query db1 [:insert :foos foo])]
    (assert= foo (core/query new-db1 [:get :foos 7]))))

(deftest "update"
  (let [[db1-1 c] (core/query db1
                    [:update :elems {:mr "+"} {:where [:in :id [2 4]]}])]
    (assert= 2 c)
    (assert= ["+" nil] (map :mr
                         (core/query db1-1
                           [:select :elems {:where [:in :id [2 3]]}])))))

(deftest "delete"
  (let [[db1-1 c] (core/query db1 [:delete :elems {:where [:in :id [2 4]]}])]
    (assert= 2 c)
    (assert= (set [1 3 5 6])
             (set (map :id (core/query db1-1 [:select :elems]))))))

(deftest "list-indexes: none"
  (assert-nil (core/query db1 [:list-indexes :foos])))

(deftest "create-/drop-index"
  (let [[db1-1 _] (core/query db1   [:create-index :elems [[:name :asc]]])
        [db1-2 _] (core/query db1-1 [:drop-index   :elems [[:name :asc]]])]
    (assert= [[[:name :asc]]] (core/query db1-1 [:list-indexes :elems]))
    (assert-nil (core/query db1-2 [:list-indexes :elems]))))

(deftest "list-indexes: some"
  (let [[db1-1 _] (core/query db1   [:create-index :elems [[:name :asc]]])
        [db1-2 _] (core/query db1-1 [:create-index :elems [[:age :desc]]])]
    (assert= (set [[[:name :asc]] [[:age :desc]]])
             (set (core/query db1-2 [:list-indexes :elems])))))

(deftest "multi-read"
  (assert= [1 [{:id 4, :lt "e"} {:id 2, :lt "c"}]]
           (core/query db1
             [:multi-read
               [[:count :elems {:where [:= :lt "e"]}]
                [:select :elems {:where [:in :id [4 2]]}]]])))

(deftest "multi-write"
  (let [elems [{:id 7 :lt "g"} {:id 8 :lt "h"}]
        elem  (first elems)
        [new-db1 [i-result d-result]]
          (core/query db1
            [:multi-write [[:insert :elems elems]
                           [:delete :elems {:where [:= :id 6]}]]])]
    (assert= 2 i-result)
    (assert= 1 d-result)
    (assert= elem (core/query new-db1 [:get :elems (:id elem)]))
    (assert-not   (core/query new-db1 [:get :elems 6]))))

(deftest "checked-write: fail"
  (let [[new-db1 [pass actual]]
          (core/query db1 [:checked-write [:count :elems] 7
                                          [:insert :elems {:id 8 :lt "h"}]])]
    (assert= db1 new-db1)
    (assert-not pass)
    (assert= actual 6)))

(deftest "checked-write: pass"
  (let [elem               {:id 8 :lt "h"}
        [new-db1 [pass result]]
           (core/query db1 [:checked-write [:count :elems] 6
                                           [:insert :elems elem]])]
     (assert-that pass)
     (assert= result 1)
     (assert= elem (core/query new-db1 [:get :elems 8]))))

(deftest "list-colls"
  (let [[new-db1 _] (core/query db1 [:create-index :foos [[:name :asc]]])]
    (assert= (set [:elems :foos]) (core/query new-db1 [:list-colls]))))

(deftest "explain: non-select"
  (assert-throws #"select"
    (core/query db1 [:explain [:list-indexes :elems]])))

(deftest "explain: select"
  (assert= [:rmap-lookup [:elems 3]]
           (core/query db1 [:explain [:select :elems {:where [:= :id 3]}]])))
