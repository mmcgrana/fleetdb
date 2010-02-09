(ns fleetdb.core-test
  (:require (fleetdb [core :as core]))
  (:use (clj-unit core) (fleetdb test-util [util "only" (def- defmacro-)])))

(defn- db-with [coll records ispecs]
  (let [empty (core/init)
        with-records (first (core/query empty ["insert" coll records]))
        with-indexes (reduce #(first (core/query %1 ["create-index" coll %2]))
                             with-records
                             ispecs)]
    with-indexes))

(def- r1 {"id" 1 "lt" "a" "num" 1 "tp" "a"})
(def- r2 {"id" 2 "lt" "c" "num" 4 "tp" "a"})
(def- r3 {"id" 3 "lt" "b" "num" 2 "tp" "a"})
(def- r4 {"id" 4 "lt" "e" "num" 2 "tp" "b"})
(def- r5 {"id" 5 "lt" "f" "num" 3 "tp" "b"})
(def- r6 {"id" 6 "lt" "d" "num" 3 "tp" "c"})

(def- records [r1 r2 r3 r4 r5 r6])

(def- db1 (db-with "elems" records nil))

(def- db2 (db-with "elems" records ["lt" ["num" "tp"]]))

(defn assert-find [select-result db coll & [select-opts]]
  (let [select-assert (if (set? select-result) assert-set= assert=)]
    (select-assert select-result
                   (core/query db ["select" coll select-opts]))
    (assert= (count select-result)
             (core/query db ["count" coll (dissoc select-opts "only")]))))

(deftest "find: no coll"
  (assert-find [] db1 "foos"))

(deftest "find: full coll"
  (assert-find (set records) db1 "elems"))

(deftest "find: ad-hoc single attr sort"
  (assert-find [r1 r3 r2 r6 r4 r5] db1 "elems" {"order" ["lt" "asc"]}))

(deftest "find: ad-hoc non-abbreviated single attr sort"
  (assert-find [r1 r3 r2 r6 r4 r5] db1 "elems" {"order" [["lt" "asc"]]}))

(deftest "find: ad-hoc multi attr sort"
  (assert-find [r2 r3 r1 r5 r4 r6]
               db1 "elems" {"order" [["tp" "asc"] ["num" "desc"]]}))

(deftest "find: sort, offset, and limit"
  (assert-find [r2 r6 r4]
               db1 "elems" {"order" ["lt" "asc"] "offset" 2 "limit" 3}))

(deftest "find: by id"
  (assert-find [r4] db1 "elems" {"where" ["=" "id" 4]}))

(deftest "find: by ids"
  (assert-find #{r4 r2} db1 "elems" {"where" ["in" "id" [4 100 2]]}))

(deftest "find: not by ids"
	(assert-find #{r4 r2} db1 "elems" {"where" ["not in" "id" [1 3 5 6]]}))

(deftest "find: by simple ad-hoc pred"
  (assert-find [r4] db1 "elems" {"where" ["=" "lt" "e"]}))

(deftest "find: by and predicate"
  (assert-find #{r2 r3}
               db1 "elems" {"where" ["and" ["=" "tp" "a"] [">=" "num" 2]]}))

(deftest "find: by or predicate"
  (assert-find #{r1 r3 r4}
               db1 "elems" {"where" ["or" ["=" "lt" "a"] ["=" "num" 2]]}))

(deftest "find: only with array"
  (assert-find #{["e"] ["c"]}
               db1 "elems" {"where" ["in" "id" [4 2]] "only" ["lt"]}))

(deftest "find: only with element"
  (assert-find #{"e" "c"}
               db1 "elems" {"where" ["in" "id" [4 2]] "only" "lt"}))

(deftest "find: distinct"
  (let [count-occurr (fn [coll] (vals (reduce #(update-in %1 [%2] inc)
                                              {"a" 0 "b" 0 "c" 0}
                                              coll)))]
    (assert-fn #(every? (partial = 1) (count-occurr %))
               (core/query
                 db1 ["select" "elems" {"distinct" true "only" "tp"}]))))

(deftest "find: distinct preserves order (asc)"
  (assert= ["a" "b" "c"]
           (core/query
             db1 ["select" "elems"
                  {"distinct" true "only" "tp" "order" ["id" "asc"]}])))

(deftest "find: distinct preserves order (desc)"
  (assert= ["c" "b" "a"]
           (core/query
             db1 ["select" "elems"
                  {"distinct" true "only" "tp" "order" ["id" "desc"]}])))

(deftest "select: index lookup seqing"
  (let [db2-1 (first (core/query db2 ["insert" "elems" {"id" 7 "lt" "d"}]))]
    (assert-not-fn set? (core/query db2-1
                          ["select" "elems" {"where" ["=" "lt" "d"]}]))))

(deftest "select: raise on mixed attr types"
  (let [db1-1 (first (core/query db1 ["insert" "elems" {"id" 7 "lt" 3}]))]
    (assert-throws #"Cannot compare"
      (core/query db1-1 ["select" "elems" {"order" ["lt" "asc"]}]))))

(deftest "explain: select, count, update, delete"
  (let [coll      "elems"
        find-opts {"where" ["=" "id" 3]}
        queries   [["select" coll find-opts]
                   ["count"  coll find-opts]
                   ["delete" coll find-opts]
                   ["update" coll {"up" true} find-opts]]]
    (doseq [query queries]
      (assert= ["collection-lookup" ["elems" 3]]
               (core/query db1 ["explain" query])))))

(defn assert-plan [opts plan1-expected & [plan2-expected]]
  (let [plan1-actual (core/query db1 ["explain" ["select" "elems" opts]])
        plan2-actual (core/query db2 ["explain" ["select" "elems" opts]])]
    (assert= plan1-expected plan1-actual)
    (assert= (or plan2-expected plan1-expected) plan2-actual)
    (let [res1-actual (core/query db1 ["select" "elems" opts])
          con1-actual (core/query db1 ["count"  "elems" (dissoc opts "only")])
          res2-actual (core/query db2 ["select" "elems" opts])
          con2-actual (core/query db2 ["count"  "elems" (dissoc opts "only")])]
      (if (get opts "order")
        (assert= res1-actual res2-actual)
        (assert-set= res1-actual res2-actual))
      (assert= con1-actual con2-actual))))

(deftest "plan: no conditions"
  (assert-plan nil
    ["collection-scan" "elems"]))

(deftest "plan: one id"
  (assert-plan {"where" ["=" "id" 2]}
    ["collection-lookup" ["elems" 2]]))

(deftest "plan: many ids"
  (assert-plan {"where" ["in" "id" [2 3 4]]}
    ["collection-multilookup" ["elems" [2 3 4]]]))

(deftest "plan: many ids with order"
  (assert-plan
    {"where" ["in" "id" [2 3 4]] "order" ["lt" "asc"]}
    ["sort" ["lt" "asc"]
      ["collection-multilookup" ["elems" [2 3 4]]]]))

(deftest "plan: many opts"
  (assert-plan
    {"where" ["=" "tp" "a"] "order" ["lt" "asc"] "limit" 3 "offset" 6 "only" ["id" "lt"]}
    ["only" ["id" "lt"]
       ["limit" 3
         ["offset" 6
           ["sort" ["lt" "asc"]
             ["filter" ["=" "tp" "a"]
               ["collection-scan" "elems"]]]]]]
    ["only" ["id" "lt"]
       ["limit" 3
         ["offset" 6
           ["filter" ["=" "tp" "a"]
             ["index-range" ["elems" "lt" "left-right" :neg-inf true :pos-inf true]]]]]]))

(deftest "plan: union"
   (assert-plan {"where" ["or" ["=" "lt" "a"] ["=" "num" 2]] "order" ["tp" "asc"]}
     ["union" ["tp" "asc"]
       [["sort" ["tp" "asc"]
          ["filter" ["=" "lt" "a"]
            ["collection-scan" "elems"]]]
        ["sort" ["tp" "asc"]
          ["filter" ["=" "num" 2]
            ["collection-scan" "elems"]]]]]
     ["union" ["tp" "asc"]
       [["sort" ["tp" "asc"]
          ["index-lookup" ["elems" "lt" "a"]]]
        ["index-range" ["elems" ["num" "tp"] "left-right" [2 :neg-inf] true [2 :pos-inf] true]]]]))

(deftest "plan: attr equality"
  (assert-plan {"where" ["=" "lt" "a"]}
    ["filter" ["=" "lt" "a"]
      ["collection-scan" "elems"]]
    ["index-lookup" ["elems" "lt" "a"]]))

(deftest "plan: attr range"
  (assert-plan {"where" [">" "lt" "c"]}
    ["filter" [">" "lt" "c"]
      ["collection-scan" "elems"]]
    ["index-range" ["elems" "lt" "left-right" "c" false :pos-inf true]]))

(deftest "plan: attr equality when index obscured"
  (assert-plan {"where" ["=" "tp" "a"]}
    ["filter" ["=" "tp" "a"]
      ["collection-scan" "elems"]]))

(deftest "plan: multi-attr equality"
  (assert-plan {"where" ["and" ["=" "num" 2] ["=" "tp" "a"]]}
    ["filter" ["and" ["=" "num" 2] ["=" "tp" "a"]]
      ["collection-scan" "elems"]]
    ["index-lookup" ["elems" ["num" "tp"] [2 "a"]]]))

(deftest "plan: multi-attr range"
  (assert-plan {"where" ["and" ["=" "num" 2] [">=" "tp" "a"]]}
    ["filter" ["and" ["=" "num" 2] [">=" "tp" "a"]]
      ["collection-scan" "elems"]]
    ["index-range" ["elems" ["num" "tp"]
                   "left-right" [2 "a"] true [2 :pos-inf] true]]))

(deftest "plan: index order left right"
  (assert-plan {"order" ["lt" "asc"]}
    ["sort" ["lt" "asc"]
      ["collection-scan" "elems"]]
    ["index-range" ["elems" "lt" "left-right" :neg-inf true :pos-inf true]]))

(deftest "plan: index order right left"
  (assert-plan {"order" ["lt" "desc"]}
    ["sort" ["lt" "desc"]
      ["collection-scan" "elems"]]
    ["index-range" ["elems" "lt" "right-left" :neg-inf true :pos-inf true]]))

(deftest "plan: index order trailing attrs"
  (assert-plan {"where" ["=" "tp" "a"] "order" ["num" "desc"]}
    ["sort" ["num" "desc"]
      ["filter" ["=" "tp" "a"]
        ["collection-scan" "elems"]]]
    ["filter" ["=" "tp" "a"]
      ["index-range" ["elems" ["num" "tp"] "right-left" [:neg-inf :neg-inf] true [:pos-inf :pos-inf] true]]]))

(deftest "plan: index lookup and order"
  (assert-plan {"where" ["=" "num" 2] "order" ["tp" "desc"]}
    ["sort" ["tp" "desc"]
      ["filter" ["=" "num" 2]
        ["collection-scan" "elems"]]]
    ["index-range" ["elems" ["num" "tp"] "right-left" [2 :neg-inf] true [2 :pos-inf] true]]))

(deftest "plan: index lookup with remnant filter"
  (assert-plan {"where" ["and" [">" "lt" "b"] ["=" "tp" "a"]]}
    ["filter" ["and" [">" "lt" "b"] ["=" "tp" "a"]]
      ["collection-scan" "elems"]]
    ["filter" ["=" "tp" "a"]
      ["index-range" ["elems" "lt" "left-right" "b" false :pos-inf true]]]))

(deftest "plan: index most useful"
  (assert-plan {"where" ["and" ["=" "lt" "a"] ["=" "num" 1] ["=" "tp" "a"]]}
    ["filter" ["and" ["=" "lt" "a"] ["=" "num" 1] ["=" "tp" "a"]]
      ["collection-scan" "elems"]]
    ["filter" ["=" "lt" "a"]
      ["index-lookup" ["elems" ["num" "tp"] [1 "a"]]]]))

(deftest "insert: one"
  (let [[new-db1 c] (core/query db1 ["insert" "elems" {"id" 7}])]
    (assert= 1 c)
    (assert= [{"id" 7}] (core/query new-db1 ["select" "elems" {"where" ["=" "id" 7]}]))))

(deftest "insert: multiple"
  (let [[new-db1 c] (core/query db1 ["insert" "elems" [{"id" 7} {"id" 8}]])]
    (assert= 2 c)
    (assert= [{"id" 7}] (core/query new-db1 ["select" "elems" {"where" ["=" "id" 7]}]))))

(deftest "insert: no id"
  (assert-throws #"Record does not have an id"
    (core/query db1 ["insert" "elems" {"lt" "g"}])))

(deftest "insert: duplicated existing id"
  (assert-throws #"Duplicated id"
    (core/query db1 ["insert" "elems" {"id" 2}])))

(deftest "insert: duplicated given ids"
  (assert-throws #"Duplicated id"
    (core/query db1 ["insert" "elems" [{"id" 1} {"id" 1}]])))

(deftest "insert: new coll"
  (let [[new-db1 c] (core/query db1 ["insert" "foos" {"id" 7}])]
    (assert= [{"id" 7}] (core/query new-db1 ["select" "foos" {"where" ["=" "id" 7]}]))))

(deftest "update"
  (let [[db1-1 c] (core/query db1
                    ["update" "elems" {"mr" "+"} {"where" ["in" "id" [2 4]]}])]
    (assert= 2 c)
    (assert= ["+" nil] (core/query db1-1
                         ["select" "elems" {"where" ["in" "id" [2 3]]
                                            "only" "mr"}]))))

(deftest "update: new indexed attribute"
  (let [[db2-1 _] (core/query db2 ["insert" "elems" {"id" 7}])]
    (core/query db2-1 ["update" "elems" {"lt" "f"} {"where" ["=" "id" 7]}])))

(deftest "update: inc"
  (let [[db1-1 c] (core/query db1
                    ["update" "elems" {"$inc" {"num" 1}} {"where" ["=" "id" 1]}])]
    (assert= 1 c)
    (assert= [2] (core/query db1-1
                   ["select" "elems" {"where" ["=" "id" 1] "only" "num"}]))))

(deftest "delete"
  (let [[db1-1 c] (core/query db1 ["delete" "elems" {"where" ["in" "id" [2 4]]}])]
    (assert= 2 c)
    (assert-set= [r1 r3 r5 r6]
                 (core/query db1-1 ["select" "elems"]))))

(deftest "delete: all"
  (let [[db2-1 c] (core/query db2 ["delete" "elems"])]
    (assert= 6 c)
    (assert= (list) (core/query db2-1 ["select" "elems"]))
    (assert-set= ["lt" ["num" "tp"]]
                 (core/query db2-1 ["list-indexes" "elems"]))
    (let [[db2-2 _] (core/query db2-1 ["insert" "elems" {"id" "1" "lt" "a"}])]
      (assert= ["a"]
               (core/query db2-2 ["select" "elems"
                                   {"where" [">=" "lt" "a"] "only" "lt"}])))))

(deftest "multi-read"
  (assert= [1 [r4 r2]]
           (core/query db1
             ["multi-read"
               [["count" "elems" {"where" ["=" "lt" "e"]}]
                ["select" "elems" {"where" ["in" "id" [4 2]]}]]])))

(deftest "multi-write"
  (let [elems [{"id" 7 "lt" "g"} {"id" 8 "lt" "h"}]
        elem  (first elems)
        [new-db1 [i-result d-result]]
          (core/query db1
            ["multi-write" [["insert" "elems" elems]
                            ["delete" "elems" {"where" ["=" "id" 6]}]]])]
    (assert= 2 i-result)
    (assert= 1 d-result)
    (assert= [elem] (core/query new-db1 ["select" "elems" {"where" ["=" "id" (elem "id")]}]))
    (assert= []     (core/query new-db1 ["select" "elems" {"where" ["=" "id" 6]}]))))

(deftest "multi-write: some reads"
   (let [elem {"id" 7 "lt" "g"}
         [new-db1 [c-result i-result]]
           (core/query db1
             ["multi-write" [["count" "elems"]
                            ["insert" "elems" elem]]])]
     (assert= 6 c-result)
     (assert= 1 i-result)
     (assert= [elem] (core/query new-db1 ["select" "elems" {"where" ["=" "id" (elem "id")]}]))))

(deftest "checked-write: fail"
  (let [[new-db1 [pass actual]]
          (core/query db1 ["checked-write" ["count" "elems"] 7
                                          ["insert" "elems" {"id" 8 "lt" "h"}]])]
    (assert= db1 new-db1)
    (assert-not pass)
    (assert= actual 6)))

(deftest "checked-write: pass"
  (let [elem               {"id" 8 "lt" "h"}
        [new-db1 [pass result]]
           (core/query db1 ["checked-write" ["count" "elems"] 6
                                           ["insert" "elems" elem]])]
     (assert-that pass)
     (assert= result 1)
     (assert= [elem] (core/query new-db1 ["select" "elems" {"where" ["=" "id" 8]}]))))

(deftest "list-collections: no collections"
  (assert= [] (core/query (core/init) ["list-collections"])))

(deftest "list-collections: records and indexes"
  (let [[db1-1 _] (core/query db1 ["create-index" "foos" "name"])]
    (assert-set= ["elems" "foos"] (core/query db1-1 ["list-collections"]))))

(deftest "drop-collection"
  (let [[db1-1 r1] (core/query db1  ["drop-collection" "elems"])]
    (assert= 1 r1)
    (assert= [] (core/query db1-1 ["list-collections"]))
    (assert= [] (core/query db1-1 ["list-indexes" "elems"]))
    (let [[db1-2 r2] (core/query db1-1 ["drop-collection" "elems"])]
      (assert= 0 r2))))

(deftest "create-/drop-index"
  (let [[db1-1 r1 ] (core/query db1   ["create-index" "elems" "name"])
        [_     r1d] (core/query db1-1 ["create-index" "elems" "name"])
        [db1-2 r2 ] (core/query db1-1 ["drop-index"   "elems" "name"])
        [_     r2d] (core/query db1-2 ["drop-index"   "elems" "name"])]
    (assert= r1  1)
    (assert= r1d 0)
    (assert= r2  1)
    (assert= r2d 0)
    (assert= ["name"] (core/query db1-1 ["list-indexes" "elems"]))
    (assert= (list) (core/query db1-2 ["list-indexes" "elems"]))))

(deftest "list-indexes: none"
  (assert= [] (core/query db1 ["list-indexes" "foos"])))

(deftest "list-indexes: some"
  (let [[db1-1 _] (core/query db1   ["create-index" "elems" "name"])
        [db1-2 _] (core/query db1-1 ["create-index" "elems" ["age" "height"]])
        [db1-3 _] (core/query db1-2 ["create-index" "elems" [["age" "desc"] ["height" "asc"]]])]
    (assert= (set ["name" ["age" "height"] [["age" "desc"] ["height" "asc"]]])
             (set (core/query db1-3 ["list-indexes" "elems"])))))

(deftest "clear"
  (let [[db1-1 _] (core/query db1   ["insert" "foos" {"id" 1}])
        [db1-2 r] (core/query db1-1 ["clear"])]
    (assert= [] (core/query db1-2 ["list-collections"]))
    (assert= 2 r)
    (assert= [] (core/query db1-2 ["select" "elems"]))))
