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

(deftest "select: no coll"
  (assert= [] (core/query db1 ["select" "foos"])))

(deftest "select: full coll"
  (assert-set= records (core/query db1 ["select" "elems"])))

(deftest "select: ad-hoc single attr sort"
  (assert= [r1 r3 r2 r6 r4 r5]
           (core/query db1 ["select" "elems" {"order" ["lt" "asc"]}])))

(deftest "select: ad-hoc non-abbreviated single attr sort"
  (assert= [r1 r3 r2 r6 r4 r5]
           (core/query db1 ["select" "elems" {"order" [["lt" "asc"]]}])))

(deftest "select: ad-hoc multi attr sort"
  (assert= [r2 r3 r1 r5 r4 r6]
           (core/query db1 ["select" "elems"
                             {"order" [["tp" "asc"] ["num" "desc"]]}])))

(deftest "select: sort, offset, and limit"
  (assert= [r2 r6 r4]
           (core/query db1
              ["select" "elems" {"order" ["lt" "asc"] "offset" 2 "limit" 3}])))

(deftest "select: by id"
  (assert= [r4]
           (core/query db1 ["select" "elems" {"where" ["=" "id" 4]}])))

(deftest "select: by ids"
  (assert-set= [r4 r2]
                 (core/query db1 ["select" "elems" {"where" ["in" "id" [4 100 2]]}])))

(deftest "select: by simple ad-hoc pred"
  (assert-set= [r4]
                 (core/query db1 ["select" "elems" {"where" ["=" "lt" "e"]}])))

(deftest "select: by and predicate"
  (assert-set= [r2 r3]
               (core/query db1 ["select" "elems"
                                 {"where" ["and" ["=" "tp" "a"] [">=" "num" 2]]}])))

(deftest "select: by or predicate"
  (assert= [r1 r3 r4]
           (core/query db1 ["select" "elems"
                             {"where" ["or" ["=" "lt" "a"] ["=" "num" 2]]}])))

(deftest "select: only with array"
  (assert= [["e"] ["c"]]
           (core/query db1
             ["select" "elems" {"where" ["in" "id" [4 2]] "only" ["lt"]}])))

(deftest "select: only with element"
  (assert= ["e" "c"]
           (core/query db1
             ["select" "elems" {"where" ["in" "id" [4 2]] "only" "lt"}])))

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

(defn assert-find [opts plan1-expected & [plan2-expected]]
  (let [plan1-actual (core/query db1 ["explain" ["select" "elems" opts]])
        plan2-actual (core/query db2 ["explain" ["select" "elems" opts]])]
    (assert= plan1-expected plan1-actual)
    (assert= (or plan2-expected plan1-expected) plan2-actual)
    (let [res1-actual (core/query db1 ["select" "elems" opts])
          res2-actual (core/query db2 ["select" "elems" opts])]
      (if (get opts "order")
        (assert= res1-actual res2-actual)
        (assert-set= res1-actual res2-actual)))))

(deftest "find: no conditions"
  (assert-find nil
    ["collection-scan" "elems"]))

(deftest "find: one id"
  (assert-find {"where" ["=" "id" 2]}
    ["collection-lookup" ["elems" 2]]))

(deftest "find: many ids"
  (assert-find {"where" ["in" "id" [2 3 4]]}
    ["collection-multilookup" ["elems" [2 3 4]]]))

(deftest "find: many ids with order"
  (assert-find
    {"where" ["in" "id" [2 3 4]] "order" ["lt" "asc"]}
    ["sort" ["lt" "asc"]
      ["collection-multilookup" ["elems" [2 3 4]]]]))

(deftest "find: many opts"
  (assert-find
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

(deftest "find: union"
   (assert-find {"where" ["or" ["=" "lt" "a"] ["=" "num" 2]] "order" ["tp" "asc"]}
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

(deftest "find: attr equality"
  (assert-find {"where" ["=" "lt" "a"]}
    ["filter" ["=" "lt" "a"]
      ["collection-scan" "elems"]]
    ["index-lookup" ["elems" "lt" "a"]]))

(deftest "find: attr range"
  (assert-find {"where" [">" "lt" "c"]}
    ["filter" [">" "lt" "c"]
      ["collection-scan" "elems"]]
    ["index-range" ["elems" "lt" "left-right" "c" false :pos-inf true]]))

(deftest "find: attr equality when index obscured"
  (assert-find {"where" ["=" "tp" "a"]}
    ["filter" ["=" "tp" "a"]
      ["collection-scan" "elems"]]))

(deftest "find: multi-attr equality"
  (assert-find {"where" ["and" ["=" "num" 2] ["=" "tp" "a"]]}
    ["filter" ["and" ["=" "num" 2] ["=" "tp" "a"]]
      ["collection-scan" "elems"]]
    ["index-lookup" ["elems" ["num" "tp"] [2 "a"]]]))

(deftest "find: multi-attr range"
  (assert-find {"where" ["and" ["=" "num" 2] [">=" "tp" "a"]]}
    ["filter" ["and" ["=" "num" 2] [">=" "tp" "a"]]
      ["collection-scan" "elems"]]
    ["index-range" ["elems" ["num" "tp"]
                   "left-right" [2 "a"] true [2 :pos-inf] true]]))

(deftest "find: index order left right"
  (assert-find {"order" ["lt" "asc"]}
    ["sort" ["lt" "asc"]
      ["collection-scan" "elems"]]
    ["index-range" ["elems" "lt" "left-right" :neg-inf true :pos-inf true]]))

(deftest "find: index order right left"
  (assert-find {"order" ["lt" "desc"]}
    ["sort" ["lt" "desc"]
      ["collection-scan" "elems"]]
    ["index-range" ["elems" "lt" "right-left" :neg-inf true :pos-inf true]]))

(deftest "find: index order trailing attrs"
  (assert-find {"where" ["=" "tp" "a"] "order" ["num" "desc"]}
    ["sort" ["num" "desc"]
      ["filter" ["=" "tp" "a"]
        ["collection-scan" "elems"]]]
    ["filter" ["=" "tp" "a"]
      ["index-range" ["elems" ["num" "tp"] "right-left" [:neg-inf :neg-inf] true [:pos-inf :pos-inf] true]]]))

(deftest "find: index lookup and order"
  (assert-find {"where" ["=" "num" 2] "order" ["tp" "desc"]}
    ["sort" ["tp" "desc"]
      ["filter" ["=" "num" 2]
        ["collection-scan" "elems"]]]
    ["index-range" ["elems" ["num" "tp"] "right-left" [2 :neg-inf] true [2 :pos-inf] true]]))

(deftest "find: index lookup with remnant filter"
  (assert-find {"where" ["and" [">" "lt" "b"] ["=" "tp" "a"]]}
    ["filter" ["and" [">" "lt" "b"] ["=" "tp" "a"]]
      ["collection-scan" "elems"]]
    ["filter" ["=" "tp" "a"]
      ["index-range" ["elems" "lt" "left-right" "b" false :pos-inf true]]]))

(deftest "find: index most useful"
  (assert-find {"where" ["and" ["=" "lt" "a"] ["=" "num" 1] ["=" "tp" "a"]]}
    ["filter" ["and" ["=" "lt" "a"] ["=" "num" 1] ["=" "tp" "a"]]
      ["collection-scan" "elems"]]
    ["filter" ["=" "lt" "a"]
      ["index-lookup" ["elems" ["num" "tp"] [1 "a"]]]]))

(deftest "count: empty"
  (assert= 0 (core/query db1 ["count" "foos"])))

(deftest "count: nonempty"
  (assert= 6 (core/query db1 ["count" "elems"])))

(deftest "count: qualified"
  (assert= 3 (core/query db1 ["count" "elems" {"limit" 3}])))

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

(deftest "delete"
  (let [[db1-1 c] (core/query db1 ["delete" "elems" {"where" ["in" "id" [2 4]]}])]
    (assert= 2 c)
    (assert-set= [r1 r3 r5 r6]
                 (core/query db1-1 ["select" "elems"]))))

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
    (assert= [] (core/query db1-2 ["list-indexes" "elems"]))))

(deftest "list-indexes: none"
  (assert= [] (core/query db1 ["list-indexes" "foos"])))

(deftest "list-indexes: some"
  (let [[db1-1 _] (core/query db1   ["create-index" "elems" "name"])
        [db1-2 _] (core/query db1-1 ["create-index" "elems" ["age" "height"]])
        [db1-3 _] (core/query db1-2 ["create-index" "elems" [["age" "desc"] ["height" "asc"]]])]
    (assert= (set ["name" ["age" "height"] [["age" "desc"] ["height" "asc"]]])
             (set (core/query db1-3 ["list-indexes" "elems"])))))
