(ns fleetdb.lint
  (:require (fleetdb [types :as types]))
  (:use (fleetdb [util :only (def- defmacro- domap raise boolean?)])))

(defn- fail [message val]
  (raise "Malformed query: " message " '" (pr-str val) "'"))

(defmacro- lint [pred val message]
  `(if-not (~pred ~val)
     (fail ~message ~val)))

(def- query-types
  #{"insert" "select" "count" "update" "delete" "explain" "list-collections"
    "create-index" "drop-index" "list-indexes" "multi-read" "multi-write"
    "checked-write"})

(defn- lint-coll [coll]
  (lint string? coll "collection not a string"))

(def- attr? string?)

(defn- lint-attr [attr]
  (lint attr? attr "attr not a string"))

(defn val? [v]
  (or (boolean? v) (nil? v)
      (string? v)  (number? v)))

(defn- lint-val [val]
  (lint val? val "value not of a recognized type"))

(defn- lint-prop [[attr val]]
  (lint-attr attr)
  (lint-val val))

(defn- lint-dir [dir]
  (lint #{"asc" "desc"} dir "unrecognized direction"))

(defn- lint-record [r]
  (lint map? r "record not a map")
  (domap lint-prop r))

(defn- lint-update-spec [us]
  (lint map? us "update spec not a map")
  (doseq [[attr val :as prop] us]
    (if (= "$inc" attr)
      (let [[on-attr by-param] (first val)]
        (lint-attr on-attr)
        (lint number? by-param "inc amount was not a number"))
      (lint-prop prop))))

(defn- lint-pos-int [i type]
  (lint #(and (integer? %) (pos? %)) i
    (str type " not a positive integer")))

(defn- lint-order-comp [order-comp]
  (lint vector? order-comp "order component not a vector")
  (lint #(= 2 (count %)) order-comp "order component should have 2 elements")
  (lint-attr (nth order-comp 0))
  (lint-dir  (nth order-comp 1)))

(defn- lint-order [order]
  (lint vector? order "order not a vector")
  (if (attr? (nth order 0))
    (lint-order-comp order)
    (domap lint-order-comp order)))

(def- sing-ops #{"=" "!=" "<" "<=" ">" ">="})
(def- doub-ops #{"><" ">=<" "><=" ">=<="})
(def- conj-ops #{"and" "or"})
(def- incl-ops #{"in" "not-in"})

(defn- lint-where [where]
  (lint vector? where "where not a vector")
  (let [op (nth where 0)]
    (cond
       (sing-ops op)
         (do
           (lint #(= 3 (count %)) where "wrong number of elems in where clase")
           (let [[_ attr val] where]
             (lint-attr attr)
             (lint-val val)))
       (doub-ops op)
         (do
           (lint #(= 3 (count %)) where "wrong number of elems in where clause")
           (let [[_ attr vals] where]
             (lint-attr attr)
             (lint vector? vals (str "vals for " op " must be in a vector"))
             (let [[val1 val2 & vrest] vals]
               (lint-val val1)
               (lint-val val2)
               (lint nil? vrest "extra vals in where component"))))
       (conj-ops op)
         (let [[_ & sub-wheres] where]
           (lint #(not (empty? %)) sub-wheres "no sub clauses given")
           (domap lint-where sub-wheres))
       (incl-ops op)
         (do
           (lint #(= 3 (count %)) where (str op " clause has 2 arguments"))
           (let [[_ attr vals] where]
             (lint-attr attr)
             (lint sequential? vals (str "vals for " op " must be in a sequence"))
             (domap lint-val vals)))
       :else
         (fail "unrecognized where operation" op))))

(defn- lint-only [only]
  (cond
    (attr?    only) :ok
    (vector?  only) (domap lint-attr only)
    :else           (fail "unrecognized only value" only)))

(defn- lint-distinct [d]
  (lint (partial contains? #{true false}) d "distinct must be true or false"))

(defn- lint-find-opts [opts allow-only]
  (when-not (nil? opts)
    (lint map? opts "options not a map")
    (doseq [[opt-name opt-val] opts]
      (condp = opt-name
        "limit"  (lint-pos-int opt-val "limit")
        "offset" (lint-pos-int opt-val "offset")
        "order"  (lint-order opt-val)
        "where"  (lint-where opt-val)
        "only"
          (if-not allow-only
            (fail "only option not applicable for this query" opt-val)
            (lint-only opt-val))
        "distinct" (lint-distinct opt-val)
        (fail "unrecognized find option" opt-name)))))

(defn- lint-id [i]
  (lint val? i "invalid id"))

(defn- lint-ispec [ispec]
  (cond
    (attr?   ispec) :ok
    (vector? ispec)
      (doseq [ispec-comp ispec]
        ;
        (cond
          (attr?   ispec-comp) :ok
          (vector? ispec-comp)
            (do
              (lint #(= 2 (count %)) ispec-comp "index spec component vector must have 2 elements")
              (lint-attr (nth ispec-comp 0))
              (lint-dir  (nth ispec-comp 1)))
          :else
            (fail "index spec component must be a string or vector" ispec-comp)))
    :else
      (fail "index spec must be a string or vector" ispec)))

(defn lint-num-args [q n]
  (if (integer? n)
    (lint #(= n (dec (count %))) q
          (str (nth q 0) " takes " n " arguments"))
    (lint #(n (dec (count %))) q
          (str (nth q 0) " takes "
               (first n) " or " (second n) " arguments"))))

(defn- lint-insert [q]
  (lint-num-args q 2)
  (let [[_ coll records] q]
    (lint-coll coll)
    (cond
      (map? records)
        (lint-record records)
      (sequential? records)
        (domap lint-record records)
      :else
        (fail "not recognized as record or records" records))))

(defn- lint-get [q]
  (lint-num-args q 2)
  (let [[_ coll vals] q]
    (lint-coll coll)
    (if (vector? vals)
      (domap lint-id vals)
      (lint-id vals))))

(defn- lint-select [q]
  (lint-num-args q #{1 2})
  (let [[_ coll find-opts] q]
    (lint-coll coll)
    (lint-find-opts find-opts true)))

(defn- lint-count [q]
  (lint-num-args q #{1 2})
  (let [[_ coll find-opts] q]
    (lint-coll coll)
    (lint-find-opts find-opts false)))

(defn- lint-update [q]
  (lint-num-args q #{2 3})
  (let [[_ coll update-spec find-opts] q]
    (lint-coll coll)
    (lint-update-spec update-spec)
    (lint-find-opts find-opts false)))

(defn- lint-delete [q]
  (lint-num-args q #{1 2})
  (let [[_ coll find-opts] q]
    (lint-coll coll)
    (lint-find-opts find-opts false)))

(declare lint-query)

(defn- lint-explain [q]
  (lint-num-args q 1)
  (lint-query (nth q 1)))

(defn- lint-list-collections [q]
  (lint-num-args q 0))

(defn- lint-drop-collection [q]
  (lint-num-args q 1)
  (lint-coll (nth q 1)))

(defn- lint-create-index [q]
  (lint-num-args q 2)
  (lint-coll (nth q 1))
  (lint-ispec (nth q 2)))

(defn- lint-drop-index [q]
  (lint-create-index q))

(defn- lint-list-indexes [q]
  (lint-num-args q 1)
  (lint-coll (nth q 1)))

(declare lint-query lint-read-query lint-write-query)

(defn- lint-multi-read [q]
  (lint-num-args q 1)
  (let [reads (nth q 1)]
    (lint vector? reads "read queries not given in a vector")
    (domap lint-read-query reads)))

(defn- lint-multi-write [q]
  (lint-num-args q 1)
  (let [writes (nth q 1)]
    (lint vector? writes "write queries not given in a vector")
    (domap lint-query writes)))

(defn- lint-checked-write [q]
  (lint-num-args q 3)
  (let [[_ read-query expected-result write-query] q]
    (lint-read-query read-query)
    (lint-write-query write-query)))

(defn- lint-clear [q]
  (lint-num-args q 0))

(defn- lint-auth [q]
  (lint-num-args q 1)
  (lint string? (nth q 1) "password not a string"))

(defn- lint-ping [q]
  (lint-num-args q 0))

(defn- lint-compact [q]
  (lint-num-args q 0))

(defn lint-info [q]
  (lint-num-args q 0))

(defn lint-query [q]
  (if-not (vector? q)
    (fail "query not a vector" q)
    (condp = (nth q 0)
      "insert"           (lint-insert           q)
      "select"           (lint-select           q)
      "count"            (lint-count            q)
      "update"           (lint-update           q)
      "delete"           (lint-delete           q)
      "explain"          (lint-explain          q)
      "list-collections" (lint-list-collections q)
      "drop-collection"  (lint-drop-collection  q)
      "create-index"     (lint-create-index     q)
      "drop-index"       (lint-drop-index       q)
      "list-indexes"     (lint-list-indexes     q)
      "multi-read"       (lint-multi-read       q)
      "multi-write"      (lint-multi-write      q)
      "checked-write"    (lint-checked-write    q)
      "clear"            (lint-clear            q)
      "auth"             (lint-auth             q)
      "ping"             (lint-ping             q)
      "compact"          (lint-compact          q)
      "info"             (lint-info             q)
      (fail "unrecognized query type" (nth q 0)))))

(defn- lint-read-query [q]
  (lint types/read-queries
        (nth q 0)
        "query not a read query")
  (lint-query q))

(defn- lint-write-query [q]
  (lint types/write-queries
        (nth q 0)
        "query not a write query")
  (lint-query q))
