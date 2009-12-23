(ns fleetdb.lint
  (:use (fleetdb [util :only (def- defmacro- domap raise boolean?)])))

(defn- fail [message val]
  (raise "Malformed query: " message " '" (pr-str val) "'"))

(defmacro- lint [pred val message]
  `(if-not (~pred ~val)
     (fail ~message ~val)))

(def- query-types
  #{:insert :get :select :count :update :delete :explain :list-collections
    :create-index :drop-index :list-indexes :multi-read :multi-write
    :checked-write})

(defn- lint-coll [coll]
  (lint keyword? coll "collection not a keyword"))

(defn- lint-attr [attr]
  (lint keyword? attr "attr not a keyword?"))

(defn val? [v]
  (or (boolean? v) (nil? v)
      (string? v) (keyword? v) (number? v)))

(defn- lint-val [val]
  (lint val? val "value not of a recognized type"))

(defn- lint-prop [[attr val]]
  (lint-attr attr)
  (lint-val val))

(defn- lint-dir [dir]
  (lint #{:asc :desc} dir "unrecognized direction"))

(defn- lint-record [r]
  (lint map? r "record not a map")
  (domap lint-prop r))

(defn- lint-pos-int [i type]
  (lint #(and (integer? %) (pos? %)) i
    (str type " not a positive integer")))

(defn- lint-order-comp [order-comp]
  (lint vector? order-comp "order component not a vector")
  (lint #(= 2 (count %)) order-comp "order component should have 2 elements")
  (lint-attr (first order-comp))
  (lint-dir (second order-comp)))

(defn- lint-order [order]
  (lint vector? order "order not a vector")
  (if (keyword? (first order))
    (lint-order-comp order)
    (domap lint-order-comp order)))

(def- sing-ops #{:= :!= :< :<= :> :>=})
(def- doub-ops #{:>< :>=< :><= :>=<=})
(def- conj-ops #{:and :or})

(defn- lint-where [where]
  (lint vector? where "where not a vector")
  (let [op (first where)]
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
       (= :in op)
         (do
           (lint #(= 3 (count %)) where ":in clause has 2 arguments")
           (let [[_ attr vals] where]
             (lint-attr attr)
             (lint vector? vals "vals for :in must be in a vector")
             (domap lint-val vals)))
       :else
         (fail "unrecognized where operation" op))))

(defn- lint-only [only]
  (cond
    (keyword? only) :ok
    (vector?  only) (domap lint-attr only)
    :else           (fail "unrecognized :only value" only)))

(defn- lint-find-opts [opts allow-only]
  (when-not (nil? opts)
    (lint map? opts "options not a map")
    (doseq [[opt-name opt-val] opts]
      (condp = opt-name
        :limit  (lint-pos-int opt-val "limit")
        :offset (lint-pos-int opt-val "offset")
        :order  (lint-order opt-val)
        :where  (lint-where opt-val)
        :only
          (if-not allow-only
            (fail "only option not applicable for this query" opt-val)
            (lint-only opt-val))
        (fail "unrecognized find option" opt-name)))))

(defn- lint-id [i]
  (lint val? i "invalid id"))

(defn- lint-ispec [ispec]
  (cond
    (keyword? ispec) :ok
    (vector? ispec)
      (doseq [ispec-comp ispec]
        (cond
          (keyword? ispec-comp) :ok
          (vector? ispec-comp)
            (lint #(= 2 (count %)) ispec-comp "index spec component vector must have 2 elements")
            (lint-attr (first ispec-comp))
            (lint-dir  (second ispec-comp)))
          :else
            (fail "index spec component must be a keyword or vector" ispec-comp))
    :else
      (fail "index spec must be a keyword or vector" ispec)))

(defn lint-num-args [n q]
  (if (integer? n)
    (lint #(= n (dec (count %))) q
          (str (first q) " query takes " n " arguments"))
    (lint #(n (dec (count %))) q
          (str (first q) " query takes "
               (first n) " or " (second n) " arguments"))))

(defn- lint-insert [q]
  (lint-num-args 2 q)
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
  (lint-num-args 2 q)
  (let [[_ coll vals] q]
    (lint-coll coll)
    (if (vector? vals)
      (domap lint-id vals)
      (lint-id vals))))

(defn- lint-select [q]
  (lint-num-args #{1 2} q)
  (let [[_ coll find-opts] q]
    (lint-coll coll)
    (lint-find-opts find-opts true)))

(defn- lint-count [q]
  (lint-num-args #{1 2} q)
  (let [[_ coll find-opts] q]
    (lint-coll coll)
    (lint-find-opts find-opts false)))

(defn- lint-update [q]
  (lint-num-args #{2 3} q)
  (let [[_ coll up-map find-opts] q]
    (lint-coll coll)
    (lint-record up-map)
    (lint-find-opts find-opts false)))

(defn- lint-delete [q]
  (lint-num-args #{1 2} q)
  (let [[_ coll find-opts] q]
    (lint-coll coll)
    (lint-find-opts find-opts false)))

(declare lint-query)

(defn- lint-explain [q]
  (lint-num-args 1 q)
  (lint-query (second q)))

(defn- lint-list-collections [q]
  (lint-num-args 0 q))

(defn- lint-create-index [q]
  (lint-num-args 2 q)
  (lint-ispec (second q)))

(defn- lint-drop-index [q]
  (lint-create-index q))

(defn- lint-list-indexes [q]
  (lint-num-args 1 q)
  (lint-coll (second q)))

(declare lint-read-query lint-write-query)

(defn- lint-multi-read [q]
  (lint-num-args 1 q)
  (let [reads (second q)]
    (lint vector? reads "read queries not given in a vector")
    (domap lint-read-query reads)))

(defn- lint-multi-write [q]
  (lint-num-args 1 q)
  (let [writes (second q)]
    (lint vector? writes "write queries not given in a vector")
    (domap lint-write-query writes)))

(defn- lint-checked-write [q]
  (lint-num-args 3 q)
  (let [[_ read-query expected-result write-query] q]
    (lint-read-query read-query)
    (lint-write-query write-query)))

(defn lint-query [q]
  (if-not (vector? q)
    (fail "query not a vector" q)
    (condp = (first q)
      :insert           (lint-insert           q)
      :get              (lint-get              q)
      :select           (lint-select           q)
      :count            (lint-count            q)
      :update           (lint-update           q)
      :delete           (lint-delete           q)
      :explain          (lint-explain          q)
      :list-collections (lint-list-collections q)
      :create-index     (lint-create-index     q)
      :drop-index       (lint-drop-index       q)
      :list-indexes     (lint-list-indexes     q)
      :multi-read       (lint-multi-read       q)
      :multi-write      (lint-multi-write      q)
      :checked-write    (lint-checked-write    q)
      (fail "unrecognized query type" (first q)))))

(defn- lint-read-query [q]
  (lint #{:get :select :count :explain :list-collections :list-indexes :multi-read}
        (first q)
        "query not a read query")
  (lint-query q))

(defn- lint-write-query [q]
  (lint #{:insert :update :delete :create-index :drop-index :multi-write :checked-write}
        (first q)
        "query not a write query")
  (lint-query q))
