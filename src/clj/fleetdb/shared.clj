(ns fleetdb.shared
  (:use (fleetdb util))
  (:import (clojure.lang Numbers)))

(def neg-inf :neg-inf)
(def pos-inf :pos-inf)

(defn- compare* [a b]
  (cond
    (identical? a b)
      0
    (or (identical? a neg-inf) (identical? b pos-inf))
      -1
    (or (identical? a pos-inf) (identical? b neg-inf))
      1
    (nil? a)
      -1
    (nil? b)
      1
    (number? a)
      (Numbers/compare a b)
    :else
      (.compareTo #^Comparable a #^Comparable b)))

(defn order-compare [order]
  (let [[[attr dir] & rorder] order]
    (if (not rorder)
      (cond
        (= dir :asc)
          #(compare* (attr %1) (attr %2))
        (= dir :dsc)
          #(compare* (attr %2) (attr %1))
        :else
          (raise ("invalid order " order)))
      (let [rcompare (order-compare rorder)]
        (cond
          (= dir :asc)
            #(let [c (compare* (attr %1) (attr %2))] (if (zero? c) (rcompare %1 %2) c))
          (= dir :dsc)
            #(let [c (compare* (attr %2) (attr %1))] (if (zero? c) (rcompare %1 %2) c))
          :else
            (raise "invalid order " order))))))
