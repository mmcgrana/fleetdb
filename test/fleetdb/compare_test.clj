(ns fleetdb.compare-test
  (:import (fleetdb Compare))
  (:use (clj-unit core)
        (fleetdb [util :only (def-)])))

(def- c #(Compare/compare %1 %2))

(deftest "infinities"
  (assert= -1 (c :neg-inf 3))
  (assert= 1  (c 3        :neg-inf))
  (assert= 1  (c :pos-inf 3))
  (assert= -1 (c 3        :pos-inf))
  (assert= 0  (c :neg-inf :neg-inf))
  (assert= 0  (c :pos-inf :pos-inf))
  (assert= -1 (c :neg-inf :pos-inf))
  (assert= 1  (c :pos-inf :neg-inf)))

(deftest "nils"
  (assert= -1 (c nil 3))
  (assert= 1  (c 3   nil)))

(deftest "booleans"
  (assert= -1 (c false true)))

(deftest "numbers"
  (assert= 0  (c 3 3))
  (assert= -1 (c 3 5))
  (assert= 1  (c 5 3))
  (assert= 0  (c 3.0 3.0))
  (assert= -1 (c 3.0 5.0))
  (assert= -1 (c 3.0 5))
  (assert= 1  (c 5.0 3.0))
  (assert= 1  (c 5.0 3)))

(deftest "strings"
  (assert= 0 (c "foo" "foo"))
  (assert= 4 (c "foo" "bar")))

(deftest "keywords"
  (assert= 0 (c :foo :foo))
  (assert= 4 (c :foo :bar)))

(deftest "non-matching elems"
  (assert-throws #"cannot be cast"
    (c 1 :one)))

(deftest "vectors"
  (assert= 0 (c [1 :2] [1 :2]))
  (assert= 1 (c [1 :2] [1 :1])))

(deftest "vectors with infinities"
  (assert= -1 (c [1 :neg-inf] [1 0])))

(deftest "non-matching vectors"
  (assert-throws #"Cannot compare"
    (c [1 2] [1]))
  (assert-throws #"cannot be cast"
    (c [1] [:one])))