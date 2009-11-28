(ns fleetdb.test-util
  (:use (clj-unit core)))

(defn assert-set= [expected actual]
  (let [expected-set (set expected)
        actual-set   (set actual)]
    (assert-truth (= expected-set actual-set)
      (format "Expected set %s but got %s." expected-set actual-set))))