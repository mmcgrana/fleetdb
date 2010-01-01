(ns fleetdb.json-test
  (:require (fleetdb [json :as json]))
  (:import (java.io StringReader BufferedReader))
  (:use (clj-unit core)))

(deftest "generate-string/parse-string round trip"
  (let [struct {"int" 3 "long" 52001110638799097 "bigint" 9223372036854775808
                "double" 1.23 "boolean" true "nil" nil "string" "string"
                "vec" [1 2 3] "map" {"a" "b"} "list" (list "a" "b")}]
    (assert= struct (json/parse-string (json/generate-string struct)))))

(deftest "generate-string keyword coercion"
  (assert= {"foo" "bar"}
           (json/parse-string (json/generate-string {:foo "bar"}))))

(deftest "parse-lines"
  (let [br (BufferedReader. (StringReader. "1\n2\n3\n"))]
    (assert= (list 1 2 3) (json/parse-lines br))))