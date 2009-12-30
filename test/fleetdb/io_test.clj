(ns fleetdb.io-test
  (:require (fleetdb [io :as io]))
  (:use (clj-unit core)))

(deftest "dserialize/deserialize round trip"
  (let [struct {:int 3 :long 9223372036854775807 :bigint 9223372036854775808
                :double 1.23 :boolean true :nil nil :string "string"
                :list (list 1 2 3) :vec [1 2 3]}]
    (assert= struct (io/deserialize (io/serialize struct) io/eof))))

(deftest "generate-string/parse-string round trip"
  (let [struct {"int" 3 "long" 52001110638799097 "bigint" 9223372036854775808
                "double" 1.23 "boolean" true "nil" nil "string" "string"
                "vec" [1 2 3] "map" {"a" "b"}}]
    (assert= struct (io/parse-string (io/generate-string struct) io/eof))))

(deftest "generate-strink keyword coercion"
  (assert= {"foo" "bar"}
           (io/parse-string (io/generate-string {:foo "bar"}) io/eof)))
