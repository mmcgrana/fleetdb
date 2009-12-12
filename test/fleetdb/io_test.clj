(ns fleetdb.io-test
  (:require (fleetdb [io :as io]))
  (:use (clj-unit core)))

(deftest "serialize/deserialize round trip"
  (let [struct {:int 3 :long 9223372036854775807 :bigint 9223372036854775808
                :double 1.23 :boolean true :nil nil :string "string"
                :list (list 1 2 3) :vec [1 2 3]}]
    (assert= struct (io/deserialize (io/serialize struct) io/eof))))
