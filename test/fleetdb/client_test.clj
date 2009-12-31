(ns fleetdb.client-test
  (:require (fleetdb [client :as client]))
  (:use (clj-unit core)))

(defmacro with-client [[name host-form port-form] & body]
  `(let [~name (client/connect ~host-form ~port-form)]
     (try
       (client/query ~name ["delete" "elems"])
       ~@body
       (finally
         (client/close ~name)))))

(defmacro with-test-client [name & body]
  `(with-client [~name "127.0.0.1" 3400] ~@body))

(deftest "ping"
  (with-test-client client
    (assert= "pong" (client/query client ["ping"]))))

(deftest "malformed query"
  (with-test-client client
    (assert-throws #"Malformed query"
      (client/query client ["foo"]))))

(deftest "valid query"
  (with-test-client client
    (let [r1 (client/query client ["insert" "elems" {"id" 1}])
          r2 (client/query client ["select" "elems"])]
      (assert= r1 1)
      (assert= r2 [{"id" 1}]))))
