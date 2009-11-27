(ns fleetdb.client-test
  (:require (fleetdb [client :as client]))
  (:use (clj-unit core)))

(defmacro with-client [[name host-form port-form] & body]
  `(let [~name (client/connect ~host-form ~port-form)]
     (try
       (client/query ~name [:delete :elems])
       ~@body
       (finally
         (client/close ~name)))))

(defmacro with-test-client [name & body]
  `(with-client [~name "127.0.0.1" 3400] ~@body))

(deftest "ping"
  (with-test-client client
    (assert= [0 "pong"] (client/query client [:ping]))))

(deftest "invalid query"
  (with-test-client client
    (let [[c e] (client/query client [:foo])]
      (assert= c 1)
      (assert-match #"invalid query" e))))

(deftest "valid query"
  (with-test-client client
    (let [[c1 r1] (client/query client [:insert :elems {:id 1}])
          [c2 r2] (client/query client [:select :elems])]
      (assert= c1 0)
      (assert= r1 1)
      (assert= c2 0)
      (assert= r2 [{:id 1}]))))
