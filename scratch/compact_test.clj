(ns scratch.compact-test
  (:require (fleetdb [client :as client])))

(let [client (client/connect "localhost" 4444)]
  (client/query client [:delete])
  (dotimes [i 1000]
    (client/query client [:insert {:id i}]))
  (prn (client/query client [:count]))
  (dotimes [i 500]
    (client/query client [:delete {:where [:= :id i]}]))
  (prn (client/query client [:count]))
  (println (client/query client [:compact])))