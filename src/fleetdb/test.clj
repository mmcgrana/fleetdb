(ns fleetdb.test
  (:use (fleetdb core)))

(def db (init))

(def records
  (for [x (range 1000000)]
    {:id x :name "Mark" :age 23}))

(dorun records)
(prn "records built")

(dotimes [n 3]
  (time
    (reduce
      #(exec %1 [:insert %2])
      db
      records)))
