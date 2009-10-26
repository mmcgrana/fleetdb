(ns fleetdb.test
  (:use (fleetdb core)))

(def db (init))

(def records
  (for [x (range 1000000)]
    {:id x :name "Mark" :age 23}))

(prn "building records")
(dorun records)

; (prn "inserting")
; (dotimes [n 10]
;   (time
;     (reduce
;       #(exec %1 [:insert {:record %2}])
;       db
;       records)))

(prn "building db")
(def db
  (reduce
    #(exec %1 [:insert {:record %2}])
    db
    (take 10 records)))

; (prn "selecting")
; (dotimes [n 10]
;   (time
;     (prn (exec db [:select {:where [:= :id 100] :limit 1 :only [:name]}]))))

; (prn "counting")
; (dotimes [n 10]
;   (time
;     (prn (exec db [:count {:where [:= :id 100]}]))))

(prn "updating")
(def db (exec db [:update {:where [:= :id 5] :with {:name "Frank" :grandpop true}}]))
(prn db)

(prn "deleting")
(def db (exec db [:delete {:where [:in :id [0 2 4 6 8]]}]))
(prn db)