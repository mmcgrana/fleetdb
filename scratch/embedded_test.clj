(use '(fleetdb [embedded :as embedded]))

(def dba (embedded/init))

(prn (embedded/query dba [:select]))
(embedded/query dba [:insert {:records [{:id 1 :name "mark"} {:id 2 :name "matt"}]}])
(prn (embedded/query dba [:select]))
(embedded/close dba)