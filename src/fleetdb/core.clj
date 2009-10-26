(ns fleetdb.core)

(defn init []
  {:records (sorted-map)})

(defn- insert [db {:keys [record]}]
  (assoc-in db [:records (:id record)] record))

(defn exec [db [query-type opts]]
  (if (= query-type :insert)
    (insert db opts)
    (throw (Exception. "command not recognized"))))
