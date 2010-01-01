(ns fleetdb.client
  (:require (fleetdb [json :as json]))
  (:import (java.net Socket)
           (java.io  OutputStreamWriter BufferedWriter
                     InputStreamReader BufferedReader)))

(defn connect [#^String host #^Integer port]
  (let [socket (Socket. host port)]
    {:writer (BufferedWriter. (OutputStreamWriter. (.getOutputStream  socket)))
     :reader (BufferedReader. (InputStreamReader.  (.getInputStream socket)))
     :socket socket}))

(defn query [client q]
  (let [#^BufferedWriter writer (:writer client)
        #^BufferedReader reader (:reader client)]
    (.write writer #^String (json/generate-string q))
    (.write writer "\n")
    (.flush writer)
    (if-let [in-line (.readLine reader)]
      (let [[status result] (json/parse-string in-line)]
        (if (zero? status)
          result
          (throw (Exception. #^String result))))
      (throw (Exception. "No response from server.")))))

(defn close [client]
  (.close #^BufferedReader (:reader client))
  (.close #^BufferedWriter (:writer client))
  (.close #^Socket         (:socket client)))
