(ns fleetdb.file
  (:import (java.io File)))

(defn exist? [#^String path]
  (.exists (File. path)))

(defn size [#^String path]
  (.length (File. path)))

(defn mv [#^String from #^String to]
  (assert (.renameTo (File. from) (File. to))))

(defn rm [#^String path]
  (.delete (File. path)))

(defn touch [#^String path]
  (.createNewFile (File. path)))
