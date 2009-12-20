(ns fleetdb.file
  (:import (java.io File)))

(defn exist? [#^String path]
  (.exists (File. path)))

(defn size [#^String path]
  (.length (File. path)))

(defn tmp-path [#^String tmp-dir-path prefix]
  (let [tmp-dir  (File. tmp-dir-path)
        tmp-file (File/createTempFile prefix nil tmp-dir)]
    (.getAbsolutePath tmp-file)))

(defn mv [#^String from #^String to]
  (assert (.renameTo (File. from) (File. to))))

(defn rm [#^String path]
  (.delete (File. path)))