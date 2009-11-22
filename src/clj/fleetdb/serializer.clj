(ns fleetdb.serializer
  (:import (fleetdb Serializer)
           (java.io File ByteArrayOutputStream ByteArrayInputStream
                    FileOutputStream BufferedOutputStream DataOutputStream
                    FileInputStream  BufferedInputStream  DataInputStream)))

(defn serialize [obj]
  (let [baos (ByteArrayOutputStream.)
        dos  (DataOutputStream. baos)]
    (Serializer/serialize dos obj)
    (.toByteArray baos)))

(defn deserialize [bytes eof-val]
  (let [bais  (ByteArrayInputStream. bytes)
        dis   (DataInputStream. bais)]
    (Serializer/deserialize dis eof-val)))

(defn dos-init [#^String dos-path]
  (let [dos-file (File. dos-path)]
    (assert (.createNewFile dos-file))
    (DataOutputStream. (BufferedOutputStream. (FileOutputStream. dos-path)))))

(defn dos-write [#^DataOutputStream dos obj]
  (Serializer/serialize dos obj)
  (.flush dos))

(defn dos-close [#^DataOutputStream dos]
  (.close dos))

(defn dis-init [#^String dis-path]
  (let [dis-file (File. dis-path)]
    (assert (.exists dis-file))
    (DataInputStream. (BufferedInputStream. (FileInputStream. dis-path)))))

(defn dis-read [dis eof-val]
  (Serializer/deserialize dis eof-val))

(defn dis-close [#^DataInputStream dis]
  (.close dis))

(def eof (Object.))