(ns fleetdb.io
  (:import (fleetdb Serializer)
           (java.io ByteArrayOutputStream ByteArrayInputStream
                    FileOutputStream BufferedOutputStream DataOutputStream
                    FileInputStream  BufferedInputStream  DataInputStream))
  (:require (fleetdb [file :as file])))

(defn serialize [obj]
  (let [baos (ByteArrayOutputStream.)
        dos  (DataOutputStream. baos)]
    (Serializer/serialize dos obj)
    (.toByteArray baos)))

(defn deserialize [bytes eof-val]
  (let [bais  (ByteArrayInputStream. bytes)
        dis   (DataInputStream. bais)]
    (Serializer/deserialize dis eof-val)))

(def eof (Object.))

(defn dos-init [#^String dos-path]
  (DataOutputStream. (BufferedOutputStream.
    (FileOutputStream. dos-path #^Boolean (file/exist? dos-path)))))

(defn dos-write [#^DataOutputStream dos obj]
  (Serializer/serialize dos obj)
  (.flush dos))

(defn dos-close [#^DataOutputStream dos]
  (.close dos))

(defn dis-init [#^String dis-path]
  (assert (file/exist? dis-path))
  (DataInputStream. (BufferedInputStream. (FileInputStream. dis-path))))

(defn dis-read [dis eof-val]
  (Serializer/deserialize dis eof-val))

(defn dis-seq [dis]
  (lazy-seq
    (let [elem (dis-read dis eof)]
      (if-not (identical? elem eof)
        (cons elem (dis-seq dis))))))

(defn dis-close [#^DataInputStream dis]
  (.close dis))
