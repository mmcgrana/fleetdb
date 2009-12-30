(ns fleetdb.io
  (:import (fleetdb Serializer Bert Json)
           (org.codehaus.jackson JsonFactory JsonParser JsonGenerator)
           (java.io ByteArrayOutputStream ByteArrayInputStream
                    FileOutputStream BufferedOutputStream DataOutputStream
                    FileInputStream  BufferedInputStream  DataInputStream
                    FileWriter       BufferedWriter
                    FileReader       BufferedReader
                    StringWriter     StringReader
                    Writer           Reader
                    EOFException))
  (:require (fleetdb [file :as file]))
  (:use (fleetdb [util :only (def-)])))

(def eof (Object.))

(defn serialize [obj]
  (let [baos (ByteArrayOutputStream.)
        dos  (DataOutputStream. baos)]
    (Serializer/serialize dos obj)
    (.toByteArray baos)))

(defn deserialize [bytes eof-val]
  (let [bais  (ByteArrayInputStream. bytes)
        dis   (DataInputStream. bais)]
    (Serializer/deserialize dis eof-val)))

(defn bert-encode [obj]
  (let [baos (ByteArrayOutputStream.)
        dos  (DataOutputStream. baos)]
    (Bert/encode dos obj)
    (.toByteArray baos)))

(defn bert-decode [bytes eof-val]
  (let [bais  (ByteArrayInputStream. bytes)
        dis   (DataInputStream. bais)]
    (Bert/decode dis eof-val)))

(defn dos-init [#^String dos-path]
  (DataOutputStream. (BufferedOutputStream.
    (FileOutputStream. dos-path #^Boolean (file/exist? dos-path)))))

(defn dos-close [#^DataOutputStream dos]
  (.close dos))

(defn dis-init [#^String dis-path]
  (assert (file/exist? dis-path))
  (DataInputStream. (BufferedInputStream. (FileInputStream. dis-path))))

(defn dis-close [#^DataInputStream dis]
  (.close dis))

(defn dos-serialize [#^DataOutputStream dos obj]
  (let [#^"[B" bytes (serialize obj)]
    (.write dos bytes)
    (.flush dos)))

(defn dis-deserialize [dis eof-val]
  (Serializer/deserialize dis eof-val))

(defn dis-deserialized-seq [dis]
  (lazy-seq
    (let [elem (dis-deserialize dis eof)]
      (if-not (identical? elem eof)
        (cons elem (dis-deserialized-seq dis))))))

(defn dos-bert-encode [#^DataOutputStream dos obj]
  (let [#^"[B" bytes (bert-encode obj)]
    (.write dos bytes)
    (.flush dos)))

(defn dis-bert-decode [#^DataInputStream dis eof-val]
  (Bert/decode dis eof-val))

(defn dis-bert-decoded-seq [dis]
  (lazy-seq
    (let [elem (dis-bert-decode dis eof)]
      (if-not (identical? elem eof)
        (cons elem (dis-bert-decoded-seq dis))))))

(defn dos-berp-encode [#^DataOutputStream dos obj]
  (let [#^"[B" bytes (bert-encode obj)]
    (.writeInt dos (alength bytes))
    (.write dos bytes)
    (.flush dos)))

(defn dis-berp-decode [#^DataInputStream dis eof-val]
  (try
    (let [len   (.readInt dis)
          bytes (byte-array len)]
      (.read dis bytes)
      (bert-decode bytes eof-val))
    (catch EOFException e
      eof-val)))

(def- #^JsonFactory factory (JsonFactory.))

(defn writer->generator [#^Writer writer]
  (.createJsonGenerator factory writer))

(defn os->generator [#^OutputStream]
  (writer->generator (OutputStreamWriter. (BufferedOutputStream. os))))

(defn path->generator [#^String path]
  (writer->generator (BufferedWriter. (FileWriter. path))))

(defn generate [generator obj]
  (Json/generate generator obj)
  (.flush #^JsonGenerator generator))

(defn generate-string [obj]
  (let [sw (StringWriter.)]
    (generate (writer-generator sw) obj)
    (.toString sw)))

(defn generator-close [#^JsonGenerator generator]
  (.close generator))

(defn reader->parser [#^Reader reader]
  (.createJsonParser factory reader))

(defn is->parser [#^InputStream is]
  (reader->parser (InputStreamReader. (BufferedInputStream. is))))

(defn path->parser [#^String path]
  (reader-parser (BufferedReader. (FileReader. path))))

(defn parse [parser eof]
  (Json/parse parser true eof))

(defn parse-string [string eof]
  (parse (reader-parser (StringReader. string)) eof))

(defn parsed-seq [parser]
  (lazy-seq
    (let [elem (parse parser eof)]
      (if-not (identical? elem eof)
        (cons elem (parsed-seq parser))))))

(defn close-parser [#^JsonParser parser]
  (.close parser))
