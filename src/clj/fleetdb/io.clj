(ns fleetdb.io
  (:import (fleetdb Serializer Json)
           (org.codehaus.jackson JsonFactory JsonParser JsonGenerator)
           (java.io FileOutputStream BufferedOutputStream
                    FileInputStream  BufferedInputStream
                    FileWriter StringWriter OutputStreamWriter BufferedWriter
                    FileReader StringReader InputStreamReader  BufferedReader
                    InputStream OutputStream Writer Reader EOFException))
  (:require (fleetdb [file :as file]))
  (:use (fleetdb [util :only (def-)])))

(def eof (Object.))

(def- #^JsonFactory factory (JsonFactory.))

(defn writer->generator [#^Writer writer]
  (.createJsonGenerator factory writer))

(defn os->generator [#^OutputStream os]
  (writer->generator (OutputStreamWriter. (BufferedOutputStream. os))))

(defn path->generator [#^String path]
  (writer->generator
    (BufferedWriter. (OutputStreamWriter.
      (FileOutputStream. path #^Boolean (file/exist? path))))))

(defn generate [generator obj]
  (Json/generate generator obj)
  (.flush #^JsonGenerator generator))

(defn generate-string [obj]
  (let [sw (StringWriter.)]
    (generate (writer->generator sw) obj)
    (.toString sw)))

(defn generator-close [#^JsonGenerator generator]
  (.close generator))

(defn reader->parser [#^Reader reader]
  (.createJsonParser factory reader))

(defn is->parser [#^InputStream is]
  (reader->parser (InputStreamReader. (BufferedInputStream. is))))

(defn path->parser [#^String path]
  (reader->parser (BufferedReader. (FileReader. path))))

(defn parse [parser eof]
  (Json/parse parser true eof))

(defn parse-string [string eof]
  (parse (reader->parser (StringReader. string)) eof))

(defn parsed-seq [parser]
  (lazy-seq
    (let [elem (parse parser eof)]
      (if-not (identical? elem eof)
        (cons elem (parsed-seq parser))))))

(defn parser-close [#^JsonParser parser]
  (.close parser))
