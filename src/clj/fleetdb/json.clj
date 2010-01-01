(ns fleetdb.json
  (:import (fleetdb Json)
           (org.codehaus.jackson JsonFactory JsonParser)
           (java.io StringWriter StringReader BufferedReader))
  (:use (fleetdb [util :only (def-)])))

(def- #^JsonFactory factory (JsonFactory.))

(defn generate-string [obj]
  (let [sw        (StringWriter.)
        generator (.createJsonGenerator factory sw)]
    (Json/generate generator obj)
    (.flush generator)
    (.toString sw)))

(defn parse-string [string]
  (Json/parse (.createJsonParser factory (StringReader. string)) true nil))

(defn- parse-lines* [#^JsonParser parser]
  (let [eof (Object.)]
    (lazy-seq
      (let [elem (Json/parse parser true eof)]
        (if-not (identical? elem eof)
          (cons elem (parse-lines* parser)))))))

(defn parse-lines [#^BufferedReader reader]
  (parse-lines* (.createJsonParser factory reader)))
