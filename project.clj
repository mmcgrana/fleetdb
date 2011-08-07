(defproject fleetdb "0.3.1"
  :description "A schema-free database optimized for agile development."
  :url "http://github.com/mmcgrana/fleetdb"
  :source-path "src/clj"
  :java-source-path "src/jvm/"
  :javac-fork "true"
  :aot [fleetdb.server]
  :dependencies
    [[org.clojure/clojure "1.2.0"]
     [org.clojure/clojure-contrib "1.2.0"]
     [clj-stacktrace "0.1.2"]
     [net.sf.jopt-simple/jopt-simple "3.2"]
     [clj-json "0.2.0"]]
  :dev-dependencies
    [[org.clojars.mmcgrana/lein-javac "1.2.1"]
     [clj-unit "0.1.0"]])
