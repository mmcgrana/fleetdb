(defproject fleetdb "0.1.1-SNAPSHOT"
  :description "A schema-free database optimized for agile development."
  :url "http://github.com/mmcgrana/fleetdb"
  :source-path "src/clj"
  :java-source-path "src/jvm/"
  :javac-fork "true"
  :dependencies [[org.clojure/clojure "1.1.0"]
                 [org.clojure/clojure-contrib "1.1.0-master-SNAPSHOT"]
                 [clj-stacktrace "0.1.0-SNAPSHOT"]
                 [net.sf.jopt-simple/jopt-simple "3.2"]
                 [clj-json "0.1.0-SNAPSHOT"]]
  :dev-dependencies [[lein-clojars "0.5.0-SNAPSHOT"]
                     [clj-unit "0.1.0-SNAPSHOT"]
                     [lein-javac "0.0.2-SNAPSHOT"]]
  :namespaces [fleetdb.server])
