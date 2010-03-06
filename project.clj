(defproject fleetdb "0.2.0-RC1"
  :description "A schema-free database optimized for agile development."
  :url "http://github.com/mmcgrana/fleetdb"
  :source-path "src/clj"
  :java-source-path "src/jvm/"
  :javac-fork "true"
  :dependencies [[org.clojure/clojure "1.1.0"]
                 [org.clojure/clojure-contrib "1.1.0"]
                 [clj-stacktrace "0.1.0"]
                 [net.sf.jopt-simple/jopt-simple "3.2"]
                 [clj-json "0.2.0"]]
  :dev-dependencies [[org.clojars.mmcgrana/lein-clojars "0.5.0"]
                     [org.clojars.mmcgrana/lein-javac "0.1.0"]
                     [clj-unit "0.1.0"]]
  :namespaces [fleetdb.server])
