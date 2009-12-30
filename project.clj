(defproject fleetdb "0.1.0-SNAPSHOT"
  :description "A record-oriented, persistent, main-memory database implemented in Clojure and Java."
  :url "http://github.com/mmcgrana/fleetdb"
  :source-path "src/clj"
  :java-source-path "src/jvm/"
  :javac-fork "true"
  :dependencies [[org.clojure/clojure "1.1.0-master-SNAPSHOT"]
                 [org.clojure/clojure-contrib "1.0-SNAPSHOT"]
                 [clj-stacktrace "0.1.0-SNAPSHOT"]
                 [net.sf.jopt-simple/jopt-simple "3.2"]
                 [org.codehaus.jackson/jackson-core-asl "1.4.0"]]
  :dev-dependencies [[lein-clojars "0.5.0-SNAPSHOT"]
                     [clj-unit "0.1.0-SNAPSHOT"]
                     [lein-javac "0.0.2-SNAPSHOT"]])
