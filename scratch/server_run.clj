(set! *warn-on-reflection* true)

(ns scratch.server-run
  (:require fleetdb.server))

(fleetdb.server/run
  {:port 4444 :protocol (keyword (first *command-line-args*)) :threads 100
   :persistent true :db-path "/Users/mmcgrana/Desktop/test.fleet"})