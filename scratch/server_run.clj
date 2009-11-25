(ns scratch.server-run
  (:require fleetdb.server))

(fleetdb.server/run
  {:port 4444 :binary false :persistent true
   :db-path "/Users/mmcgrana/Desktop/test.fleet"})