(ns scratch.log-cat
  (:use (fleetdb [io :as io])))

(let [dis-seq (io/dis-seq (io/dis-init (first *command-line-args*)))]
 (doseq [elem dis-seq]
   (prn elem)))
