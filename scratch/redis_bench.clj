(ns scratch.redis-bench
  (:require redis))


(redis/with-server
  {:host "127.0.0.1" :port 6379 :db 0}
  (do
    (time
      (dotimes [_ 100000]
        (redis/set "123" "{:id 123 :name :mark}")))))

;(println "Reply:" (redis/set "foo" "bar"))
;(println "Reply:" (redis/get "foo"))))