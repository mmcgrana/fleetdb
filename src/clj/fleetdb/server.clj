(set! *warn-on-reflection* true)

(ns fleetdb.server
  (:require (fleetdb [embedded :as embedded] [io :as io]))
  (:import  (java.net ServerSocket Socket)
            (java.io PrintWriter BufferedReader InputStreamReader PushbackReader)))

(defn run [read-path write-path port]
  (let [server-socket (ServerSocket. port)
        dba           (embedded/init read-path write-path)]
    (loop []
      (let [socket (.accept server-socket)]
        (try
          (let [out   (PrintWriter. (.getOutputStream socket))
                in    (PushbackReader. (BufferedReader. (InputStreamReader. (.getInputStream socket))))]
            (loop []
              (let [query (read in false io/eof)]
                (if (identical? query io/eof)
                  (do
                    (.close out)
                    (.close in)
                    (.close socket))
                  (do
                    (.print out (prn-str query))
                    (.flush out)
                    (recur))))))
          (catch Exception e
            (.printStackTrace e)))
        (recur)))))

(use 'clojure.contrib.shell-out)
(sh "rm" "-f" "/Users/mmcgrana/Desktop/log")
(run nil "/Users/mmcgrana/Desktop/log" 4444)
