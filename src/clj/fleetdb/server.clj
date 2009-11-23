(set! *warn-on-reflection* true)

(ns fleetdb.server
  (:import (java.net ServerSocket Socket)
           (java.io PrintWriter BufferedReader InputStreamReader)))

(let [server-socket (ServerSocket. 4444 10000)]
  (loop []
    (let [socket (.accept server-socket)]
      (try
        (let [out (PrintWriter. (.getOutputStream socket))
              in  (BufferedReader. (InputStreamReader. (.getInputStream socket)))]
          (.readLine in)
          (.println out "HTTP/1.0 200 OK")
          (.println out)
          (.println out "Content-Length: 1354")
          (.println out "<html>hi</html>")
          (.close out)
          (.close in))
        (catch Exception e
          (.printStackTrace e))))
    (recur)))
