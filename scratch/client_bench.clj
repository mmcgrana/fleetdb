(set! *warn-on-reflection* true)

(require (fleetdb client))

(defn hit [n]
  (let [socket (Socket. "localhost" 4444)
        dis    (DataInputStream.  (BufferedInputStream.  (.getInputStream  socket)))
        dos    (DataOutputStream. (BufferedOutputStream. (.getOutputStream socket)))
        q      [:ping]]
    (dotimes [_ n]
      (io/dos-write dos q)
      (io/dis-read dis io/eof))))

(time (hit 100000))

(time
  (let [executor (exec/init-pool 10)]
    (dotimes [_ 10]
      (exec/submit executor #(hit 10000)))
    (exec/shutdown executor)
    (exec/await-termination executor 100)))
