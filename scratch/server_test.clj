(set! *warn-on-reflection* true)

(require '(fleetdb [io :as io]))
(import '(java.net Socket)
        '(java.io DataInputStream BufferedInputStream DataOutputStream BufferedOutputStream))

(let [socket (Socket. "localhost" 4444)
      dis    (DataInputStream.  (BufferedInputStream.  (.getInputStream  socket)))
      dos    (DataOutputStream. (BufferedOutputStream. (.getOutputStream socket)))]
  (io/dos-write dos [:select])
  (prn (io/dis-read dis io/eof))
  (io/dos-write dos [:insert {:records [{:id 1 :name "mark"} {:id 2 :name "laura"}]}])
  (prn (io/dis-read dis io/eof))
  (io/dos-write dos [:select])
  (prn (io/dis-read dis io/eof)))


