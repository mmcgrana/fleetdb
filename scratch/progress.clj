(defn bar [n]
  (apply str (take n (repeat "-"))))

(defn left [n]
  (str "\033[" n "D"))

(def clear "\033[2K")


(dotimes [n 50]
  (.print System/out clear)
  (if-not (zero? n)
    (.print System/out (left (dec n))))
  (.print System/out (bar n))
  (.flush System/out)
  (Thread/sleep 100))
(.println System/out)
