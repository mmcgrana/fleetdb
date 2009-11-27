(set! *warn-on-reflection* true)

(defn bench1 [n]
  (let [ispecs '([[:age :asc]] [[:name :asc] [:age :asc]] [[:name :asc] [:age :asc] [:height :desc]])
        where  [:and [:= :name "mark"] [:= :age 23]]
        order  [[:height :asc]]
        offset 4
        limit  2
        only   [:name :age :height]]
  (dotimes [i n]
    (find-plan ispecs where order offset limit only))))

(defn bench2 [n]
  (let [where [:= :id "abc"]]
    (dotimes [i n]
      (find-plan nil where nil nil nil nil))))

(when false
  (bench1 100000)
  (time (bench1 100000))
  (bench2 100000)
  (time (bench2 100000)))
