[:create-index <coll> <order>]
[:select {:where [:= :attr "val"] :order <order>}]

; fully qualified
[[:name :asc]]
[[:name :asc] [:age :asc]]
[[:name :asc] [:age :asc] [:height :asc]]

[[:name :desc]]
[[:name :desc] [:age :desc]]
[[:name :desc] [:age :desc] [:height :desc]]

[[:name :asc] [:age :desc]]
[[:name :asc] [:age :desc] [:height :asc]]


:name
[:name :age]
[:name :age :height]

[:name :desc]
[[:name :desc] [:age :desc]]
[[:name :desc] [:age :desc] [:height :desc]]

[:name [:age :desc]]
[:name [:age :desc] :height]


