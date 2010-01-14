(ns fleetdb.types)

(def write-queries
  #{"insert" "update" "delete" "create-index" "drop-index" "drop-collection"
    "multi-write" "checked-write" "clear"})

(def read-queries
  #{"select" "count" "explain" "list-collections" "list-indexes" "multi-read"})
