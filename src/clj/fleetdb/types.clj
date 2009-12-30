(ns fleetdb.types)

(def write-queries
  #{"insert" "update" "delete" "create-index" "drop-index"
    "multi-write" "checked-write"})

(def read-queries
  #{"select" "count" "explain" "list-collections" "list-indexes" "multi-read"})