(use 'clj-unit.core)

(def base-tests ['fleetdb.compare-test
                 'fleetdb.core-test
                 'fleetdb.embedded-test])

(def server-test 'fleetdb.server-test)

(apply require-and-run-tests
  (if (= (first *command-line-args*) "--no-server")
    base-tests
    (conj base-tests server-test)))
