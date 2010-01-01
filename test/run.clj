(use 'clj-unit.core)

(require 'fleetdb.server)

(require-and-run-tests
  'fleetdb.compare-test
  'fleetdb.core-test
  'fleetdb.embedded-test
  ;'fleetdb.server-test
)
