(use 'clj-unit.core)

(set! *warn-on-reflection* true)
(require 'fleetdb.server)

(require-and-run-tests
  'fleetdb.compare-test
  'fleetdb.json-test
  'fleetdb.core-test
  'fleetdb.embedded-test
  'fleetdb.client-test
)
