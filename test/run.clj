(use 'clj-unit.core)

(require-and-run-tests
  'fleetdb.compare-test
  'fleetdb.io-test
  'fleetdb.core-test
  'fleetdb.embedded-test
  'fleetdb.client-test
)
