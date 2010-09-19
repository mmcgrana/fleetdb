## 0.3.1 (2010-09-19)

* Allow `limit` and `offset` to be 0.

## 0.3.0 (2010-08-30)

* Clojure 1.2 compatibility

## 0.2.0 (2010-07-15)

* No user-facing changes

## 0.2.0-RC2 (2010-07-09)

* Put temporary snapshot and compact files in the same directory as the corresponding main files instead of /tmp
* Fix bug in in checking of empty logs
* Fix bug in compaction
* Provide more visibility into errors occurring in spawned threads

## 0.2.0-RC1 (2010-03-06)

* Introduce `not-in` where condition
* Introduce `distinct` query qualifier
* Introduce `clear` query
* Optimize `count` queries
* Introduce `drop-collection` query
* More explicitly handle type mismatch errors
* Check the log for integrity on startup, repairing if necessary
* Allow comparison of boolean values
* Introduce `-x <password>` command line option to require client authentication
