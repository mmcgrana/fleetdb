Server CLI
==========

Once you have [downloaded](...) FleetDB, you can start a server as follows:

    $ java [jvm-options] -cp fleetdb-standalone.jar fleetdb.server [fleetdb-options]

The available FleetDB options are:

    -f <path>   Path to database log file                                          
    -e          Ephemeral: do not log changes to disk                              
    -p <port>   TCP port to listen on (default: 3400)                              
    -a <addr>   Local address to listen on (default: 127.0.0.1)                    
    -t <num>    Maximum number of worker threads (default: 100)                    
    -i <name>   Client/server protocol: one of {text,binary,bert} (default: bert)
    -h          Print this help and exit.

For example, to start a server that writes to a file at `/tmp/data.fdb`, use:

    $ java -cp fleetdb-standalone.jar fleetdb.server -f /tmp/data.fdb

You may also want to specify extra JVM options. For example, to run FleetDB in a "server" mode JVM with 2 gigabytes of heap space:
    
    $ java -server -Xmx2g -cp fleetdb-standalone.jar fleetdb.server -f /tmp/data.fdb
