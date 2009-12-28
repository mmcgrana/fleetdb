Client Libraries
================

Client programs connect to a FleetDB server using a [BERT](http://bert-rpc.org)-based protocol. This is the default protocol for the server, so you don't need to specify it when starting the server from the [command line](...).

Clojure
-------

If you are using Clojure, you can use the client library included with the FleetDB distribution: `fleetdb.client`. 

    => (use '(fleetdb [client :as client]))
    => (def c (client/connect "127.0.0.1" 3400))
    
    => (client/query c [:ping])
    "pong"
    
    => (client/query c [:select :accounts {:where [:= :id 2]}])
    [{:id 2 :owner "Alice" :credits 150}]
    
Other BERT-Enabled Languages
----------------------------

Many languages have [BERT](http://bert-rpc.org) encoding libraries available, including [C++](http://github.com/ruediger/libbert), [Erlang](http://github.com/mojombo/bert.erl), [Factor](http://github.com/wookay/factor-bert), [Go](http://github.com/josh/gobert), [Haskell](http://github.com/mariusaeriksen/bert), [Javascript](http://github.com/rklophaus/BERT-JS), [Python](http://github.com/samuel/python-bert), [Ruby](http://github.com/mojombo/bert), and [Scala](http://github.com/stephenjudkins/scala-bert). You could use one these libraries to build a FleetDB client.

The FleetDB BERT protocol is very simple. The client sends a request as a BERP and receives a response as a BERP. The request BERP corresponds to the serialized data structure expressing the query. The response BERP is a 2-tuple containing an integer status code and a response value. A status code of `0` indicates success, `1` a client error, and `2` a server error. The second element is the query return value on success or an error message otherwise.

As an example, here is a simple Ruby client:

    require "socket"
    require "bert"
    
    class FleetClient
      def initialize(host, port)
        @socket = TCPSocket.new(host, port)
      end
    
      def query(q)
        bert = BERT.encode(q)
        @socket.write([bert.length].pack("N"))
        @socket.write(bert)
        header = @socket.read(4) || raise("disconnected")
        length = header.unpack("N").first
        bert = @socket.read(length) || raise("no data")
        status, result = BERT.decode(bert)
        status == 0 ? result : raise(result)
      end
    end

We would use the client as follows:

    require "fleet_client"
    
    c = FleetClient.new("127.0.0.1", 3400)
    
    c.query(t[:ping])
    # => "pong"
    
    c.query(t[:select, :accounts, {:where => t[:=, :id, 2]}])
    # => t[{:id=>2, :owner => "Alice", :credits => 150}]
    
    
    
