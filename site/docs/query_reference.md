Query Reference
===============

* [insert](#insert)
* [select](#select)
* [count](#count)
* [delete](#delete)
* [update](#update)
* [explain](#explain)
* [list-collections](#list-collections)
* [create-index](#create-index)
* [drop-index](#drop-index)
* [list-indexes](#list-indexes)
* [multi-read](#multi-read)
* [multi-write](#multi-write)
* [checked-write](#checked-write)
* [ping](#ping)
* [compact](#compact)
* [snapshot](#snapshot)


<h2 id="insert">insert</h2>

    [:insert <collection> <record>|<records>]
    <count>
    
    [:insert :people {:id 1 :name "Bob"}
    1
    
    [:insert :people [{:id 1 :name "Bob"} {:id 2 :name "Amy"}]]
    2

Insert `record` or `records` into the `collection`. `record` is a map, `records` a vector of maps.


<h2 id="select">select</h2>

    [:select <collection> <find-options>?]
    <records>
    
    [:select :people]
    [{:id 1 :name "Bob"} {:id 2 :name "Amy"} ...]
    
    [:select :people {:where [:= :name "Bob"]}]
    [{:id 1 :name "Bob"}]
    
    [:select :people {:offset :limit 2 :only [:id :name]}]
    [[1 "Bob"] [2 "Amy"]]
    
    [:select :people {:order [:name :asc] :limit 1 :only :name}]
    ["Bob"]

Retrieve records from the `collection`, optionally qualified by the `find-options`.

`find-options` is a map of options that describe the search parameters. Recognized options are `:where`, `:order`, `:offset`, `:limit`, and `:only`.

`:offset` and `:limit` are integer-valued  options that have the same behavior as the SQL operators of the same name.

':only' is given as a vector of attribute names or as a single attribute name. If it is a vector, returned records are represented as vectors of attribute values in the same order. If `:only` is a single attribute, returned records are represented by the corresponding value.

`:order` specifies the sort order in which the records should be retrieved. Records can be ordered by one attribute or by multiple attributes:

    [<attribute> <:asc|:desc>]
    
    [[<attribute> <:asc|:desc>]+]

The following are examples of `:order` option values:
    
    [:name :asc]
    
    [[:name :asc] [:age :desc]]

If `:order` is not given, the ordering of the records in the result is unspecified.

`:where` describes the criteria by which records should be filtered:

    [:=    <attribute> <value>]
    [:!=   <attribute> <value>]
    [:<    <attribute> <value>]
    [:<=   <attribute> <value>]
    [:>    <attribute> <value>]
    [:>=   <attribute> <value>]
    [:in   <attribute> [<value>+]]
    [:><   <attribute> [<low-value> <high-value>]]
    [:>=<  <attribute> [<low-value> <high-value>]]
    [:><=  <attribute> [<low-value> <high-value>]]
    [:>=<= <attribute> [<low-value> <high-value>]]
    [:or   <where-condition>+]
    [:and  <where-condition>+]

The following are examples of `:where` conditions:

    [:= name "Bob"]
    
    [:in :name ["Bob" "Amy"]]
    
    [:>=< :age [30 40]]
    
    [:and [:in :name ["Bob" "Amy"]] [:>=< :age [30 40]]]

If no `find-options` are given, `:select` returns all records in the collection.

<h2 id="count">count</h2>

    [:count <collection> <find-options>?]
    <count>
    
    [:count :people]
    25
    
    [:count :people {:where [:> :age 30]}]
    14

Returns the number of records in the `collection` that match the `find-options`. 

`find-options` are of the same form as for [select](#select) queries, except that the `:only` option is not recognized.

<h2 id="delete">delete</h2>

    [:delete <collection> <find-options>?]
    <count>
    
    [:delete :people]
    25
    
    [:delete :people {:where [:= :name "Bob"]}]
    1
    
Removes all records from the `collection` that match the `find-options`. Returns the number of deleted records.

`find-options` are of the same form as for [select](#select) queries, except that the `:only` option is not recognized.

<h2 id="update">update</h2>

    [:update <collection> <update-map> <find-options>?]
    <count>
     
    [:update :people {:vip true} {:where [:= :name "Bob"]}]
    1

Updates records in the `collection` by merging `update-map` into any records that match `find-options`. Returns the number of updated records.

If an attribute is present in both `update-map` and a matching record, the updated record assumes the value from the `update-map`. If an attribute is valued with `nil` in the update map, that attribute will not be present in the updated record.

`find-options` are of the same form as for [select](#select) queries, except that the `:only` option is not recognized.

<h2 id="explain">explain</h2>

    [:explain <query>]
    <query-plan>
    
    [:explain [:select :people]]
    [:record-scan :people]
    
    [:explain [:select :people {:where [:= :name "Bob"] :order [:age :asc]}]]
    [:sort [:age :asc]
         [:index-lookup [:people :name "Bob"]]]

Returns the query plan that will used by the database to execute the given `query`.

`query` must be on of `:select`, `:count`, `:update`, or `:delete`.


<h2 id="list-collections">list-collections</h2>

    [:list-collections]
    <collections>
    
    [:list-collections]
    [:people :friendships]

Returns a vector of names of non-empty collections in the database. A collection is defined to be non-empty if it has a positive number of records or a positive number of indexes.


<h2 id="create-index">create-index</h2>

    [:create-index <collection> <index-spec>]
    <0|1>
  
    [:create-index <collection> :name]
    1
    
    [:create-index <collection> [:name [:age :desc]]]
    1
    
Ensures that the index described by `index-spec` exists on the given collection. Returns 1 if such an index is created or 0 if it already exists.

`index-spec` may indicate an index on 1 attribute or on multiple attributes.

    <attribute>
    
    [<attribute>|[<attribute> <:asc|desc>]+]

If an order is not given for an attribute in a multi-attribute index, it is assumed to be ascending.

The following are examples of `index-specs`:
    
    :name
    
    [:name [:age :desc]]
    
    [[:name :desc] [:age :asc]]

([top](#top))


<h2 id="drop-index">drop-index</h2>

    [:drop-index <collection> <index-spec>]
    <0|1>
    
    [:drop-index :people :name]
    1
    
Ensures that the index described by `index-spec` does not exist on the given collection. Returns 1 if the index existed and was removed or 0 if it did not exist.

`index-spec` is the value as was given when the index was created with [create-index](#create-index).


<h2 id="list-indexes">list-indexes</h2>

    [:list-indexes <collection>]
    <index-specs>

    [:list-indexes :people]
    [:name :age]

Returns a vector of index specs indicating all indexes on the given 'collection'.

The elements of 'index-specs' are of the same form as the 'index-spec' values given for [create-index](#create-index) and [drop-index](#drop-index) queries.


<h2 id="multi-read">multi-read</h2>

    [:multi-read [<read-query>+]]
    [<read-result>+]
    
    [:multi-read 
      [[:count :people {:where [:= :age 30]}]
       [:count :people {:where [:= :name "Bob"]}]]]
    [4 1]

Executes the given read queries on a single snapshot of the database, returning a vector of their respective results.

Each `read-query` must be either a [select](#select), [count](#count), [explain](#explain), [list-collections](#list-collections), [list-indexes](#list-indexes), or [multi-read](#multi-read). The queries may operate on different collections.


<h2 id="multi-write">multi-write</h2>

    [:multi-write [<query>+]]
    [<query-result>+]
    
    [:multi-write
      [:select :people {:where [:= :counted false]}]
      [:update :people {:counted true} {:where [:= :counted false]}]]
    [[{:id 1 :name "Bob"} {:id 2 :name "Amy"}] 2]
    
Executes the given queries atomically and in order on the current snapshot of the database. Unlike [multi-read](#multi-read), this query allows write queries as well as read queries.


<h2 id="checked-write">checked-write</h2>

    [:checked-write <read-query> <expected-read-result> <write-query>]
    [true  <write-result>] | [false <actual-read-result>]
    
    [:checked-write
      [:count :registrations {:where [:= :person_id 2]}]
      6
      [:insert :registrations {:id 13 :person_id 2 :event_id 4}]]
    [true 1] | [false 7]

First, executes `read-query` and compares the result to `expected-read-result`. If the two are equal, then executes `write-query` and returns the result prefixed by `true`. If the actual and expected read results differ, does not execute `write-query` and instead returns the actual read result prefixed by `false`.

`read-query` must be one of the queries listed for [multi-read](#multi-read). `write-query` must be one of [insert](#insert), [update](#update), [delete](#delete), [create-index](#create-index), [drop-index](#drop-index), or [multi-write](#multi-write).


<h2 id="ping">ping</h2>

    [:ping]
    "pong"

Tests if the server is alive.


<h2 id="compact">compact</h2>

    [:compact]
    true

Asynchronously compacts the database log file.


<h2 id="snapshot">snapshot</h2>

    [:snapshot <path>]
    true
    
    [:snapshot "/var/data/db-002.fdb"]
    true

Asynchronously writes a snapshot of the current database to the specified `path`.


