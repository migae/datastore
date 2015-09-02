# migae - clojurizing the appengine datastore

*CAVEAT* late Aug 2015: still working on code and doc; this will give
 you the general idea, that's all.  Some recent documentation is at
 [Entities](doc/Entities.md), [Keychains](doc/Keychain.md), and
 [Datastore](doc/Datastore.md).

Very much a work in progress.  Works well enough to have some fun
playing around.  Not packaged on clojars, to experiment, clone the
repo and run the tests.  Not much documentation at the moment, but
lots of simple tests (see esp. test/migae.tutorial.clj) that
demonstrate the semantics.

## getting started

Fork/clone the library.  The project is configured to load
`dev/user.clj` on repl startup.  That file sets up the GAE datastore
local test environment and defines a few vars to save typing during
experimentation.  So from the root directory, fire up a repl and start
experimenting:

```clojure
$ lein repl
nREPL server started on port 52304 on host 127.0.0.1 - nrepl://127.0.0.1:52304
REPL-y 0.3.5, nREPL 0.2.6
Clojure 1.7.0
Java HotSpot(TM) 64-Bit Server VM 1.8.0_45-b14
migae datastore repl.  (ds-reset) to reinitialize test datastore.
user=> k   ;; predefined in dev/user.clj
[:A/B]
user=> m   ;; predefined in dev/user.clj
{:a 1}
user=> em   ;; predefined in dev/user.clj
{:a 1}
user=> (meta em)
{:migae/key [:A/B], :type migae.datastore.EntityMap}
user=> (type em)
migae.datastore.EntityMap
user=> (println (.entity em))
\#object[com.google.appengine.api.datastore.Entity 0x78859a2b <Entity [A("B")]:
	a = 1
>
]
nil
user=> (ds/keychain? k)   ;; ds loaded by dev/user.clj
true
user=> (ds/entity-map? em)
true
user=> (ds/keychain em)
[:A/B]
user=> (key em)
[:A/B]
user=> (val em)
{:a 1}
user=> (keys em)
(:a)
user=> (vals em)
(1)
user=> (ds-reset)
\#object[com.google.appengine.tools.development.testing.LocalServiceTestHelper 0x7d7d7520 "com.google.appengine.tools.development.testing.LocalServiceTestHelper@7d7d7520"]
user=> (ds/entity-map k m)    ;; local constructor
{:a 1}
user=> (ds/entity-map! k m)   ;; push constructor - saves to datastore
Sep 02, 2015 6:38:18 AM com.google.appengine.api.datastore.dev.LocalDatastoreService init
INFO: Local Datastore initialized:
	Type: Master/Slave
	Storage: In-memory
{:a 1}
user=> (ds/entity-map* k m)   ;; pull constructor - retrieves matches from datastore
{:a 1}
```

If you get `NullPointerException No API environment is registered for
this thread.` then run `user=> (ds-reset)`.

## testing

See the tests for lots of examples of how to use the library.  Be **be
forewarned**, the test cases are in flux; some of them are outdated,
and the api is changing.

## types

* `migae.datastore.EntityMap` - migae representation of underlying `com.google.appengine.api.datastore.Entity`.  Implements IPersistentCollection, IPersistentMap, IMapEntry, etc.  See [Entities](doc/Entities.md).
* `migae.datastore.Keychain`  - migae representation of underlying `com.google.appengine.api.datastore.Key`.  A vector of Clojure keywords.  See [Keychains](doc/Keychains.md).  (**Not yet implemented**)

These are represented in migae as `entity-map` and `keychain`, respectively.

## construction

We have three ways to construct EntityMap objects:

* local constructor:  `(entity-map <keychain> <map>)`
* push constructor:   `(entity-map! <keychain> <map>)` - construct locally and push to datastore
* pull constructor:   `(entity-map* <keychain> <map>)` - pull matching entities from datastore and construct corresponding EntityMap objects locally


```
(def em (entity-map [:A/B C/D] {:a 1 :b 2}))
(is  (coll? em))
(is  (map? em))
(is  (entity-map? em))
(is (= (key  em) [:A/B :C/D]))
(is (= (val  em) {:a 1 :b 2}))
(is (= (keys em) [:a :b]))
(is (= (vals em) [1 2]))
```

### Keys, keychains, keylinks, and dogtags

**_Caveat_**: note the difference between a datastore Key and a
  clojure key.  The former is a type (class), the latter is a
  structural role (first element of a mapentry).

A keylink is a namespaced keyword, e.g. `:Foo/Bar`.  This corresponds
to a datastore Key, which has a Kind of type String and an Identifier,
which can be either a String name or a long Id.  See
[Keychains](doc/Keychain.md) for more detail.

A proper keychain is a vector of namespaced keywords.  To use numeric
Ids, include a notational prefix, 'd' for decimal and 'x' for
hexadecimal.  E.g. `[:Foo/d11]` or `[:Foo/x0B]`.

The last link in the chain is the _dogtag_, so named because it serves
as a (quasi-) identifier for its entity-map.  A dogtag is just a
Clojure keyword with namespace (e.g. [:A/B]); it corresponds to the
datastore Key of the underlying datastore Entity.  The Key of an
Entity does identify it, because it contaiins a link to its parent
key; but a dogtag does not completely identify its entity-map, since
it contains no link to its predecessor.  In migae, the "key" of an
entity-map is the entire keychain.  However, the kind and identifier
(name or id) of the dogtag do characterize the entity-map.

Note that a dogtag predicate `(dogtag? x)` doesn't make sense - it's
not a type.  What makes a keyword a dogtag is its position in a
keychain.

```
(ds/ekey? (ds/to-ekey :A/B)) ; migae keylink to datastore entity Key (ekey)
(is (= (ds/dogtag [:A/B]) (ds/dogtag [:X/Y :A/B]) ;; dogtag is last link in chain :A/B
(is (= (ds/keychain (ds/to-ekey :A/B)) [:A/B]))
(is (= (ds/kind [:A/B]) (ds/kind [:X/Y :A/B])))
      (is (= (ds/name e1) (ds/name e2) (ds/name e3)))

```

### kinds

In the datastore, kinds are strings; in migae, kinds are keywords.

```
(= (kind (entity-map [:Foo/Bar] {:a 1})) :Foo)
(= (kind (entity-map [:Foo/Bar :X/d3] {:a 1})) :X)
```

### autogenned ids

Use a partial ("improper") keychain to have the datastore autogen Id
values.  All but the last links in the vector must be namespaced; e.g. `[:A/B :C/D :E]`.

```
(def em1 (entity-map [:Foo] m)) ;; kind="Foo", id is autogenned.  SIDE EFFECT: empty entity put to ds
(def em2 (entity-map [:Foo] m)) ;; em1 and em2 have different key ids
(def em2 (entity-map [:A/B :C/D Foo] m)) ;; long keychains ok too
```

### field types

The value part of an entity-map is just a map.  The datastore
restricts the permissible value types; see  [Properties and value types](https://cloud.google.com/appengine/docs/java/datastore/entities#Java_Properties_and_value_types)

```
(entity-map [:Foo/Bar] {:a 1})  ;; java.lang.Long
(entity-map [:Foo/Bar] {:a 1.0})  ;; java.lang.Double
(entity-map [:Foo/Bar] {:a true})  ;; java.lang.Boolean
(entity-map [:Foo/Bar] {:a "baz"})  ;; java.lang.String
(entity-map [:Foo/Bar] {:a :b})  ;; keywords (stored as String)
(entity-map [:Foo/Bar] {:a 'b})  ;; symbols (stored as String)
(entity-map [:Foo/Bar] {:a [1 2 3]})  ;; vectors
(entity-map [:Foo/Bar] {:a '(1 2)})   ;; lists
(entity-map [:Foo/Bar] {:a {:b :c}})  ;; maps
(entity-map [:Foo/Bar] {:a #{1 'b "c"}})  ;; sets
(entity-map [:Foo/Bar] {:a {:b [1 {:c true}]})  ;; mixed, nested
(entity-map [:Foo] {:a {:b :c}
                    :b [1 2]
                    :c '(foo bar)
                    :d #{1 'x :y "z"}})
```

Datastore field types:
```
(entity-map [:Foo/bar] {:int 1 ;; BigInt and BigDecimal not supported
                        :float 1.1
                        :bool true
						:string "I'm a string"
                        :today (java.util.Date.)
                        :email (Email. "foo@example.org")
                        :dskey [:A/B :C/D] ;; foreign key
                        :link (Link. "http://example.org")
						;; TODO: EmbeddedEntity (not same as map value)
                        ;; TODO: Blob, ShortBlob, Text, GeoPt, PostalAddress, PhoneNumber, etc.
                        })
```

TODO: support all datastore property types.

## retrieval

We treat the datastore as just another map: `(get datastore k)`
retrieves the entity-map whose keychain is `k`.  Since there is only
one datastore, we sugar this to `(get-ds k)`.

### experimental:  co-construction




```
(entity-map* [:A/B]) ;; "co-constructs" (retrieves) entity with key [:A/B] if it exists, otherwise throws exception
```

## queries

**NB**: these query patterns are experimental and very likely to change.

Query syntax looks like constructor syntax; the difference is we treat
the map part as a pattern.

Conventions: terminal '?' means "predicate"; '?' followed by another
char means "find".

* `(entity-map?! k m)` - find ('?'), necessarily ('!').
 * if `k` is a proper keychain, then:
  * if no entity with key `k` exists in the datastore, create and store it
  * if an entity with key `k` does exist, return it (ignoring m argument)
 * if `k` is an improper keychain, then '?!' means "find _some_ entity", so:
  * create and save a new entity with autogenned key id

## mutation

```
(into-ds (entity-map [:A/B] {:a 1})) ;; non-destructive: fail if already exists
(into-ds! (entity-map [:A/B] {:a 1})) ;; destructive: replace existing
```

Patterns:

* augmentation: add a field, or add a value to a field
* replacement:  replace value of a field, replace entire entity
* removal:  delete a field or entity

Note that datastore fields may be singletons or collections.  So for
example you can start by storing an int, and then you can add another
value to the field, effectively converting it from type int to type
collection.  So there are three kinds of change that can apply to a
field: change the value, or augment it by adding another value, or
remove it.

This clashes a little bit with Clojure abstractions.  For example,
`into` replaces stuff.  That's fine, but we also need a way to
augment, so we'll have to spell that out - call it `onto`?.

```
(let [e (get-ds [:A/B])
      e2 (into e {:foo "bar"})] ;; std clojure.core/into: replace val at :foo, or add if not present
  (into-ds! e2)) ;; replace e
```

augmentation:

```
(let [e (get-ds [:A/B])
     (e2 (ds/augment {:foo 27}))] ;; turn {:foo "bar"} into {:foo ["bar" 27]}
  (into-ds! e))
```
