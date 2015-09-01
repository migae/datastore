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


# examples

## construction

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

### keychains

A keylink is a namespaced keyword, e.g. `:Foo/Bar`.  This corresponds
to a datastore Key, which has a Kind of type String and an Identifier,
which can be either a String name or a long Id.  See
[Keychains](doc/Keychain.md) for more detail.

A proper keychain is a vector of namespaced keywords.  To use numeric
Ids, include a notational prefix, 'd' for decimal and 'x' for
hexadecimal.  E.g. `[:Foo/d11]` or `[:Foo/x0B]`.

```
(def m {:a 1})
(entity-map [:Foo/Bar] m) ;;  kind="Foo",  identifier (name) ="Bar"
(entity-map [:Foo/d10] m) ;;  identifier (id) =  10
(entity-map [:Foo/x0A] m) ;;  identifier (id) =  10 (hex A)
(entity-map [(keyword "Foo" "10")] m) ;; same
(entity-map [:A/B :C/D :E/F :G/H :I/J] m) ;; keychains can be long; only one entity created
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

TODO: support all datastore property types.  see [Properties and value types](https://cloud.google.com/appengine/docs/java/datastore/entities#Java_Properties_and_value_types)

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
augment, so we'll have to spell that out.

```
(let [e (get-ds [:A/B])
      e2 (into e {:foo "bar"})] ;; replace val at :foo, or add if not present
  (into-ds! e))
```

augmentation:

```
(let [e (get-ds [:A/B])
;; todo: turn {:foo "bar"} into {:foo ["bar" "baz"]}
  (into-ds! e))
```
