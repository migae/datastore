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
(def k [:A/B :C/D]) ;; a keychain
(def em (entity-map k {:a 1}))
(is  (coll?em))
(is  (map?em))
(is  (entity-map?em))
```

### keychains

```
(def m {:a 1})
(entity-map [:Foo/Bar] m) ;;  kind="Foo",  identifier (name) ="Bar"
(entity-map [:Foo/d10] m) ;;  identifier (id) =  10
(entity-map [:Foo/x0A] m) ;;  identifier (id) =  10 (hex A)
(entity-map [(keyword "Foo" "10")] m) ;; same
(def em1 (entity-map [:Foo] m)) ;; kind="Foo", id is autogenned.  SIDE EFFECT: empty entity put to ds
(def em2 (entity-map [:Foo] m)) ;; em1 and em2 have different key ids
(entity-map [:A/B :C/D :E/F :G/H :I/J] m) ;; keychains can be long; only one entity created
```

### kinds

In the datastore, kinds are strings; in migae, kinds are keywords.

```
(= (kind (entity-map [:Foo/Bar] {:a 1})) :Foo)
(= (kind (entity-map [:Foo/Bar :X/d3] {:a 1})) :X)
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
