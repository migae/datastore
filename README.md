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
(entity-map [:Foo/Bar] m) ;;  name identifier "Bar"
(entity-map [:Foo/d10] m) ;;  long identifier 10
(entity-map [:Foo/x0A] m) ;;  long identifier 10 (hex A)
(entity-map [(keyword "Foo" "10")] m) ;; same
(def em1 (entity-map [:Foo] m)) ;; long identifier autogenned
(def em2 (entity-map [:Foo] m)) ;; em1 and em2 have different key identifiers
```

### field types
```
(entity-map [:Foo/Bar] {:a 1})  ;; ints
```

## mutation

```
(into-ds (entity-map [:A/B] {:a 1}))
```
