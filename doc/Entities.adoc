# migae entity-maps

migae.datastore.EntityMap: a deftype that implements
protocols/interfaces to make it behave like a clojure map (extended so
it also behaves like a map entry).  The actual Datastore Entity is
stored as a field in the EntityMap.

### api

* `(entity-map [:A/B :C/D] {:a 1})` => creates
  migae.datastore.EntityMap object that contains an Entity object.
* `(map? (entity-map [:A/B] {:a 1}))` => true
* `(entity-map? (entity-map [:A/B] {:a 1}))` => true

The standard operations for collections and maps are supported; see
the test suite for lots of examples using e.g. `into`, `assoc`,
`merge`, etc.

*Caveat*: the Entity key is treated just like any key; for example, if
 you say `(into em1 em2)` the result will be a copy of em1 augmented
 by em2, with em2 vals replacing em1 vales where keys match.  Since
 both have a datastore key (`:migae/key`), the datastore key of the
 result will be that of em2, not em1.  TODO: more on the rational for
 this design choice.


Note that EntityMap behaves like a map and also like a MapEntry.  That
is, it supports `keys` and `vals`, but also `key` and `val`.  The
justification for this is that technically speaking we should treat an
Entity as a MapEntry; but the type signature of an Entity is fixed; it
always has a key of type Key, and a value whose type is effectively
Map.  So we treat it as a kind of specialized map, one with a
distinguished key (`:migae/key`).  And that's just because we're lazy;
we want to write `(:foo em)` instead of `(:foo (val em))`, and `(keys
em)` instead of `(keys (val em))`, etc.

In other words, instead of treating an Entity as a MapEntry of form
[Key,PropertyContainer], we (conceptually) inject/pivot the key into
the PropertyContainer as the value associated with a disinguished key
(:migae/key) so we can treat it as a Map.

**CAVEAT**: the standard map/coll ops are not all fully implemented or
  tested, but see the test suite.

# mutability

The datastore is mutable.  Entities in the datastore are quasi
immutable; you can replace them, but you cannot change them (i.e. by
mutating a field in an Entity).  Entities in your program space are
fully mutable, but EntityMaps are not.  To change a saved
Entity, retrieve it to an entity-map and use standard map/collection
operations to "update" it - i.e. to generate new entity-maps.  Then
put the new entity-map into the datastore using the original key, and
you will replace the old Entity in the datastore.


# design/implementation notes
*CAVEAT* These are old notes.

EntityMap implements:

* clojure.lang.IFn
* clojure.lang.ILookup
* clojure.lang.IMapEntry
* clojure.lang.IMeta
* clojure.lang.IObj
* clojure.lang.IPersistentCollection
* clojure.lang.IPersistentMap
* clojure.lang.IReduce
* clojure.lang.ITransientCollection
* clojure.lang.Associative
* clojure.lang.Indexed
* clojure.lang.Seqable
* java.lang.Iterable
* java.util.Map$Entry

EntityMap - design goal is to have DS entities behave just like
ordinary Clojure maps.  E.g. for ent.getProperty("foo") we want to
write (ent :foo); instead of ent.setProperty("foo", val) we want
either (assoc ent :foo val), (merge ent :foo val), dissoc, etc.

Implementation alternatives:

* eager: store Entity object in field of EntityMap (this is the current design)
* lazy: store a clojure pair [keychain valmap] in Entity map, and only
  convert to Entity on demand, i.e. when saving to datastore.
  Conversely, when retrieving an Entity, store it in the EntityMap and
  only convert to Clojure on demand (as currently done).  For the lazy
  strategy we could try two fields in the EntityMap, one for pure
  clojure structures and one for the corresponding Entity, with
  conversion on demand.

One strategy: use ordinary clj maps with key in metadata, then define
funcs to convert to Entities at save time.  In this case the map is
pure clojure, and "glue" functions talk to gae/ds. This would require
something like dss/getEntity, dss/setEntity.  It would also require
conversion of the entire Entity each time, all at once.  I.e. getting
an entity would require gae/ds code to fetch the entity, then iterate
over all its properties in order to create the corresponding map.
This seems both inefficient and error prone.  We might be interested
in a single property of an entity that contains dozens of them -
translating all of them would be a waste.

Strategy two: deftype a class with support for common map funcs so it
will behave more or less like a map.  In this case the data struct
itself wraps gae/ds functionality.  Access to actual data would be
on-demand (JIT) - we don't convert until we have an actual demand.

SEE http://david-mcneil.com/post/16535755677/clojure-custom-map

deftype "dynamically generates compiled bytecode for a named class
with a set of given fields, and, optionally, methods for one or more
protocols and/or interfaces. They are suitable for dynamic and
interactive development, need not be AOT compiled, and can be
re-evaluated in the course of a single session.  So we use deftype
with a single data field (holding a map) and the protocols needed to
support a map-like interface.

The problem is that there doesn't seem to be a way to support
metadata, which we need for the key.  Also the doc warns sternly
against mutable fields.  But do we really need metadata?  Can't we
just designate a privileged :key field?  The only drawback is that
this would become unavailable for use by clients - but so what?
Solution: namespace it, :migae/key

CORRECTION: we don't need any metadata.  Just store the Entity in a
field ("entity"!).

NB: defrecord won't work - no way to override clojure interfaces
NB: gen-class won't work - Entity is final

Other notes:

Update (late March 2015): major change in approach.  On my first try I
used ordinary metadata to wrap the datastore stuff in clojure.  I've
since learned a lot more about how to implement Clojure interfaces, so
this version abandons the use of metadata on plain Clojure maps in
favor of deftypes (EntityMap, EntityMapCollIterator) that behave just
like plain Clojure data structures.  For example:

```
;; emap!! - retrieve if exists, overriding props, otherwise create and save
    (let [em1 (ds/emap!! [:Foo/Bar] {:a 1, :b 2}) ;; kind: "Foo", name: "Bar"
          em2 (ds/emap!! [:Foo] {:a 1}) ;; kind: Foo, id auto-generated
          em3 (ds/emap!! [:A/B :C/D] {:c [1 2] :d #{'sym1 'sym2} })
          em4 (into em1 em2) ;; =>  [:Foo/Bar] {:a 1 :b 2 :c [1 2] :d #{'sym1 'sym2}}
          em5 (assoc em3 {:z "foo"})
		  em6 (merge e2 {:fld1 :val1} {:fld2 "val2"})
		  ;; nested vals automatically converted to Java types acceptable to datastore:
          em7 (ds/emap!! [:Foo/d99] {:a {:a1 'a2} :b #{:b1 :b2} :c [1 2]}) ; id: 99 (long)
		  a (:a em1)
		  b (em1 :b)
		  ...etc...
```

This approach involves a certain amount of overhead; everything must
be converted/deconverted to/from appropriate types; for example,
keywords in the data must be converted to Key objects and stored as
property vals, map vals must be converted to EmbeddedEntity objects,
and so forth.  But the payoff is a GAE datastore interface that makes
working with GAE data largely indistinguishable from working with
plain Clojure data.
