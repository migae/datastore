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

Note that EntityMap behaves like a map and also like a MapEntry.  That
is, it supports `keys` and `vals`, but also `key` and `val`.  The
justification for this is that technically speaking we should treat an
Entity as a MapEntry; but the type signature of an Entity is fixed; it
always has a key of type Key, and a value whose type is effectively
Map.  So we treat it as a kind of specialized map, one with a
distinguished key (`:migae/key`).  And that's just because we're lazy;
we want to write `(:foo em)` instead of `(:foo (val em))`, and `(keys
em)` instead of `(keys (val em))`, etc.

**CAVEAT**:  not fully implemented or tested, but see the test suite.

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
