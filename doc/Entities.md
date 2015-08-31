# migae entity-maps

*CAVEAT* These are old notes.

EntityMap: a deftype that implements protocols/interfaces to make it
behave like a clojure map (extended so it also behaves like a map
entry):

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
