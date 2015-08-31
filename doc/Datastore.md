# migae Datastore

### API summary

* `(into-ds! e)` - destructively save [entity](Entities.md) `e` into the datastore;
  any previously saved version of `e` will be replaced.
* `(get-ds k)` - retrieve the entity whose [keychain](Keychain.md) is `k`.
* `(datastore k)` - retrieve the entity whose keychain is `k`.
* `(k datastore)` - retrieve the entity whose keychain is `k`.

# overview

The GAE datastore is a collection of _Entities_; an Entity extends
PropertyContainer (i.e. a Map) by adding a Key.

Conceptually the Datastore can be thought of as a map whose domain is
a set of Key objects, and whose domain is a collection of maps.  We
can think of an Entity as a MapEntry, but since all entities have the
same (key, map) structure, we can also treat an Entity as a Map - see
[Entities](Entities.md) for more info.  Clojure allows us the express
this conceptual structure more-or-less directly, in contrast to the
low-level Java API, which involves lots of classes that tend to
obscure the structure of the datastore (IMO).

For example, to save an entity `e` using the native API, one obtains a
DatastoreService object `dss`, and then calls `dss.put(e)`.  To fetch
an entity by key, one calls `dss.get(k)`.

In Clojure we can express this in standard abstractions, using `into`,
for example: `(into dss e)`.  But since there is only one datastore,
we can abbreviate this a bit: `(into-ds e)`.  And since the datastore
is mutable, we use `!`, giving `(into-ds! e)`.

To fetch an entity by key, we treat the datastore as an ordinary map:
`(let [e (get ds k)]...)`.  But here again we don't need a `ds`
object; we can just write `(get-ds k)`.  Alternatively, since the
(migae) datastore is a map, we can write `(datastore k)` or `(k
datastore)`.

# updating

Datastore does not support selective updating of portions of an
Entity.  The unit of change is the entire Entity; if you want to
change one field in an entity, you must retrieve the entire Entity,
change the field in the Entity object, and then save the entire
Entity.  This will replace the previously stored Entity _in toto_.

# namespaces

Unlike most database systems, GAE datastore does not allow creation of
multiple databases: there is only one datastore.  However, it does
support _namespaces_, which allows us to partition data in useful
ways.  The mechanism is pretty simple and transaction-like: datastore
operations always use the "current" namespace, so to segregate data
just
[set the (global) namespace](https://cloud.google.com/appengine/docs/java/multitenancy/multitenancy#Java_Setting_the_current_namespace)
before you start working with the datastore.

A Clojurish way to handle this sort of situation is to use a
`with-namespace` operation.  So we might write:

```clojure
(with-namespace "foo"
    (into-ds! (entity-map [:A/B] {:a 1}))
(with-namespace "bar"
    (into-ds! (entity-map [:A/B] {:a 1}))
```

Here we have inserted identical entities into distinct namespaces.
**_WARNING_**: _entity construction must occur within a `with-namespace`
expression_.  Namespacing is implemented in the Key and Query
objects - in effect, a namespace is written into each entity as a
component of its Key.

See [Using namespaces with the Datastore](https://cloud.google.com/appengine/docs/java/multitenancy/multitenancy#Java_Using_namespaces_with_the_Datastore)

# Java low-level APIs

[Datastore API](https://cloud.google.com/appengine/docs/java/javadoc/com/google/appengine/api/datastore/package-summary) summary

[DatastoreService](https://cloud.google.com/appengine/docs/java/javadoc/com/google/appengine/api/datastore/DatastoreService)

[Entity](https://cloud.google.com/appengine/docs/java/javadoc/com/google/appengine/api/datastore/Entity)

[Namespaces](https://cloud.google.com/appengine/docs/java/multitenancy/) (a/k/a Multitenancy)
