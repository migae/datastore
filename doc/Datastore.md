# migae Datastore

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
  (let [e (entity-map [:A/B] {:a 1})]
    (into-ds! e)))
(with-namespace "bar"
  (let [e (entity-map [:A/B] {:a 1})]
    (into-ds! e)))
```

# Java low-level APIs

[Datastore API](https://cloud.google.com/appengine/docs/java/javadoc/com/google/appengine/api/datastore/package-summary) summary

[DatastoreService](https://cloud.google.com/appengine/docs/java/javadoc/com/google/appengine/api/datastore/DatastoreService)

[Entity](https://cloud.google.com/appengine/docs/java/javadoc/com/google/appengine/api/datastore/Entity)

[Namespaces](https://cloud.google.com/appengine/docs/java/multitenancy/) (a/k/a Multitenancy)
