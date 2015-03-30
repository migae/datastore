# migae - clojurizing the appengine datastore

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

This approach involves a certain amount of overhead - keywords in the
data must be stored as Key properties, everything must be
converted/deconverted to/from appropriate types, etc.  But the payoff
is GAE datastore data that are largely indistinguishable from Clojure
data.

Very much a work in progress - expect some breakage - but works well
enough to have some fun playing around.  Not much documentation at the
moment, but lots of simple tests (see esp. test/migae.tutorial.clj)
that demonstrate the semantics.

# Ancestry - i.e. Keys, Names, and Namespaces

The identity of a datastore entity is determined by its key; two
entities are equal if they have equal keys.  In other words, the
Entity method "equals" tests for key equality.

The language of Keys is a little confusing and obscure.  Migae tries
to make things a little more explicit and clear.

Keys are composed of a pair of a Kind and an Identifier (either a
string name or a long id), plus a parent key.  This recursive
structure establishes an "ancestor path" for each key.  The native API
does not provide a "getAncestors" method; to construct the entire path
you have to recur using getParent.

Note that there is an inherent ambiguity here.  The key of an Entity
is a Java object that may refer to a parent key - another object.  But
Entities are not identified solely by their key object - it's the
entire chain of keys determined by the parent chain that functions as
the key of the entity.  The Entity's key object is actually the last
link in a chain of keys.  If you call getKey on an Entity, you don't
get the entire chain of key objects.  For that, you have to recur using
getParent.

In other words, when the DS doco says "Key", it often means something
like last node or link in a path or chain of Key objects.


The Kind of an entity is determined by the kind of its key, which is
to say by the kind of the last element in its keychain.  Ditto for its
Identifier (name or id).

This means that distinct Entities can have the same Kind and the same
Identifier, so long as they have distinct ancestor paths.  So we can
think of ancestor paths as determining a namespace.

In migae, we use the notion of a keychain to refer to the entire chain
of ancestor path plus entity Kind+Identifier, and we treat the latter
as the "name" of the Entity, the former as the namespace.  We
represent the keychain as a vector of Clojure keywords; the entire
keychain identifies the entity; the last element of the vector is the
"name" of the Entity, and the vector up to the last element represents
the ancestor path.  For example:


    [:Family/Felidae :Subfamily/Felinae :Genus/Felis :Species/Felis_catus :Cat/Chibi]

In this example, `:Cat/Chibi` is the "key node" or "name" of
the Entity, and `[:Family/Felidae :Subfamily/Felinae :Genus/Felis :Species/Felis_catus]` is
the "namespace" or ancestor path.  If you print this key from the ds
you get something like:

    [Family("Felidae")/Subfamily("Felinae")/Genus("Felis")/Species("Felis_catus")]

Migae wraps the gory details.  If you ask for the key of an Entity,
you get the entire keychain vector.(? - SUBJECT TO CHANGE) If you just
want the "name" part of the key (without the namespace, i.e. the
ancestor path), use ds/key-name.  ("name" is reserved for getting the
name component of a Key node).

[Note that the keys in a keychain need not be associated with actual Entities in the datastore.]

Migae uses keywords to encode Kinds and Identifiers.  The native
datastore uses String for Kinds, and either String or Long for
Identifiers ("name" and "id", respectively).

    Datastore API			migae
	Entity("MyKind")	->  (emap :Mykind) or (emap [:MyKind])
	Entity("MyKind", 99) ->  (emap :Mykind/d99) or (emap [:MyKind/d99]) or (emap (keyword "MyKind" "99"))
	Entity("MyKind, "Foo") -> (emap :Mykind/Foo) or etc.

Setting ancestry:

    Entity parent = new Entity("A", "B");
	Entity("MyKind", "Foo", parent) ->   (emap [:A/B :MyKind/Foo])

## EntityMap

See the test tutorial.clj for lots of examples.

EntityMap (aka emap) is a deftype that holds a reference to an Entity in field
"entity".  The user can always use the native API by doing `(.entity
em)` (where `em` is an EntityMap instance).  But the goal here is to
completely hide the Java api behind a natural Clojure idiom.

To get or make an emap use emap, emap!, or emap!!.  First arg must be
a vector of keywords that will serve as the key (all but the last must
have a namespace), second arg can be a literal map or a function that
takes an EntityMap and does things to it.

EntityMaps behave just like Clojure maps (except the implementation is
incomplete and unstable and may be buggy), except for the obvious
deviation: they're not immutable.  So if you `(into em {:foo "bar"}),
em - more precisely, the Entity it wraps - will be mutated.

CAVEAT: emap returns an EntityMap that wraps an Entity that has not
been stored.  emap! checks to see of the key is already in the
datastore; if so, fetchs it and ignores the second arg of the emap!
expression; emap!! fetches an existing Entity if there is one, the
adds/overrides its properties using the second arg, then saves the
result.

### Getting and putting

We're lazy so currently most if not all operations just go ahead and
`put` results to the datastore.  Coming up with a more sensible set of
policies and apis requires further research and experimentation; for
the moment, the main focus is on making the EntityMap behave just like
Clojure's maps, DS query results behave like a seq, etc.


# Obsolete

Below are notes I took on my first try.  Mostly abandoned.

Caveat: originally I was thinking in terms of the datastore ("ds/ds")
as a giant map, so putting a new entity would go like: (into ds/ds
theEntity). This is discussed below.  I've since changed my mind and
decided to tread ds/Entities as the giant map and to dispense with
"into" and the like, on grounds that there is only One True Datastore,
so all you can do is look up entities: (ds/Entities theKey).
Operators like "into" are not needed because (conceptually at least)
the DS already contains everything.  Obviously, the code behind the
curtain "creates" Entity POJOs as needed, but from the user's
perspective, there is only one way to use the ds/Entities map and that
is to use it to construct a referring name like so:

    (ds/Entities theKey)

NB: conceptually this is a (composed) name, not an "operation".

## Other Caveat

This doc contains a certain amount of thinking out loud, including
ideas I've since abandoned.  I'll get it updated someday but in the
meantime it should not be taken as a reliable description of what the
code actually does, now.  Read the source for that, *especially the
test cases in test/datastore/service_test.clj*.  They give examples of
all (most) of the ways of using keys and entities.  Read this doc for
some discussion of ideas and design alternatives.

### Concepts:

    ds/Keys, ds/Entities - platonic collections.  No object
    life-cycle, only naming.

        (ds/Entities theKey)
	(ds/Entities ^{:kind :Foo :id 99}{}

    Such expressions name entities; they do not create anything.  The
    library will create entities as needed behind the scenes, but
    conceptually such expressions merely refer with no implied process.

    In other words (ds/Entities <key or keymap>) conceptually refers,
    in practice causes a get and a create of the object is not found.

    However, sometimes we might want to determine if an entity has
    already been created.  ds/Entities always finds its entity since
    it maintains the fiction that entities are eternal.  So we need a
    way to ask if an entity has in fact been created.  But we shift
    the concept from "has it been created?" to "has it been
    referenced?", since a reference using ds/Entity causes object
    creation.  Ideally we would use an ordinary function like:

        (contains? ds/Entities theEntity)

    But since ds/Entities only emulates a Clojo we have to define our
    own operators:

        (ds/contains? ds/Entities theEntity)

    NB: on our conceptualization contains? etc. would always be true.
    Something like ready?, available? or used? would be more
    appropriate.  Or maybe:

    		  (ds/Entities? theEntity)
		  (ds/Entity? theEntity)

    And since Keys are used for such queries:

    	(ds/Entity? theKey)
	(ds/active? theKey)
	(ds/used? theKey)

    I think (ds/used? theKey) might be best; we're not asking whether
    an entity exists, we're asking whether a key has been used to
    refer to an entity.  Entity != Key.  So we never ask if an entity
    has been "created", only whether a key has been used to refer.
    From which we can infer that using it to refer will cause the
    referent to be created locally.

    (For "key" read "key or keymap")

# Datastore

# Keys

NB: the documentation is a little misleading insofar as it fails to
distinguish between "identifier" and "id".  The "identifier" part of a
key can be either a "name" or an "id" (NB: id != identifier).  The Key
api distinguishes between key "id" (numeric) and key "name" (string).
So the key schema is: kind/identifier/parentkey, where "identifier"
may be either a numeric Id or string name:

    kind/id/parentkey
or
    kind/name/parentkey

(NB where "parentkey" is "ancestor path".)


# Entities

Appengine entities are basically untyped (schema-less) maps.  The only
requirement is that each entity must have a key, which is a map whose
keys are "kind", "id", and, optionally "ancestor path".

The user must supply the kind and may optionally supply the id.

Design alternatives: currently aem defines entities in terms of a
protocol and supports use of a :key metadatum as well as a :kind
metadatum.  This is confusing, since kind is part of the key in the
Datastore.

Another possibility is to support use of ordinary clojure maps.  For
key/kind info, we could designate required keys (:kind, :id), or we
could designate required metadata so that any fields could be so-used.
E.g.  ^{:kind :foo, :id :myid}{...:foo "bar", ... :myid 9134 ...}

That way any clojure map could be saved to the datastore.

Problem: datastore entities can have multiple values of multiple types
for any field in an entity.  This would correspond to clojure map
entries whose values are collections of some sort.

Problem: using defrecord to model entities is overly restrictive.  You
can add or remove entity fields any time; can you do that with records?

I don't think so, anyway.  defrecord:

"Dynamically generates compiled bytecode for class with the given
name, in a package with the same name as the current namespace, the
given fields, and, optionally, methods for protocols and/or
interfaces.

The class will have the (immutable) fields named by
fields,..."

But this is a problem.  GAE datastore entities are not Java objects,
they don't have a Java class (entity kind != class) nor a type.

From the api docs:

"Entity is the fundamental unit of data storage. It has an immutable
identifier (contained in the Key) object, a reference to an optional
parent Entity, a kind (represented as an arbitrary string), and a set
of zero or more typed properties."

(But: each property can have multiple vals, of different types??)

So instead we use ordinary maps, and we define functions to get them
into and out of the datastore.  The datastore itself is conceptualized
as a gigantic map of maps.

So currently:

(ds/defentity Author [^{:tag :key} name, birthday])
;; Writes three authors to the datastore.
(let [will (Author. "Shakespeare, William" nil)
      geoff (Author. "Chaucer, Geoffrey" "1343")
      oscar (Author. "Wilde, Oscar" "1854-10-16")]
  ;; First, just write Will, without a birthday.
  (ds/save! will)

Instead we do:
(let [will ^{:kind :Author :id "Shakespeare"}{:name "Shakespeare, William"}
      geoff ^{:kind :Author :id "Chaucer"}
      	    {:name "Chaucer, Geoffrey", :birthday "1343"}
      oscar ^{:kind :Author :id "Wilde"}
      	    {:name "Wilde, Oscar", :birthday "1854-10-16"}]
  ;; First, just write Will, without a birthday.
  (ds/into! ds/ds will)

The drawback is that you have to explicitly construct each map; you
can't use the conveniences of defrecord.  On the other hand, if you
really need to create lots of entities from say a table of data it is
trivial to write a function to do so using e.g. map.  It would
actually be easier (and more efficient) than creating a new record
(=java object) for each.

It is only when we save ("put") a map to the datastore that it
"becomes" an entity.

What should this look like?  Say we def myrec as a map and want to put
it to the ds.  Conceptually, this is just adding it to a map, so
idiomatically we want to use "into":

    (ds/into ds myrec)

NB the distinction between adding to a clojure map with "into" (etc.)
and writing to the datastore.  Our "into" function should mean both;
in other words, adding to the ds means doing "put".

On the other side, fetching an entity from the ds is conceptually
equivalent to doing "find" or the like.

In short, we just reimplement ordinary collection functions to make
them datastore-aware.

Take the first example from the docs:

DatastoreService datastore = DatastoreServiceFactory.getDatastoreService();
Entity employee = new Entity("Employee");
employee.setProperty("firstName", "Antonio");
employee.setProperty("lastName", "Salieri");
Date hireDate = new Date();
employee.setProperty("hireDate", hireDate);
employee.setProperty("attendedHrTraining", true);
datastore.put(employee);

In our api:

(def e {:kind :Employee, :firstName "Antonio", :lastName "Salieri", ... etc...})
(ds/into! ds e)

NB: the "ds" arg to ds/into is redundant, since there is only one
possibility.  ds/save! works just as well; the question is how closely
we want to stick to map idioms.

Also ds/into is misleading since it does not return the augmented map
like plain "into" does.  So we make it "into!".  And we make it return
the entity key.

Other examples from the docs:

Entity employee = new Entity("Employee");
datastore.put(employee);
Entity address = new Entity("Address", employee.getKey());
datastore.put(address);

becomes (assuming ds/into! returns key of put entity):

(let [e {:kind :Employee}
      a {:kind :Address, :parent (ds/into! ds e)}]
      (ds/into! ds a))

But now what about fetching?  We need sth that behaves like a map
(i.e. key lookups) but also supports the Entity api.  So it can't be a
plain map.  For example, it should work with seq, map, keys, etc.

One possibility: on fetch, construct plain old map and stuff the java
Entity in the metadata?  E.g. assuming "this" is an Entity:

       ^{:entity this}...

Then key, kind, etc. all accessible as metadata rather than data?

(NB: generally you don't need rec type as part of the record, so this
fits.  type info is metadata.)

But then we need to intercept e.g. (:kind e) if :kind is metadata.

Do we need sth like with-entity?  We have seven "get"s: getProperty,
getKey, etc.

## API

types:

	keymap:  {:kind <kind keyword> :id <string or number>}

	entitymap:  ^keymap {:k1 v1, k2 v2, ...}

For entities we have:

    Key - kind, id, parent components
    keymap - clojure :kind/:id/:parent map
    Entity
    entitymap (set of clojure key/val property pairs)

The key stuff is all essentially metadata.

So for any entity we want to be able to access e.g.

    e.kind, e.id, e.parent, e.key, e.entitymap

Since these are all just "fields" in a sense, we can emulate clojure
by implementing entities as functions, so:

    (myEnt :kind), (myEnt :id), etc.

However, this would prevent users from using those fields in their
entities.  So let's use metadata:

    (:kind (meta myEnt)) etc.

To make this work dynamically we need to be able to hook into metadata
lookup (compare Lua) in order e.g. to make a ds call for :kind.

We can fake it by defining ds/meta:  ((ds/meta myEnt) :kind)

ds/meta returns a function that takes a key and does the lookup.
Three cheers for first class functions.

What about fields?  Wrap Entity in function so we can do stuff like (myent :foo).

What about the fields collection?  How to iterate?  The Clojurish way:

    (doseq [[k v] myent] ...do sth with k, v...)

So whenever we make (ds/into!) or fetch (ds/ds) an entity, the result
is a function wrapping the java Entity object.

But this would allow us to get metadata by kw: (myent :key) etc.  Bad
since it removes these keywords from user space.  

Best: use std clojure meta func.  But can we do this if we add
metadata to our POJO wrapper function?

Ideally, we want ((meta myEnt) :kind) = (:kind (meta myEnt)) to return
the kind.  But since that involves calls to the ds api, it must return
a function which we need to call:

    ((:kind (meta myEnt)))
    (((meta myEnt) :kind))

Ugly, but it should work.

The alternative is to memoize these properties.  Actually that's ok
for keys, since they never change!


