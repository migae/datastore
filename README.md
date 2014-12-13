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

