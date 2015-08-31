# migae Keychains

A Datastore entity is a pair of a
[Key](https://cloud.google.com/appengine/docs/java/javadoc/com/google/appengine/api/datastore/Key)
and a
[PropertyContainer](https://cloud.google.com/appengine/docs/java/javadoc/com/google/appengine/api/datastore/PropertyContainer).
The migae api represents these as _keychains_ and _maps_,
respectively.

A Datastore Key is a peculiar creature.  An object of class
`com.google.appengine.api.datastore.Key` "looks like" a Key object,
but in fact it is a link in a linked list of Key objects, and it is
the entire list that serves to identify Entity object.  Hence migae
uses the terms _keychain_ to refer to this list, and _keylink_ to
refer to particular links in such a list.

### anatomy of a Datastore Key

A Key is composed of:

* a _kind_ of type `String`
* an _identifier_, which can be either
 * a _name_ of type string, or
 * an _id_ of type `long`
* optionally, a _parent_ key
* (hidden: a namespace string)

If you print an Entity to stdout you'll see something like the following:

```
Subfamily("Felinae")/Genus("Felis")/Species("Felis_catus")
```

This is the print representation of a Key whose final node has kind
"Species" and name "Felis\_catus", with a parent Key whose kind
is "Genus" and whose name is "Felis", etc.  The _root link_ has kind
"Subfamily" and name "Felinae".

Constructing Keys with the Java API is a fairly complex matter;
Clojure makes it trivial.  The above key can be expressed as the
following keychain in migae:

```
[:Subfamily/Felinae :Genus/Felis :Species/Felis_catus]
```

You can see that a keychain is just a vector of keywords.  The only
constraint is that each keyword must be namespaced; so for example
`[:A/B :C :D/E]` would not be accepted as a keychain since it contains `:C`,
an element (aka "keylink") with no namespace.

# keys and the datastore

When you save an Entity with a multi-link keychain to the datastore,
the keychain is of course stored as part of the Entity.  But the links
in the chain are not stored as Entity keys.  In other words, you don't
have to store an Entity for each link in a keychain, and conversely,
just because you have a keylink that formed part of a stored Entity
Key doesn't mean you can retrieve an Entity for that keylink.

# keychains in queries

Datastore supports _kind queries_, which retrieve all Entities having
a specified kind.  Such a query can also specify an "ancestor"
keychain.  In migae this is easily expressed using a _partial
keychain_, which is a vector of keywords, all of which except the last
are namespaced.  For example:

```
[:Subfamily/Felinae :Genus]
```

Used as a query specification, this would match all Entities having
kind `:Genus` and having parent Key `Subfamily("Felinae")` - the
family [Felinae](https://en.wikipedia.org/wiki/Felinae) includes, in
addition to genus _Felis_, _Leopardus_, _Lynx_, _Puma_, and several
others, so this might match `[:Subfamily/Felinae :Genus/Felis]`,
`[:Subfamily/Felinae :Genus/Leopardus]`, etc.
