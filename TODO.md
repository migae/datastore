# Keys

* Support syntax like :Foo/Bar/Baz/Buz so that such chains can be used
  as keywords for associative lookup, e.g.  (:Foo/Bar/Baz/Buz @ds/DSMap)

* Filtering on DatastoreMap.  The problem is that standard `filter`
  first applies `seq` to the collection arg.  We can't do that; we
  have to apply the filter first in the form of a ds query.  So we
  need a non-seq filter function.  (Call it `retlif`, filter
  backwards?)  Or a workaround involving iterators: maybe we can trick
  Clojure by passing a seq with a custom iterator that will fetch the
  data...

# misc

Fetch logic:  catch not found exceptions in the lib code!


Should fetch be in ds or dsqry?  e.g.

    (ds/fetch ...)
    (dsqry/fetch ...)

In other words, does the client really need to now about query
objects?  All the client really needs to do is provide the parameters
for a query and then say "gimme this".  All the machinery of
constructing a query object, then prepping it, then casting the result
to an interator, etc. can be hidden.
