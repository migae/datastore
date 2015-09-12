(ns migae.datastore.PersistentEntityMapSeq
  (:refer-clojure :exclude [get into name print println print-str pr-str reduce])
  (:import [java.lang IllegalArgumentException RuntimeException]
           [java.util
            Collection
            Collections
            ;; Collections$UnmodifiableMap
            ;; Collections$UnmodifiableMap$UnmodifiableEntrySet
            ;; Collections$UnmodifiableMap$UnmodifiableEntrySet$UnmodifiableEntry
            ArrayList
            HashMap HashSet
            Map Map$Entry
            Vector]
           ;; [clojure.lang MapEntry]
           [com.google.appengine.api.blobstore BlobKey]
           [com.google.appengine.api.datastore
            Blob
            DatastoreFailureException
            DatastoreService
            DatastoreServiceFactory
            DatastoreServiceConfig
            DatastoreServiceConfig$Builder
            Email
            Entity EmbeddedEntity EntityNotFoundException
            FetchOptions$Builder
            ImplicitTransactionManagementPolicy
            Key KeyFactory KeyFactory$Builder
            Link
            PhoneNumber
            ReadPolicy ReadPolicy$Consistency
            Query Query$SortDirection
            Query$FilterOperator Query$FilterPredicate
            Query$CompositeFilter Query$CompositeFilterOperator
            ShortBlob
            Text
            Transaction]
           [clojure.lang IFn ILookup IMapEntry IObj
            IPersistentCollection IPersistentMap IReduce IReference ISeq ITransientCollection]
           )
  (:require [clojure.core :as clj]
            [clojure.walk :as walk]
            [clojure.stacktrace :refer [print-stack-trace]]
            [clojure.tools.reader.edn :as edn]
            [migae.datastore.service :as ds]
            [migae.datastore.keychain :as ekey]
;;            [migae.datastore.ctor-push :as push]
;;            [migae.datastore.adapter :refer :all]
            ;; [migae.datastore.entity :as dse]
            ;; [migae.datastore.key :as dskey]
            ;; [migae.datastore.query :as dsqry]
            [migae.infix :as infix]
            [clojure.tools.logging :as log :only [trace debug info]]))

(clojure.core/println "reloading PersistentEntityMapSeq")

(declare ->PersistentEntityMap)

(deftype PersistentEntityMapSeq [query]

  ;;   java.lang.Iterable
  ;;   (^java.util.Iterator iterator [this]
  ;;     (log/trace "Iterable iterator")
  ;;     this)
  ;;     ;; (let [props (.getProperties content) ;; java.util.Map<java.lang.String,java.lang.Object>
  ;;     ;;       entry-set (.entrySet props)
  ;;     ;;       e-iter (.iterator entry-set)
  ;;     ;;       em-iter (PersistentEntityMapSeq. e-iter) ]
  ;;     ;; ;; (log/trace "Iterable res:" em-iter)
  ;;     ;; em-iter))

  ;;   java.util.Iterator
  ;;   ;; hasNext, next, remove
  ;;   ;; Java 8: forEachRemaining
  ;;   (^boolean hasNext [this]
  ;;     (do
  ;; ;;      (log/trace "PersistentEntityMapSeq.hasNext")
  ;;       (> (count query) 0)))
  ;;   (next [this] ;-> migae.datastore.IPersistentEntityMap
  ;;     (do
  ;;       (log/trace "PersistentEntityMapSeq.next")
  ;;       (log/trace "content type: " (type query))
  ;;       ))
  ;;       ;; (let [r (get-next-emap-prop this)
  ;;       ;;       k (.getKey r)
  ;;       ;;       v (.getValue r)
  ;;       ;;       res {(keyword k) v}]
  ;;       ;;   (log/trace "emap-iter next" res)
  ;;       ;;   res)))
  ;;   (remove    [this] ;-> void
  ;;     (do
  ;;       (log/trace "PersistentEntityMapSeq.remove")))

  clojure.lang.ISeq ;; < IPersistentCollection (< Seqable)
  (^Object
    first [_]
    ;; (log/trace "ISeq first of" (type query))
    (let [r  (first query)
          rm (->PersistentEntityMap r nil)]
      ;; rm (migae.datastore.PersistentEntityMap. r nil)]
      ;; (log/trace "rm:" rm)
      rm))
  (^ISeq
    next [_]
    (let [res (next query)]
      (log/trace "ISeq next" (type res))
      (if (nil? res)
        nil
        (PersistentEntityMapSeq. res))))
  (^ISeq
    more [_]  ;;  same as next?
    ;; (log/trace "PersistentEntityMapSeq.ISeq more")
    (let [res (next query)]
      (log/trace "ISeq next" (type res))
      (if (nil? res)
        nil
        (PersistentEntityMapSeq. res))))
  (^ISeq ;;^clojure.lang.IPersistentVector
    ;; FIXME!  this compiles with return type ISeq, but when running
    ;; sparky/gae, which uses the lib, we get a "wrong type" msg, it
    ;; wants IPersistentVector.  but if we use that, then compile
    ;; complains that it wants ISeq!!!  So we remove the ISeq interface.
    ;; Maybe we don't want that anyway for an Interator.
    cons  ;; -> ^ISeq ;;
    [this ^Object obj]
    (log/trace "ISeq cons"))

  ;;;; Seqable interface
  (^ISeq
    seq [this]  ; specified in Seqable
    (log/trace "PersistentEntityMapSeq.ISeq.seq")
    this)

  ;;;; IPersistentCollection interface
  (^int
    count [_]
    ;; (log/trace "PersistentEntityMapSeq.count")
    (count query))
  ;; cons - overridden by ISeq
  (^IPersistentCollection
    empty [_]
    (log/trace "PersistentEntityMapSeq.empty"))
  (^boolean
    equiv [_ ^Object obj]
    (log/trace "PersistentEntityMapSeq.equiv"))

  ;; clojure.lang.IndexedSeq extends ISeq, Sequential, Counted{
  ;;public int index();

  clojure.lang.Indexed                  ; extends Counted
  ;; (count [this]                         ; Counted
  ;;   (log/trace "PersistentEntityMapSeq.clojure.lang.Indexed.count")
  ;;   (count query))
  (nth [this i]                         ; called by get(int index)
    (log/trace "Indexed nth" i))
  ;; (next em-iter)) ;; HACK
  (nth [this i not-found]
    (log/trace "Indexed nth with not-found" i))
  )

