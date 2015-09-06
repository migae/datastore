(ns migae.datastore
  (:refer-clojure :exclude [get into name print reduce])
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
           ;; migae.datastore.ImproperKeylinkException
           ;; [migae.datastore PersistentEntityMap PersistentEntityMapCollIterator])
           [clojure.lang IFn ILookup IMapEntry IObj
            IPersistentCollection IPersistentMap IReduce IReference ISeq ITransientCollection]
           )
  (:require [clojure.core :as clj]
            [clojure.walk :as walk]
            [clojure.stacktrace :refer [print-stack-trace]]
            [clojure.tools.reader.edn :as edn]
            ;; [migae.datastore :refer :all]
            [migae.datastore.service :as ds]
            [migae.datastore.keychain :as ekey]
            ;; [migae.datastore.dsmap :as dsm]
            ;; [migae.datastore.emap :as emap]
            ;; [migae.datastore.entity :as dse]
            ;; [migae.datastore.key :as dskey]
            ;; [migae.datastore.query :as dsqry]
            [migae.infix :as infix]
            [clojure.tools.logging :as log :only [trace debug info]]))

(declare get-val-clj get-val-ds)
(declare props-to-map get-next-emap-prop)

;; (declare make-embedded-entity)

(defn- get-next-emap-prop [this]
  ;; (log/trace "get-next-emap-prop" (.query this))
  (let [r (.next (.query this))]
    ;; (log/trace "next: " r)
    r))

(load "datastore/keychain")

(load "datastore/PersistentEntityMapIterator")
(load "datastore/PersistentEntityMap")
(load "datastore/PersistentEntityHashMap")
(load "datastore/utils")
(load "datastore/ctor_common")
(load "datastore/hashmap/adapter")
(load "datastore/predicates")
(load "datastore/ctor_push")
(load "datastore/query")
(load "datastore/ctor_pull")
(load "datastore/ekey")
(load "datastore/dsmap")
(load "datastore/api")

