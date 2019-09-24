(clojure.core/println "Start loading migae.datastore")

(ns migae.datastore
  (:refer-clojure :exclude [print println print-str println-str])
  (:import [java.lang IllegalArgumentException RuntimeException]
           [java.util
            Collection
            Collections
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
  (:require [clojure.walk :as walk]
            [clojure.stacktrace :refer [print-stack-trace]]
            [clojure.tools.reader.edn :as edn]
            [migae.datastore.Interfaces]
            ;; [migae.datastore.types.entity-map]
            ;; [migae.datastore.types.entity-map-seq]
            ;; [migae.datastore.structure.vector]

            ;; [migae.datastore.service :as ds]
            ;; NB:  trace level not available on gae
            [clojure.tools.logging :as log :only [debug info]]
;;            [migae.datastore.types.entity-map-seq :as ems]
            ;; [migae.datastore.types.entity-map :refer :all]
            ;; [migae.datastore.types.entity-map-seq :refer :all]
;;            [migae.datastore.adapter.gae :refer :all]
            ;; [migae.datastore.impl.map :as pmap]
            ;; [migae.datastore.impl.vector :as pvec]
          )) ;; warn, error, fatal

(clojure.core/println "loading migae.datastore")

;; (declare ->PersistentStoreMap)
;; (declare ->PersistentEntityMapSeq)
;; (declare ->PersistentEntityMap)

;;(declare ds-to-clj clj-to-ds)
(declare make-embedded-entity)
(declare props-to-map get-next-emap-prop)

(declare keychain? keylink? keykind? keychain keychain-to-key
         proper-keychain?  improper-keychain?
         vector->Key)

(declare store-map store-map?)
(declare entity-map? kind)

;;(load "datastore/api")
;;(load "datastore/PersistentEntityMap")
;;(load "datastore/PersistentStoreMap")

;;(load "datastore/types/entity_map_seq") ;; PersistentEntityMapSeq")

(load "/migae/datastore/protocols")
(load "/migae/datastore/entity_map_seq")
(load "/migae/datastore/entity_map")
(load "/migae/datastore/keys")
(load "/migae/datastore/impl")

(clojure.core/println "Done loading migae.datastore")
