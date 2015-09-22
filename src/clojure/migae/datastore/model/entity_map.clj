(ns migae.datastore.model.entity-map
  ;;  (:refer-clojure :exclude [print println print-str println-str])
  (:import [com.google.appengine.api.datastore
            DatastoreFailureException
            DatastoreService
            DatastoreServiceFactory
            DatastoreServiceConfig
            DatastoreServiceConfig$Builder
            Key]
           ;; migae.datastore.PersistentStoreMap
           )
  ;; WARNING! the migae nss must be in the right order so the deftypes get instantiated
  (:require [migae.datastore.keys :as k]
            [migae.datastore.signature.entity-map :as api]
            migae.datastore.types.entity-map
            migae.datastore.types.entity-map-seq
            migae.datastore.types.store-map
            [migae.datastore.structure.map :as pmap]
            [migae.datastore.structure.vector :as pvec]
            [migae.datastore.structure.list :as plist]
            [migae.datastore.structure.keyword :as kw]
            [clojure.tools.logging :as log :only [debug info]]
            )
)

(clojure.core/println "loading migae.datastore.model.entity-map")

(extend clojure.lang.IPersistentMap
  api/Entity-Map
  {:entity-map? pmap/entity-map?
   :entity-map pmap/entity-map
   :dump pmap/dump
   :dump-str pmap/dump-str
   }
  api/Entity-Key
  {:keychain pmap/keychain
   :keychain? pmap/keychain?
   :keychain=? pmap/keychain=?
   :key=? pmap/key=?
   :kind pmap/kind
   :identifier pmap/identifier
   })

(extend clojure.lang.IPersistentVector
  api/Entity-Map
  {:entity-map? pvec/entity-map?
   :entity-map pvec/entity-map
   :entity-map! pvec/entity-map!  ;;  [k] [k m] [k m mode]) ;; push ctor
   ;; :entity-map* pvec/entity-map*  ;; [k] [k m] [k m mode]) ;; pull ctor
   ;; :entity-map$ pvec/entity-map$  ;;[k] [k m] [k m mode]) ;; local entity ctor
   }
  api/Entity-Key
  {:keychain pvec/keychain
   :keychain? k/keychain?
   :kind pvec/kind
   ;; :keychain=? pmap/keychain=?
   ;; :key=? pmap/key=?
   :identifier pvec/identifier
   :entity-key k/entity-key
   })

(extend clojure.lang.IPersistentList
  api/Entity-Map
  {:entity-map? plist/entity-map?
   :entity-map plist/entity-map}
  api/Entity-Key
  {:keychain plist/keychain
   :keychain? k/keychain?
   :kind plist/kind
   :identifier plist/identifier
   :entity-key plist/entity-key
   })

(extend clojure.lang.Keyword
  api/Entity-Map
  {:entity-map? kw/entity-map?
   :entity-map kw/entity-map
   :entity-map! kw/entity-map!  ;;  [k] [k m] [k m mode]) ;; push ctor
   ;; :entity-map* pvec/entity-map*  ;; [k] [k m] [k m mode]) ;; pull ctor
   ;; :entity-map$ pvec/entity-map$  ;;[k] [k m] [k m mode]) ;; local entity ctor
   }
  api/Entity-Key
  {:keychain kw/keychain
   :keychain? k/keychain?
   :kind kw/kind
   :identifier kw/identifier
   :entity-key kw/entity-key
   })

(extend migae.datastore.IPersistentEntityMap
  api/Entity-Map
  {:entity-map? (fn [em]
                  ;; true
                  ;; )
                  ;;(defn entity-map?
                  ;; [em]
                  (log/debug "PersistentEntityMap.entity-map?" em (type em)
                             (instance? migae.datastore.IPersistentEntityMap em))
                  (instance? migae.datastore.IPersistentEntityMap em))
   :entity-map (fn [em] em)
   :kind (fn [em] (log/debug "IPersistentMap.Entity-Map.kind" em) nil)
   :identifer (fn [em] (log/debug "IPersistentMap.Entity-Map.identifier" em) nil)
   :key=? k/key=?
   :dump-str (fn [m] (binding [*print-meta* true] (pr-str m)))
   })

