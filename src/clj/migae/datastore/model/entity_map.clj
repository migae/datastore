(ns migae.datastore.model.entity-map
  (:import [com.google.appengine.api.datastore
            DatastoreFailureException
            DatastoreService
            DatastoreServiceFactory
            DatastoreServiceConfig
            DatastoreServiceConfig$Builder
            Key]
           )
  ;; WARNING! the migae nss must be in the right order so the deftypes get instantiated
  (:require [migae.datastore.signature.entity-map :as sig]
            [migae.datastore.keys :as k]
            [migae.datastore.structure.map :as pmap]
            [migae.datastore.structure.vector :as pvec]
            [migae.datastore.structure.list :as plist]
            [migae.datastore.structure.keyword :as kw]
            [clojure.tools.logging :as log :only [debug info]]
            )
)

(clojure.core/println "loading migae.datastore.model.entity-map")

(extend clojure.lang.IPersistentMap
  sig/Entity-Map
  {:entity-map? pmap/entity-map?
   :entity-map pmap/entity-map
   :entity-map! pmap/entity-map!
   :dump pmap/dump
   :dump-str pmap/dump-str
   }
  sig/Entity-Key
  {:keychain pmap/keychain
   :keychain? pmap/keychain?
   :keychain=? pmap/keychain=?
   :key=? pmap/key=?
   :kind pmap/kind
   :identifier pmap/identifier
   })

(extend clojure.lang.PersistentArrayMap
  sig/Entity-Map
  {:entity-map? pmap/entity-map?
   :entity-map pmap/entity-map
   :entity-map! pmap/entity-map!
   :dump pmap/dump
   :dump-str pmap/dump-str
   }
  sig/Entity-Key
  {:keychain pmap/keychain
   :keychain? pmap/keychain?
   :keychain=? pmap/keychain=?
   :key=? pmap/key=?
   :kind pmap/kind
   :identifier pmap/identifier
   :entity-key (fn [this] (k/entity-key (:migae/keychain (meta this))))
   })

(extend clojure.lang.IPersistentVector
  sig/Entity-Map
  {:entity-map? pvec/entity-map?
   :entity-map pvec/entity-map
   :entity-map! pvec/entity-map!  ;;  [k] [k m] [k m mode]) ;; push ctor
   :entity-map* pvec/entity-map*
   ;; :entity-map* pvec/entity-map*  ;; [k] [k m] [k m mode]) ;; pull ctor
   ;; :entity-map$ pvec/entity-map$  ;;[k] [k m] [k m mode]) ;; local entity ctor
   }
  sig/Entity-Key
  {:keychain pvec/keychain
   :keychain? k/keychain?
   :kind pvec/kind
   ;; :keychain=? pmap/keychain=?
   ;; :key=? pmap/key=?
   :identifier pvec/identifier
   :entity-key k/entity-key
   })

(extend clojure.lang.IPersistentList
  sig/Entity-Map
  {:entity-map? plist/entity-map?
   :entity-map plist/entity-map}
  sig/Entity-Key
  {:keychain plist/keychain
   :keychain? k/keychain?
   :kind plist/kind
   :identifier plist/identifier
   :entity-key plist/entity-key
   })

(extend clojure.lang.Keyword
  sig/Entity-Map
  {:entity-map? kw/entity-map?
   :entity-map kw/entity-map
   :entity-map! kw/entity-map!  ;;  [k] [k m] [k m mode]) ;; push ctor
   :entity-map* kw/entity-map
   ;; :entity-map* pvec/entity-map*  ;; [k] [k m] [k m mode]) ;; pull ctor
   ;; :entity-map$ pvec/entity-map$  ;;[k] [k m] [k m mode]) ;; local entity ctor
   }
  sig/Entity-Key
  {:keychain kw/keychain
   :keychain? k/keychain?
   :kind kw/kind
   :identifier kw/identifier
   :entity-key kw/entity-key
   })

(extend migae.datastore.IPersistentEntityMap
  sig/Entity-Map
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

(extend com.google.appengine.api.datastore.Key
  sig/Entity-Key
  {:keychain k/keychain
   :keychain? (fn [this] true)
   :kind (fn [this] (keyword (.getKind this)))
   ;; :keychain=? pmap/keychain=?
   :key=? (fn [this k] (.equals this k))
   :identifier (fn [this] (if-let [nm (.getName this)] (symbol nm) (.getId this)))
   :entity-key (fn [this] this)
   :entity-key? (fn [this] true)
   })
