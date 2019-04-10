(clojure.core/println "Start loading migae.datastore.model.entity-map")

(in-ns 'migae.datastore)
;(import '(clojure.lang IFn IObj IPersistentMap ISeq)
(import (com.google.appengine.api.datastore DatastoreFailureException
                                            DatastoreService
                                            DatastoreServiceFactory
                                            DatastoreServiceConfig
                                            DatastoreServiceConfig$Builder
                                            Key)
        )

;(require '(clojure.tools [logging :as log :only [debug info]])

  ;; WARNING! the migae nss must be in the right order so the deftypes get instantiated
(require '[migae.datastore.Interfaces]
         '[migae.datastore.signature.entity-map :as sig]
         '[migae.datastore.keys :as k]
         '[migae.datastore.structure.map :as pmap]
         '[migae.datastore.structure.vector :as pvec]
         '[migae.datastore.structure.list :as plist]
         '[migae.datastore.structure.keyword :as kw]
         '[potemkin :refer [import-vars]]
         '[clojure.tools.logging :as log :refer [debug info]]
         )
;;)

(clojure.core/println "loading migae.datastore.model.entity-map")

;; we extend Clojure and other types to support the datastore signatures
;; this obviates the need to type-check before applying a ds operator
;; e.g. if m is a plain old clojure map, (entity-map? m) will return false instead of throwing an exception

;; expose sig operators
(import-vars [migae.datastore.signature.entity-map
              entity-map? entity-map entity-map! entity-map* entity-map$ entity-maps=?
              entity? entity-key keychain keychains=? kind identifier
              dump dump-str
              ->keychain keychain? keys=? ->kind ->identifier
              schema register-schema dump-schemata])

(extend clojure.lang.IPersistentMap
  sig/Entity-Map
  {:entity-map? pmap/entity-map?
   :entity-map pmap/entity-map
   :entity-map! pmap/entity-map!
   :keychain pmap/keychain
   :keychains=? pmap/keychain=?
   :kind pmap/kind
   :identifier pmap/identifier
   :dump pmap/dump
   :dump-str pmap/dump-str
   }
  sig/Entity-Key
  {:keychain? pmap/keychain?
   :keys=? pmap/key=?
   })

(extend clojure.lang.PersistentArrayMap
  sig/Entity-Map
  {:entity-map? pmap/entity-map?
   :entity-map pmap/entity-map
   :entity-map! pmap/entity-map!
   :entity-maps=? pmap/entity-maps=?
   :dump pmap/dump
   :dump-str pmap/dump-str
   :entity-key (fn [this] (k/entity-key (:migae/keychain (meta this))))
   :keychain pmap/keychain
   :keychain? pmap/keychain?
   :keychains=? pmap/keychain=?
   :kind pmap/kind
   :identifier pmap/identifier
   }
  sig/Entity-Key
  {:keys=? (fn [this em]
             (if (instance? migae.datastore.IPersistentEntityMap em)
               (do (log/debug "keys=? " (type em))
               (= (:migae/keychain (meta this)) (sig/keychain (.getKey (.content em)))))
               (if (sig/entity-map? em)
                 (= (sig/keychain this) (sig/keychain em)))))
   })

#_(extend clojure.lang.IPersistentList
  sig/Entity-Map
  {:entity-map? plist/entity-map?
   :entity-map plist/entity-map
   :entity-key plist/entity-key}
  sig/Entity-Key
  {:keychain plist/keychain
   :keychain? k/keychain?
   :kind plist/kind
   :identifier plist/identifier
   })

(extend clojure.lang.IPersistentVector
  sig/Entity-Map
  {:entity-map? pvec/entity-map?
   :entity-map pvec/entity-map
   :entity-map! pvec/entity-map!
   :entity-map* pvec/entity-map*
   ;; :entity-map$ pvec/entity-map$  ;;[k] [k m] [k m mode]) ;; local entity ctor
   :dump pvec/dump
   :dump-str pvec/dump-str
   :entity-key k/entity-key
   :keychain pvec/keychain
   :kind pvec/kind
   ;; :keychain=? pmap/keychain=?
   ;; :key=? pmap/key=?
   :identifier pvec/identifier
   }
  sig/Entity-Key
  {:keychain? k/keychain?}
  )

(extend clojure.lang.Keyword
  sig/Entity-Map
  {:entity-map? kw/entity-map?
   :entity-map kw/entity-map
   :entity-map! kw/entity-map!  ;;  [k] [k m] [k m mode]) ;; push ctor
   :entity-map* kw/entity-map*
   ;; :entity-map* pvec/entity-map*  ;; [k] [k m] [k m mode]) ;; pull ctor
   ;; :entity-map$ pvec/entity-map$  ;;[k] [k m] [k m mode]) ;; local entity ctor
   :keychain kw/keychain
   :kind kw/kind
   :identifier kw/identifier
   :entity-key kw/entity-key
   }
  sig/Entity-Key
  {:keychain? k/keychain?
   })

(extend migae.datastore.PersistentEntityMap
  sig/Entity-Map
  {:entity-map? (fn [this] true)
                  ;; ;; true
                  ;; ;; )
                  ;; ;;(defn entity-map?
                  ;; ;; [this]
                  ;; (log/debug "PersistentEntityMap.entity-map?" this (type this)
                  ;;            (instance? migae.datastore.IPersistentEntityMap this))
                  ;; (instance? migae.datastore.IPersistentEntityMap this))
   ;; FIXME: create a new copy?
   :entity-map (fn [this] this)
   :entity-key (fn [this] (.getKey (.content this)))
   :keychain (fn [this] (k/keychain (.content this)))
   :keychains=? (fn [this em]
                  (if (instance? migae.datastore.IPersistentEntityMap em)
                    (.equals (.getKey (.content this)) (.getKey (.content em)))
                    (if (sig/entity-map? em)
                      (= (sig/keychain this) (sig/keychain em)))))
   :kind (fn [this] (.getKind (.getKey (.content this))))
   :identifier (fn [this]
                 (let [k (.getKey (.content this))]
                   (if-let [id (.getId k)]
                     id
                     (.getName k))))
   :dump (fn [this] (binding [*print-meta* true] (with-out-str (prn this))))
   :dump-str (fn [this] (binding [*print-meta* true] (with-out-str (pr-str this))))
   }
  ;; sig/Entity-Key
  ;; {;; :keychain plist/keychain
  ;;  ;; :keychain? k/keychain?
  ;;  }
   )

(extend com.google.appengine.api.datastore.Key
  sig/Entity-Map
  {
   :keychain k/keychain
   :kind (fn [this] (.getKind this))
   :identifier (fn [this] (if-let [nm (.getName this)] (symbol nm) (.getId this)))
   ;; :entity-key (fn [this] this)
   }
  sig/Entity-Key
  {:keychain? (fn [this] true)
   :entity-key? (fn [this] true)
   ;; :keychain=? pmap/keychain=?
   :keys=? (fn [this k] (.equals this k))
   })

