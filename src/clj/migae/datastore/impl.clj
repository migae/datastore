(clojure.core/println "Start loading migae.datastore.impl")

(in-ns 'migae.datastore)
;(import '(clojure.lang IFn IObj IPersistentMap ISeq)
(import (com.google.appengine.api.datastore DatastoreFailureException
                                            DatastoreService
                                            DatastoreServiceFactory
                                            DatastoreServiceConfig
                                            DatastoreServiceConfig$Builder
                                            Key)
        )

  ;; WARNING! the migae nss must be in the right order so the deftypes get instantiated
(require ;;'[migae.datastore.Interfaces]
         ;; '[migae.datastore.signature.entity-map :as sig]
         ;; '[migae.datastore.keys :as k]
         '[migae.datastore.impl.map :as pmap]
         '[migae.datastore.impl.vector :as pvec]
         '[migae.datastore.impl.list :as plist]
         '[migae.datastore.impl.keyword :as kw]
         ;; '[potemkin :refer [import-vars]]
         '[clojure.tools.logging :as log :refer [debug info]]
         )
;;)

(clojure.core/println "loading migae.datastore.impl")

;; we extend Clojure and other types to support the datastore signatures
;; this obviates the need to type-check before applying a ds operator
;; e.g. if m is a plain old clojure map, (entity-map? m) will return false instead of throwing an exception

(extend clojure.lang.IPersistentMap
  Entity-Map
  {:entity-map? pmap/entity-map?
   :entity-map pmap/entity-map
   :entity-map! pmap/entity-map!
   :entity-key (fn [this] (vector->Key (:migae/keychain (meta this))))
   :keychain keychain
   :keychains=? pmap/keychain=?
   :kind pmap/kind
   :identifier pmap/identifier
   :dump pmap/dump
   :dump-str pmap/dump-str
   }
  Entity-Key
  {:keychain? keychain?
   :keys=? pmap/key=?
   })

(extend clojure.lang.PersistentArrayMap
  Entity-Map
  {:entity-map? pmap/entity-map?
   :entity-map pmap/entity-map
   :entity-map! pmap/entity-map!
   :entity-maps=? pmap/entity-maps=?
   :dump pmap/dump
   :dump-str pmap/dump-str
   :entity-key (fn [this] (vector->Key (:migae/keychain (meta this))))
   :keychain pmap/keychain
   :keychains=? pmap/keychain=?
   :kind pmap/kind
   :identifier pmap/identifier
   }
  Entity-Key
  {:keychain? keychain?
   :->Key (fn [this] (->Key (:migae/keychain (meta this))))
   :keys=? (fn [this em]
             (if (instance? migae.datastore.IPersistentEntityMap em)
               (do (log/debug "keys=? " (type em))
               (= (:migae/keychain (meta this)) (keychain (.getKey (.content em)))))
               (if (entity-map? em)
                 (= (keychain this) (keychain em)))))
   })

#_(extend clojure.lang.IPersistentList
  Entity-Map
  {:entity-map? plist/entity-map?
   :entity-map plist/entity-map
   :entity-key plist/entity-key}
  sig/Entity-Key
  {:keychain plist/keychain
   :keychain? keychain?
   :kind plist/kind
   :identifier plist/identifier
   })

(extend clojure.lang.IPersistentVector
  Entity-Map
  {:entity-map? pvec/entity-map?
   :entity-map pvec/entity-map
   :entity-map! pvec/entity-map!
   :entity-map* pvec/entity-map*
   ;; :entity-map$ pvec/entity-map$  ;;[k] [k m] [k m mode]) ;; local entity ctor
   :dump pvec/dump
   :dump-str pvec/dump-str
   ;; :vector->Key vector->Key
   :keychain pvec/keychain
   :kind pvec/kind
   ;; :keychain=? pmap/keychain=?
   ;; :key=? pmap/key=?
   :identifier pvec/identifier
   }
  Entity-Key
  {:keychain? pvec/keychain?
   :->keychain pvec/->keychain
   :->Key vector->Key
   :Key? (fn [this] false)
  ;; (keychain? [k])
  ;; ;; (keychains=? [k1 k2])
  ;; (keys=? [k1 k2])
  ;; (->kind [k])
  ;; (->identifier [k])
   })

;; FIXME: why extend Keyword?
(extend clojure.lang.Keyword
  Entity-Map
  {:entity-map? kw/entity-map?
   :entity-map kw/entity-map
   :entity-map! kw/entity-map!  ;;  [k] [k m] [k m mode]) ;; push ctor
   :entity-map* kw/entity-map*
   ;; :entity-map* pvec/entity-map*  ;; [k] [k m] [k m mode]) ;; pull ctor
   ;; :entity-map$ pvec/entity-map$  ;;[k] [k m] [k m mode]) ;; local entity ctor
   :keychain kw/keychain
   :kind kw/kind
   :identifier kw/identifier
   ;; :entity-key kw/entity-key
   }
  Entity-Key
  {:keychain? keychain?
   })

(extend migae.datastore.PersistentEntityMap
  Entity-Map
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
   :keychain (fn [this] (keychain (.getKey (.content this))))
   :keychains=? (fn [this em]
                  ;; (log/debug "keychains=? on migae.datastore.IPersistentEntityMap")
                  ;; (log/debug "arg 2 type: " (type em))
                  (if (instance? migae.datastore.IPersistentEntityMap em)
                    (.equals (.getKey (.content this)) (.getKey (.content em)))
                    ;; else if em is datastore.Entity
                    (if (entity-map? em)
                      (= (keychain this) (keychain em)))))
   :kind (fn [this] (.getKind (.getKey (.content this))))
   :identifier (fn [this]
                 (let [k (.getKey (.content this))]
                   (if-let [id (.getId k)]
                     id
                     (.getName k))))
   :dump (fn [this] (binding [*print-meta* true] (with-out-str (prn this))))
   :dump-str (fn [this] (binding [*print-meta* true] (with-out-str (pr-str this))))
   }
  Entity-Key
  {
  ;; (->keychain [k])
  ;; (keychain? [k])
   :->Key (fn [this] (.getKey (.content this)))
  ;; (entity-key? [k])
  ;; (keychains=? [k1 k2])
  ;; (keys=? [k1 k2])
  ;; (->kind [k])
  ;; (->identifier [k])

  ;; {;; :keychain plist/keychain
  ;;  ;; :keychain? keychain?
   })

(extend com.google.appengine.api.datastore.Key
  Entity-Map
  {
   :keychain (fn [this]
               {:pre [(not (nil? this))]}
               ;; (log/trace "Key keychain co-ctor 2: Key" this)
               (let [kind (.getKind this)
                     nm (.getName this)
                     id (str (.getId this))
                     dogtag (keyword kind (if nm nm id))
                     res (if (.getParent this)
                           (conj (list dogtag) (keychain (.getParent this)))
                           (list dogtag))]
                 ;; (log/trace "kind" kind "nm " nm " id " id " parent " (.getParent this))
                 ;; (log/trace "res: " res)
                 ;; (log/trace "res2: " (vec (flatten res)))
                 (vec (flatten res))))

   :kind (fn [this] (.getKind this))
   :identifier (fn [this] (if-let [nm (.getName this)] (symbol nm) (.getId this)))
   ;; :entity-key (fn [this] this)
   }
  Entity-Key
  {:keychain? (fn [this] true)
   :Key? (fn [this] true)
   ;; :keychain=? pmap/keychain=?
   :keys=? (fn [this k] (.equals this k))
   })

