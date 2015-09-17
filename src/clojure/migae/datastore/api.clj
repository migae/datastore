(clojure.core/println "loading migae.datastore.api")
(ns migae.datastore.api
  (:import [com.google.appengine.api.datastore
            DatastoreFailureException
            DatastoreService
            DatastoreServiceFactory
            DatastoreServiceConfig
            DatastoreServiceConfig$Builder])
  ;; WARNING! the migae nss must be in the right order so the deftypes get intantiated
  (:require [migae.datastore :as ds]  ;; this instantiates the deftypes needed by implementations
            [migae.datastore.impl.map :as pmap]
            [migae.datastore.impl.vector :as pvec]
            [clojure.tools.logging :as log :only [debug info]]
            ))

(def store-map
  (let [dsm (DatastoreServiceFactory/getDatastoreService)
        ;; foo (log/debug "dsm: " dsm (type dsm))
        psm (ds/->PersistentStoreMap dsm nil nil)]
    ;; (log/debug "psm: " psm (type psm))
    psm))

;; (log/debug "store-map: " store-map (type store-map))
;; (log/debug "store-map IReduce? " (instance? clojure.lang.IReduce store-map))
;; (log/debug "store-map IReduceInit? " (instance? clojure.lang.IReduceInit store-map))
;; (log/debug "store-map IEditableCollection? " (instance? clojure.lang.IEditableCollection store-map))

(defprotocol Entity-Map
  "protocol for entity maps"
  (entity-map? [em])
  (entity-map [k] [k m] [k m mode])
  (entity-map! [k] [k m] [k m mode])
  (entity-map* [k] [k m] [k m mode])
  (entity-map=? [em1 em2])
  (map=? [em1 em2])
  (key=? [arg1 arg2])
  (kind [em])
  (identifier [em])
  (keychain [arg])
  (keychain? [arg])
  ;; native stuff
  (entity? [e])
  (entity-key [k])
  (entity-key? [k])
  (keychain-to-key [k])
  ;; utils
  (print [arg])
  (print-str [arg])
  )

(extend clojure.lang.IPersistentMap
  Entity-Map
  {:entity-map? pmap/entity-map?
   :entity-map pmap/entity-map
   :kind pmap/kind
   :identifier pmap/identifier
   })

(extend clojure.lang.IPersistentVector
  Entity-Map
  {:entity-map? pvec/entity-map?
   :entity-map pvec/entity-map
   :kind pvec/kind
   :identifier pvec/identifier
   })

(extend migae.datastore.IPersistentEntityMap
  Entity-Map
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
   ;; "PersistentEntityMap local constructor"
   ;; ([k]
   ;;  {:pre [(keychain? k)]}
   ;;  "Construct empty entity-map"
   ;;  ;; (log/debug "ctor-local.entity-map k" k (type k))
   ;;  (if (empty? k)
   ;;    (throw (IllegalArgumentException. "keychain vector must not be empty"))
   ;;    (let [k (keychain-to-key k)
   ;;          e (Entity. k)]
   ;;      (PersistentEntityMap. e nil)))))
   ;;  :entity-map
   ;; (entity-map entity-map)
   ;; ([keychain em]
   ;;  ;; {:pre [(map? em)
   ;;  ;;        (vector? keychain)
   ;;  ;;        (not (empty? keychain))
   ;;  ;;        (every? keylink? keychain)]}
   ;;  ;; (if (empty? keychain)
   ;;  ;;   (throw (IllegalArgumentException. "keychain vector must not be empty"))
   ;;  ;;   (let [k (keychain-to-key keychain)
   ;;  ;;         ;; foo (log/trace "k: " k)
   ;;  ;;         e (Entity. k)]
   ;;  ;;     (doseq [[k v] em]
   ;;  ;;       (.setProperty e (subs (str k) 1) (get-val-ds v)))
   ;;  ;;     (PersistentEntityMap. e nil)))))
   })

