(ns migae.datastore.api
;;  (:refer-clojure :exclude [print println print-str println-str])
  (:import [com.google.appengine.api.datastore
            DatastoreFailureException
            DatastoreService
            DatastoreServiceFactory
            DatastoreServiceConfig
            DatastoreServiceConfig$Builder
            Key])
  ;; WARNING! the migae nss must be in the right order so the deftypes get instantiated
  (:require [clojure.tools.logging :as log :only [debug info]]
            ))

(clojure.core/println "loading migae.datastore.api")

;; (def store-map
;;   (let [dsm (DatastoreServiceFactory/getDatastoreService)
;;         psm (->PersistentStoreMap dsm nil nil)]
;;     psm))

(defprotocol Entity-Map
  "protocol for entity maps"
  (entity-map? [em])
  (entity-map  [k] [k m] [k m mode]) ;; local collection ctor
  (entity-map! [k] [k m] [k m mode]) ;; push ctor
  (entity-map* [k] [k m] [k m mode]) ;; pull ctor
  (entity-map$ [k] [k m] [k m mode]) ;; local entity ctor
  (entity-map=? [em1 em2])
  (map=? [em1 em2])
  (key=? [arg1 arg2])
  ;; native stuff
  (entity? [e])
  ;; utils
  (dump [arg])
  (dump-str [arg])
  )

(defprotocol Entity-Key
  "protocol for keychains and entity keys"
  (keychain? [arg])
  (keychain [arg])
  (kind [em])
  (identifier [em])
  (entity-key [k])
  (entity-key? [k])
  )

