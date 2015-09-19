(ns migae.datastore.spi
  "migae service programming interface - to be implemented by db
  adapters.  api functions use spi functions to interact with the
  underlying db service."
  (:import [com.google.appengine.api.datastore
            DatastoreFailureException
            DatastoreService
            DatastoreServiceFactory
            DatastoreServiceConfig
            DatastoreServiceConfig$Builder
            Key]))

(clojure.core/println "loading migae.datastore.spi")

;; (def store-map
;;   (let [dsm (DatastoreServiceFactory/getDatastoreService)
;;         psm (ds/->PersistentStoreMap dsm nil nil)]
;;     psm))

(defprotocol DataStore-Service
  "protocol for datastore service providers"
  )

;; (extend Foo
;;   Datastore-Service
;;   {
;;    })
