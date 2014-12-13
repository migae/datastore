(ns migae.datastore.service
  (:refer-clojure :exclude [name hash])
  (:import [com.google.appengine.api.datastore
            DatastoreService
            DatastoreServiceFactory
            DatastoreServiceConfig
            DatastoreServiceConfig$Builder]))
;  (:require [clojure.tools.logging :as log :only [trace debug info]]))


(defonce ^{:dynamic true} *datastore-service* (atom nil))

(defn get-datastore-service []
  (when (nil? @*datastore-service*)
        (reset! *datastore-service* (DatastoreServiceFactory/getDatastoreService)))
  @*datastore-service*)

(defn datastore []
  (when (nil? @*datastore-service*)
    (do (prn "datastore ****************")
        (reset! *datastore-service* (DatastoreServiceFactory/getDatastoreService))))
  @*datastore-service*)

