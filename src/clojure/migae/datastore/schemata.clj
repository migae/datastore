(ns migae.datastore.schemata
  ;; (:import [com.google.appengine.api.datastore
  ;;           DatastoreFailureException
  ;;           DatastoreService
  ;;           DatastoreServiceFactory
  ;;           DatastoreServiceConfig
  ;;           DatastoreServiceConfig$Builder
  ;;           Entity EmbeddedEntity EntityNotFoundException
  ;;           Email
  ;;           Key KeyFactory KeyFactory$Builder
  ;;           Link]
  ;;          [java.util
  ;;           Collection
  ;;           Collections
  ;;           ArrayList
  ;;           HashSet
  ;;           Vector]
  ;;          migae.datastore.InvalidKeychainException
  ;;          )
  (:require [clojure.tools.logging :as log :only [debug info]]
            [clojure.tools.reader.edn :as edn]
            [schema.core :as s])) ;; :include-macros true]

(clojure.core/println "loading migae.datastore.schemata")

;;  keys:  :Schema/#version-number, e.g. :Persons/#1
(defonce schemata (atom {}))

(defn schema
  [kw]
  (@schemata kw))

(defn register-schema
  [kw schema]
  (log/debug "register-schema: " kw schema)
  ;; FIXME: validation
  (swap! schemata assoc kw schema))

(defn dump [] (log/debug "schemata: " @schemata))
