(ns migae.datastore.impl.list
  (:import [com.google.appengine.api.datastore
            DatastoreFailureException
            DatastoreService
            DatastoreServiceFactory
            DatastoreServiceConfig
            DatastoreServiceConfig$Builder
            Entity EmbeddedEntity EntityNotFoundException
            Email
            Link]
           [java.util
            Collection
            Collections
            ArrayList
            HashSet
            Vector]
           ;;migae.datastore.PersistentEntityMap
           )
  (:require [clojure.tools.logging :as log :only [debug info]]
            [clojure.tools.reader.edn :as edn]
            [migae.datastore :as k]))

(clojure.core/println "loading migae.datastore.impl.list")


(declare dump dump-str)

(declare ->PersistentEntityMap)

(defn entity-map? [m]
  (log/debug "entity-map?" (meta m) m (type m))
  (not (nil? (:migae/keychain (meta m)))))

(defn entity-map
  "entity-map: local constructor"
  ([k]
   (log/debug "entity-map 1" (meta k) k (type k))
   (with-meta {} {:migae/keychain k}))
  ([k m]
   (log/debug "entity-map 2" k m)
   (let [em (with-meta m {:migae/keychain k})]
     (log/debug "em: " (meta em) em)
     em))
  ([k m mode]
   {:pre [(= mode :em)]}
   (log/debug "entity-map :em" k m)
   (if (empty? k)
     (throw (IllegalArgumentException. "keychain vector must not be empty"))
     (let [ds (DatastoreServiceFactory/getDatastoreService)
           key (k/vector->Key (vec k))
           foo (log/trace "k: " key)
           e (Entity. key)]
       (doseq [[k v] m]
         (.setProperty e (subs (str k) 1) (k/get-val-ds v)))
       (->PersistentEntityMap e nil))))
  )

(defn keychain
  [m]
  (log/debug "IPersistentList.keychain: " m)
  (let [k (:migae/keychain (meta m))]
    (if (k/keychain? k) k nil)))
;    (if k (:migae/keychain (meta m)) nil)))

(defn kind
  "entity-map.kind co-ctor"
  [v]
  (log/debug "entity-map.kind co-ctor" v)
  ;; FIXME validate
  ;; FIXME throw an exception on empty arg?
  (let [dogtag (last v)]
    (if-let [ns (namespace dogtag)]
      (keyword ns)
      dogtag)))

(defn identifier
  "entity-map.identifier co-ctor"
  [v]
  (log/debug "entity-map.identifier co-ctor" v)
  ;; FIXME validate
  (let [dogtag (last v)]
    (if-let [ns (namespace dogtag)]
      (keyword (name dogtag))
      dogtag)))

;; (defn entity-key
;;   ([keychain]
;;    (k/vector->Key (vec keychain))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;  utils
(defn dump
  [m]
  (binding [*print-meta* true]
    (with-out-str
      (prn m))))

(defn dump-str
  [m]
  (binding [*print-meta* true]
    (with-out-str
      (pr-str m))))
