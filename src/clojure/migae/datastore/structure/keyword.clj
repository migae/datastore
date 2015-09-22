(ns migae.datastore.structure.keyword
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
           migae.datastore.InvalidKeychainException
           migae.datastore.PersistentEntityMap
           )
  (:require [clojure.tools.logging :as log :only [debug info]]
            [clojure.tools.reader.edn :as edn]
            ;; [migae.datastore.types.entity-map :refer :all]
            [migae.datastore.structure.vector :as v]
            [migae.datastore.keys :as k]
            [migae.datastore.adapter.gae :as gae]))

(clojure.core/println "loading migae.datastore.structure.keyword")

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
           key (k/entity-key k)
           foo (log/trace "k: " key)
           e (Entity. key)]
       (doseq [[k v] m]
         (.setProperty e (subs (str k) 1) (k/get-val-ds v)))
       (PersistentEntityMap. e nil))))
  )

(defn entity-map!
  "push ctor"
  ([mode keychain]
   (log/debug "entity-map! 2" mode keychain)
   (cond
     (= mode :force) ;; force write empty map
     (cond
       (k/improper-keychain? keychain)
       (do
         (log/debug "entity-map! improper:" keychain)
         (gae/put-kinded-emap (with-meta {} {:migae/keychain keychain}))
         )
       (k/proper-keychain? keychain)
       (do
         (log/debug "entity-map! proper:" keychain)
         ;; (gae/put-proper-emap :keyvec keychain :propmap em)
         (gae/put-proper-emap :keyvec keychain :propmap {} :force true)
         )
       :else (throw (InvalidKeychainException. (str keychain))))
     :else (throw (IllegalArgumentException. (str "Invalid mode keyword: " mode)))))

  ([mode keychain em]
   (log/debug "entity-map! 3" mode keychain em)
   (cond
     (= mode :force)
     (do
       (log/debug "entity-map! :force processing...")
       (cond
         (k/improper-keychain? keychain)
         (do
           (log/debug "entity-map! improper:" keychain)
           (gae/put-kinded-emap (with-meta em {:migae/keychain keychain}))
           )
         (k/proper-keychain? keychain)
         (do
           (log/debug "entity-map! proper:" keychain)
           (let [e (gae/put-proper-emap :keyvec keychain :propmap em :force true)]
             (PersistentEntityMap. e nil))
           )
         :else
         (throw (InvalidKeychainException. (str keychain)))))
     (= mode :multi)
     (do
       (log/debug "entity-map! :multi processing...")
       (if (k/improper-keychain? keychain)
         (if (vector? em)
           (do
             (for [emap em]
               (do
                 ;; (log/debug "ctoring em" (print-str emap))
                 (v/entity-map! keychain emap))))
           (throw (IllegalArgumentException. ":multi ctor requires vector of maps")))
         (throw (IllegalArgumentException. ":multi ctor requires improper keychain"))))
     :else
     (throw (IllegalArgumentException. (str "Invalid mode keyword:" force))))
   ;; (into-ds! force keychain em))
   ))

(defn keychain
  [m]
  (log/debug "keychain: " m)
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

(defn entity-key
  [mode keychain m]
  (log/debug "Keyword entity-key: " mode keychain m)
  )
