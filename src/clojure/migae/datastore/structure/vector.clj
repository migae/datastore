(ns migae.datastore.structure.vector
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
            [migae.datastore.keys :as k]
            ;;[migae.datastore.types.entity-map :refer :all]
            [migae.datastore.structure.map :as m]
            [migae.datastore.adapter.gae :as gae]))

(clojure.core/println "loading migae.datastore.structure.vector")

(defn entity-map?
  ([k m]
   (log/debug "entity-map?" k m)
   ;; FIXME: validate m?
   (k/proper-keychain? k)))

(defn entity-map
  "entity-map: local constructor"
  ([k]
   (log/debug "entity-map 1" (meta k) k (type k))
   (if (k/keychain? k)
     (do
       ;; (log/debug "keychain:" k)
       (with-meta {} {:migae/keychain k}))
     (do
       ;; (log/debug "    not keychain:" k)
       (apply entity-map k))
     ))
  ([k m]
   (log/debug "entity-map 2" k m)
   (cond
     (k/proper-keychain? k)
     (if (m/valid-emap? m)
       (if (every? keyword? (keys m))
         (let [em (with-meta m {:migae/keychain k})]
           ;; (log/debug "em: " (meta em) em)
           em)
         (throw (IllegalArgumentException. (str "Invalid map, only keyword keys allowed: " m))))
       (throw (IllegalArgumentException. (str "Invalid map arg " m))))

     (k/improper-keychain? k)
     ;; FIXME: allow improper keychains in PersistentMap?
     ;; call them "open emaps", as opposed to closed, just like closed/open logic sentences
     (throw (IllegalArgumentException. (str "Improper keychain '" k "' not allowed for local ctor")))

     :else
     (if (empty? k)
       (throw (IllegalArgumentException. (str "Null keychain '" k "' not allowed for local ctor")))
       (throw (InvalidKeychainException. (str k))))))

  ([k m mode]
   {:pre [(= mode :em)]}
   ;; (log/debug "entity-map :em" k m)
   (if (empty? k)
     (throw (IllegalArgumentException. "keychain vector must not be empty"))
     (let [ds (DatastoreServiceFactory/getDatastoreService)
           key (k/entity-key k)
           ;; log (log/debug "k: " key)
           e (Entity. key)]
       (doseq [[k v] m]
         (.setProperty e (subs (str k) 1) (k/get-val-ds v)))
       (PersistentEntityMap. e nil))))
  )

(defn entity-map!
  "push ctor"
  ([keychain]
   ;;(into-ds! keychain)
   (cond
     (k/improper-keychain? keychain)
     (do
       (log/debug "entity-map! 1 improper:" keychain)
       ;; (gae/put-kinded-emap keychain {})
       )
     (k/proper-keychain? keychain)
     (do
       (log/debug "entity-map! 1 proper:" keychain)
       ;; (put-proper-emap :keyvec keychain :propmap {} :force true)
       )
     :else (throw (InvalidKeychainException. (str keychain)))))

  ([keychain em]
   (cond
     (k/improper-keychain? keychain)
     (do
       (log/debug "entity-map! 2 improper:" keychain)
       (let [e (gae/put-kinded-emap (with-meta em {:migae/keychain keychain}))]
         (PersistentEntityMap. e nil))
       )
     (k/proper-keychain? keychain)
     (do
       (log/debug "entity-map! 2 proper:" keychain)
       (let [e (gae/put-proper-emap :keyvec keychain :propmap em)]
         (log/debug "put entity: " e)
         (PersistentEntityMap. e nil))
       )
     :else (if (empty? keychain)
             (throw (IllegalArgumentException. (str "Null keychain '" keychain "' not allowed")))
             (throw (InvalidKeychainException. (str keychain)))))
   )
  ;; ([force keychain em]
  ;;  {:pre [(or (map? em) (vector? em))
  ;;         (vector? keychain)
  ;;         (not (empty? keychain))
  ;;         (every? keyword? keychain)]}
  ;;  (into-ds! force keychain em))
  )

(defn keychain
  [m]
  ;; (log/debug "keychain: " m)
  (let [k (:migae/keychain (meta m))]
    (if (k/keychain? k) k nil)))
;    (if k (:migae/keychain (meta m)) nil)))

(defn kind
  "entity-map.kind co-ctor"
  [v]
  ;; (log/debug "entity-map.kind co-ctor" v)
  ;; FIXME validate
  ;; FIXME throw an exception on empty arg?
  (let [dogtag (last v)]
    (if-let [ns (namespace dogtag)]
      (keyword ns)
      dogtag)))

(defn identifier
  "entity-map.identifier co-ctor"
  [v]
  ;; (log/debug "entity-map.identifier co-ctor" v)
  ;; FIXME validate
  (let [dogtag (last v)]
    (if-let [ns (namespace dogtag)]
      (keyword (name dogtag))
      dogtag)))
