(ns migae.datastore.impl.keyword
  "support ops taking :force or :multi as first arg"
  ;; FIXME: :multi not needed? can be detected via arg types?
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
           migae.datastore.PersistentEntityMap
           )
  (:require [clojure.tools.logging :as log :only [debug info]]
            [clojure.tools.reader.edn :as edn]
            ;; [migae.datastore.types.entity-map :refer :all]
            [migae.datastore.impl.vector :as v]
            [migae.datastore :as k]
            [migae.datastore.adapter.gae :as gae]))

(clojure.core/println "loading migae.datastore.impl.keyword")

(defn entity-map? [m]
  (log/trace "entity-map?" (meta m) m (type m))
  (not (nil? (:migae/keychain (meta m)))))

(defn entity-map
  "entity-map: local constructor"
  ([kw]
   ;; FIXME: broken
   (log/trace "entity-map 1" (meta kw) kw (type kw))
   (with-meta {} {:migae/keychain kw}))
  ([kw m]
   (log/trace "entity-map 2" kw m)
   (let [em (with-meta m {:migae/keychain kw})]
     (log/trace "em: " (meta em) em)
     em))
  ([kw m mode]
   {:pre [(= mode :em)]}
   (log/trace "entity-map :em" kw m)
   (if (empty? kw)
     (throw (IllegalArgumentException. "keychain vector must not be empty"))
     (let [ds (DatastoreServiceFactory/getDatastoreService)
           key (k/vector->Key kw)
           foo (log/trace "kw: " key)
           e (Entity. key)]
       (doseq [[kw v] m]
         (.setProperty e (subs (str kw) 1) (k/get-val-ds v)))
       (PersistentEntityMap. e nil))))
  )

(defn entity-map!
  "forced push ctor: if first arg is :force, overwrite"
  ([keyword] (throw (IllegalArgumentException. "cannot make entity-map from keyword")))
  ([mode keychain]
   (log/trace "entity-map! 2" mode keychain)
   (cond
     (= mode :force) ;; force write empty map
     (cond
       (k/improper-keychain? keychain)
       (do
         (log/trace "entity-map! improper:" keychain)
         (gae/put-kinded-emap (with-meta {} {:migae/keychain keychain}))
         )
       (k/proper-keychain? keychain)
       (do
         (log/trace "entity-map! proper:" keychain)
         ;; (gae/put-proper-emap :keyvec keychain :propmap em)
         (gae/put-proper-emap :keyvec keychain :propmap {} :force true)
         )
       :else (throw (ex-info "InvalidKeychainException"
                             {:type :keychain-exception, :cause :invalid}))
       )
     :else (throw (IllegalArgumentException. (str "Invalid mode keyword: " mode)))))

  ([mode keychain em]
   (log/trace "entity-map! 3" mode keychain em)
   (cond
     (= mode :force)
     (do
       (log/trace "entity-map! :force processing...")
       (cond
         (k/improper-keychain? keychain)
         (do
           (log/trace "entity-map! improper:" keychain)
           (gae/put-kinded-emap (with-meta em {:migae/keychain keychain}))
           )
         (k/proper-keychain? keychain)
         (do
           (log/trace "entity-map! proper:" keychain)
           (let [e (gae/put-proper-emap :keyvec keychain :propmap em :force true)]
             ;; (PersistentEntityMap. e nil)
             e)
           )
         :else (throw (ex-info (str "InvalidKeychainException: " keychain)
                               {:type :keychain-exception, :cause :invalid}))))

     (= mode :multi)
     (do
       (log/trace "entity-map! :multi processing..." keychain em)
       (if (k/improper-keychain? keychain)
         (do
             (if (vector? em)
               (doall (map (fn [emap]
                             (println "EM: " emap)
                             (v/entity-map! keychain emap))
                           em))
               (throw (IllegalArgumentException. ":multi ctor requires vector of maps"))))
         (throw (IllegalArgumentException. ":multi ctor requires improper keychain"))))

     :else
     (throw (IllegalArgumentException. (str "Invalid mode keyword:" force))))
   ;; (into-ds! force keychain em))
   ))

(defn entity-map*
  ;; (defmethod entity-map* [clojure.lang.PersistentVector nil]
  ([keychain]
   (log/trace "entity-map* k" keychain)
   (let [r (gae/get-ds keychain)]
     (log/trace "entity-map* result: " r)
     r)))

(defn keychain
  [m]
  (log/trace "keychain: " m)
  (let [k (:migae/keychain (meta m))]
    (if (k/keychain? k) k nil)))
;    (if k (:migae/keychain (meta m)) nil)))

(defn kind
  "entity-map.kind co-ctor"
  [v]
  (log/trace "entity-map.kind co-ctor" v)
  ;; FIXME validate
  ;; FIXME throw an exception on empty arg?
  (let [dogtag (last v)]
    (if-let [ns (namespace dogtag)]
      (keyword ns)
      dogtag)))

(defn identifier
  "entity-map.identifier co-ctor"
  [v]
  (log/trace "entity-map.identifier co-ctor" v)
  ;; FIXME validate
  (let [dogtag (last v)]
    (if-let [ns (namespace dogtag)]
      (keyword (name dogtag))
      dogtag)))

;; (defn entity-key
;;   [mode keychain m]
;;   (log/trace "Keyword entity-key: " mode keychain m)
;;   )
