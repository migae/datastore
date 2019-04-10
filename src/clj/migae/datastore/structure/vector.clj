(clojure.core/println "Start loading migae.datastore.structure.vector")

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
           migae.datastore.PersistentEntityMap
           )
  (:require [clojure.tools.logging :as log :only [debug info]]
            [clojure.tools.reader.edn :as edn]
            [migae.datastore.keys :as k]
            ;;[migae.datastore.types.entity-map :refer :all]
            [migae.datastore.structure.utils :as u]
            [migae.datastore.adapter.gae :as gae]))

(clojure.core/println "loading migae.datastore.structure.vector")

(declare dump dump-str)

(defn entity-map?
  ([k m]
   (log/trace "entity-map?" k m)
   ;; FIXME: validate m?
   (k/proper-keychain? k)))

(defn entity-map
  "entity-map: local constructor"
  ([k]
   (log/trace "entity-map 1" (meta k) k (type k))
   (if (k/keychain? k)
     (do
       ;; (log/trace "keychain:" k)
       (with-meta {} {:migae/keychain k}))
     (do
       ;; (log/trace "    not keychain:" k)
       (apply entity-map k))
     ))
  ([k m]
   ;; (println "entity-map 2" k m)
   (cond
     (k/proper-keychain? k)
     (if (u/valid-emap? m)
       (if (every? keyword? (keys m))
         (let [em (with-meta m {:migae/keychain k})]
           ;; (log/trace "em: " (meta em) em)
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
       (throw (ex-info "InvalidKeychainException" {:type :keychain-exception, :cause :invalid})))))

  ([k m mode]
   {:pre [(= mode :em)]}
   ;; (log/trace "entity-map :em" k m)
   (if (empty? k)
     (throw (IllegalArgumentException. "keychain vector must not be empty"))
     (let [ds (DatastoreServiceFactory/getDatastoreService)
           key (k/entity-key k)
           ;; log (log/trace "k: " key)
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
       (log/trace "entity-map! 1 improper:" keychain)
       ;; (gae/put-kinded-emap keychain {})
       )
     (k/proper-keychain? keychain)
     (do
       (log/trace "entity-map! 1 proper:" keychain)
       ;; (put-proper-emap :keyvec keychain :propmap {} :force true)
       )
     :else (throw (ex-info "InvalidKeychainException"
                           {:type :keychain-exception, :cause :invalid}))))

  ([keychain em]
   (log/trace "entity-map! 2")
   (cond
     (k/improper-keychain? keychain)
     (do
       ;; (log/trace "entity-map! 2 improper:" keychain)
       (let [e (gae/put-kinded-emap (with-meta em {:migae/keychain keychain}))]
         e)
       ;; (PersistentEntityMap. e nil))
       )
     (k/proper-keychain? keychain)
     (do
       (log/trace "entity-map! 2 proper:" keychain)
       (let [e (gae/put-proper-emap :keyvec keychain :propmap em)]
         (log/trace "put entity: " e)
         ;; (PersistentEntityMap. e nil)
         e)
       )
     :else (if (empty? keychain)
             (throw (IllegalArgumentException. (str "Null keychain '" keychain "' not allowed")))
             (throw (ex-info "InvalidKeychainException"
                             {:type :keychain-exception, :cause :invalid})))))
  ;; ([force keychain em]
  ;;  {:pre [(or (map? em) (vector? em))
  ;;         (vector? keychain)
  ;;         (not (empty? keychain))
  ;;         (every? keyword? keychain)]}
  ;;  (into-ds! force keychain em))
  )

(defn entity-map*
  ;; (defmethod entity-map* [clojure.lang.PersistentVector nil]
  ([keychain]
   (log/trace "entity-map* 1" keychain)
   (let [r (gae/get-ds keychain)]
     (log/trace "entity-map* result: " r)
     (log/trace "entity-map* result type: " (type r))
     r))
  ;; (defmethod entity-map* [clojure.lang.Keyword clojure.lang.PersistentVector]
  ([arg1 arg2]
   (log/trace "entity-map* 2" arg1 arg2)
   (cond
     (keyword? arg1) ;; mode keyword:  :prefix, :iso, etc.
     (do (log/trace "mode " arg1 " keychain: " arg2)
         (gae/get-ds arg1 arg2))

     (k/improper-keychain? arg1)
     (gae/get-ds arg1 arg2)

     (k/proper-keychain? arg1)
     (gae/get-ds arg1 arg2)

     :else (throw (IllegalArgumentException. "bad arg1")))
     )
  ;; modal keychain + propmap filters
  ([arg1 arg2 arg3]
   (log/trace "entity-map* 3" arg1 arg2 arg3)
   (if (keyword? arg1)
     (do
       ;; mode keyword:  :prefix, :iso, etc.
       (log/trace "mode " arg1 " keychain: " arg2))
     (throw (RuntimeException. "bad args")))))

;; (defn entity-key
;;   ([v]
;;    {:pre [(proper-keychain? keychain)]}
;;    ;; (log/trace "keychain-to-key: " keychain (type keychain) " : " (vector? keychain))
;;    ;; (if (proper-keychain? keychain)
;;    (let [k (keyword-to-key (first keychain))
;;          root (KeyFactory$Builder. k)]
;;      (.getKey (doto root (add-child-keylink (rest keychain))))
;;      (throw (InvalidKeychainException. (str keychain))))))

(defn keychain
  [v]
  ;; (println "keychain: " v)
  (if (k/keychain? v) v nil))

(defn kind
  "entity-map.kind co-ctor"
  [v]
  ;; (log/trace "entity-map.kind co-ctor" v)
  ;; FIXME validate
  ;; FIXME throw an exception on empty arg?
  (let [dogtag (last v)]
    (if-let [ns (namespace dogtag)]
      (keyword ns)
      dogtag)))

(defn identifier
  "entity-map.identifier co-ctor"
  [v]
  ;; (log/trace "entity-map.identifier co-ctor" v)
  ;; FIXME validate
  (let [dogtag (last v)]
    (if-let [ns (namespace dogtag)]
      (name dogtag)
      dogtag)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;  utils
(defn dump
  [m]
    (binding [*print-meta* true]
      (println m)))

(defn dump-str
  [m]
  (binding [*print-meta* true]
    (pr-str m)))

(clojure.core/println "Done loading migae.datastore.structure.vector")
