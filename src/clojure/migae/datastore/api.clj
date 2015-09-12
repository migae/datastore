(in-ns 'migae.datastore)
;(in-ns 'migae.datastore)

;; (ns migae.datastore.api
;;   (:refer-clojure :exclude [empty? filter get into key name reduce])
;;   (:import [java.lang IllegalArgumentException RuntimeException]
;;            [java.util
;;             Collection
;;             Collections
;;             ;; Collections$UnmodifiableMap
;;             ;; Collections$UnmodifiableMap$UnmodifiableEntrySet
;;             ;; Collections$UnmodifiableMap$UnmodifiableEntrySet$UnmodifiableEntry
;;             ArrayList
;;             HashMap HashSet
;;             Map Map$Entry
;;             Vector]
;;            ;; [clojure.lang MapEntry]
;;            [com.google.appengine.api.datastore
;;             Blob
;;             ;; DatastoreFailureException
;;             DatastoreService
;;             ;; DatastoreServiceFactory
;;             DatastoreServiceConfig
;;             DatastoreServiceConfig$Builder
;;             Email
;;             Entity EmbeddedEntity EntityNotFoundException
;;             FetchOptions$Builder
;;             ImplicitTransactionManagementPolicy
;;             Key KeyFactory KeyFactory$Builder
;;             Link
;;             PhoneNumber
;;             ReadPolicy ReadPolicy$Consistency
;;             Query Query$SortDirection
;;             Query$FilterOperator Query$FilterPredicate
;;             Query$CompositeFilter Query$CompositeFilterOperator
;;             ShortBlob
;;             Text
;;             Transaction]
;;            [com.google.appengine.api.blobstore BlobKey]
;;            ;; [migae.datastore PersistentEntityMap PersistentEntityMapCollIterator])
;;            )
;;   (:require [clojure.core :as clj]
;;             [clojure.walk :as walk]
;;             [clojure.stacktrace :refer [print-stack-trace]]
;;             [clojure.tools.reader.edn :as edn]
;;             [migae.datastore :refer :all]
;;             [migae.datastore.keychain :refer :all]
;;             ;; [migae.datastore.dsmap :as dsm]
;;             ;; [migae.datastore.emap :as emap]
;;             ;; [migae.datastore.entity :as dse]
;;             ;; [migae.datastore.key :as dskey]
;;             ;; [migae.datastore.query :as dsqry]
;;             [migae.infix :as infix]
;;             [clojure.tools.logging :as log :only [trace debug info]]))


;;(declare epr)
;; =======
(declare #_emap?? emaps?? emap-seq epr #_emap? keychain=)

(defn dsfilter                            ;defmacro?
  [keypred & valpred]
  (apply emaps?? keypred valpred))

(defn emap-seq? [arg]
  (and
   (seq? arg))
  (= (type arg) migae.datastore.PersistentEntityMapCollIterator))

(defn emap-seq [ds-iter]
  "Returns a seq on a com.google.appengine.api.datastore.QueryResultIterator"
  (let [em-iter (migae.datastore.PersistentEntityMapCollIterator. ds-iter)
        clj-iter (clojure.lang.IteratorSeq/create em-iter)]
    (if (nil? clj-iter)
      nil
      (with-meta clj-iter {:type migae.datastore.PersistentEntityMapCollIterator}))))

;; ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; ;;;;  emaps stuff

;; <<<<<<< HEAD
(defn emaps!!
  "e.g. (emaps!! [:Foo] [{:a 1} {:a 2} {:a 3}]) saves three Entities of kind :Foo

  If keychain ends in a kind (e.g. [:Foo] create anonymous Entities of
  that kind.  If it ends in a full key?  Merge the maps?"
  [keychain maps]
  (log/trace "")
  (log/trace "emaps!!" keychain maps)
  (if (keylink? (last keychain))
    (throw (IllegalArgumentException. "emaps!! keychain must end in a Kind keyword (e.g. :Foo); try emap!!"))
    ;; (let [s
    (doseq [emap maps]
      (do
        ;; (log/trace "emap" emap)
        (emap!! keychain emap)))
    ;; ]
      ;; (doseq [item s]
      ;;   (log/trace "item" (meta item) item)
      ;;   (log/trace "item entity" (.content item)))
      ))

(defn- throw-bad-keylinks
  [keylinks]
  (throw (IllegalArgumentException.
          (str "Every element of keylink vector "
               (pr-str keylinks)
               " must be a keyword with namespace and name (e.g. :Foo/Bar)"))))

(defn- throw-bad-keykind
  [keylinks]
  (throw (IllegalArgumentException.
          (str "Last element of keylink vector "
               (pr-str keylinks)
               " must be a definite (e.g. :Foo/Bar) or indefinite (e.g. :Foo) keylink"))))

(defn- emap-definite!!
  ;; keylinks already validated
  [keylinks & props]
  ;; (log/trace "emap-definite!!" keylinks props)
  (let [k (apply keychain-to-key keylinks)
        e  (Entity. k)
        propmap (first props)]
    (if (not (nil? propmap))
      (let [props (first propmap)]
        (doseq [[k v] props]
          (.setProperty e (subs (str k) 1) (get-val-ds v)))))
    (.put (ds/datastore) e)
    (migae.datastore.PersistentEntityMap. e nil)))

(defn- emap-indefinite!!
  [keylinks & props]
  ;; (log/trace "emap-indefinite!!" keylinks props)
  (let [e (if (> (count keylinks) 1)
            (let [parent (apply keychain-to-key (butlast keylinks))]
              (Entity. (name (last keylinks)) parent))
            (Entity. (name (first keylinks))))
        propmap (first props)]
    (if (not (nil? propmap))
      (let [propmap (first props)]
        ;; (log/trace "props" propmap)
        (doseq [[k v] propmap]
          (.setProperty e (subs (str k) 1) (get-val-ds v)))))
    (.put (ds/datastore) e)
    (migae.datastore.PersistentEntityMap. e nil)))


(defn- find-definite-necessarily
  [keylinks & propmap]
  (log/trace "find-definite-necessarily" keylinks propmap)
  (if (every? keylink? keylinks)
    (let [akey (apply keychain-to-key keylinks)
          e (try (.get (ds/datastore) akey)
                 (catch EntityNotFoundException ex nil))]
      (if (nil? e) ;; not found
        (let [newe  (Entity. akey)]
          (if (not (nil? propmap))
            (let [props (first propmap)]
              (doseq [[k v] props]
                (let [p (subs (str k) 1)
                      val (get-val-ds v)]
                  ;; (log/trace "PROP" p val)
                  (.setProperty newe p val)))))
          (.put (ds/datastore) newe)
          (migae.datastore.PersistentEntityMap. newe nil))
        ;; entity found:
        (let [em (migae.datastore.PersistentEntityMap. e nil)]
          (log/trace "already found" (epr em))
          ;; FIXME: match properties agains propmap?
          em)))
    (throw-bad-keylinks keylinks)))

(defn find-indefinite-necessarily
  [keylinks & propmap]
  {:pre [(every? keylink? (butlast keylinks))
         (nil? (namespace (last keylinks)))]}
  (log/trace "find-indefinite-necessarily" keylinks propmap)

  ;; if propmap not nil, construct property-filter query

  (let [e (if (nil? (butlast keylinks))
            (Entity. (name (last keylinks)))
            (let [parent (apply keychain-to-key (butlast keylinks))]
              ;; (log/trace "parent" parent)
              (Entity. (name (last keylinks)) parent)))]
        (if (not (nil? propmap))
          (let [props (first propmap)]
            ;; (log/trace "props" props)
            (doseq [[k v] props]
              (let [p (subs (str k) 1)
                    val (get-val-ds v)]
                ;; (log/trace "PROP" p val)
                (.setProperty e p val)))))
        (.put (ds/datastore) e)
        (migae.datastore.PersistentEntityMap. e nil)))
;; (throw-bad-keylinks keylinks)))


;; ;; (defn- emap-new
;; ;;   [^Key k content]
;; ;;   {:pre [(map? content)]}
;; ;;   ;; (log/trace "emap-new " k content)
;; ;;   (let [e (Entity. k)]
;; ;;     (doseq [[k v] content]
;; ;;       (let [prop (subs (str k) 1)
;; ;;             val (get-val-ds v)]
;; ;;         ;; (log/trace "emap-new setting prop: " k prop v val)
;; ;;         (.setProperty e prop val)))
;; ;;     (.put (ds/datastore) e)
;; ;;     (PersistentEntityMap. e nil)))

;; ;; (defn- emap-old
;; ;;   [^Key k ^Entity e content]
;; ;;   {:pre [(map? content)]}
;; ;;   ;; (log/trace "emap old " k content)
;; ;;   (if (empty? content)
;; ;;     (PersistentEntityMap. e nil)
;; ;;     (do
;; ;;       (doseq [[k v] content]
;; ;;         (let [prop (subs (str k) 1)]        ; FIXME - don't exclude ns!
;; ;;           (if (.hasProperty e prop)
;; ;;             (let [pval (.getProperty e prop)
;; ;;                   propval (get-val-ds pval)]
;; ;;               (if (instance? java.util.Collection propval)
;; ;;                 ;; if its already a collection, add the new val
;; ;;                 (do
;; ;;                   (.add propval v)
;; ;;                   (.setProperty e prop propval)
;; ;;                   ;;(log/trace "added val to collection prop")
;; ;;                   )
;; ;;                 ;; if its not a collection, make a collection and add both vals
;; ;;                 (let [newval (ArrayList.)]
;; ;;                   (.add newval propval)
;; ;;                   (.add newval v)
;; ;;                   (.setProperty e (str prop) newval)
;; ;;                   ;;(log/trace "created new collection prop")
;; ;;                   ))
;; ;;               ;;(log/trace "modified entity " e)
;; ;;               (PersistentEntityMap. e nil))
;; ;;             ;; new property
;; ;;             (let [val (get-val-ds v)]
;; ;;               ;; (log/trace "setting val" val (type val))
;; ;;               ;; (flush)
;; ;;               (.setProperty e prop val)))))
;; ;;       (.put (ds/datastore) e)
;; ;;       ;; (log/trace "saved entity " e)
;; ;;       (PersistentEntityMap. e nil))))

;; ;; (defn- emap-update-empty
;; ;;   [keylinks]
;; ;;   (let [k (apply keychain-to-key keylinks)
;; ;;         e (try (.get (ds/datastore) k)
;; ;;                (catch EntityNotFoundException ex nil)
;; ;;                (catch DatastoreFailureException ex (throw ex))
;; ;;                (catch java.lang.IllegalArgumentException ex (throw ex)))]
;; ;;         (if (emap? e)
;; ;;           (PersistentEntityMap. e nil)
;; ;;           (let [e (Entity. k)]
;; ;;             (.put (ds/datastore) e)
;; ;;             (PersistentEntityMap. e nil)))))

;; ;; ;; TODO: support embedded maps, e.g. (ds/emap!! [:Foo/bar] {:a 1, :b {:c 3, :d 4}})
;; ;; ;; technique: store them as embedded entities
;; ;; (defn- emap-update-map
;; ;;   [keylinks content]
;; ;;   ;; (log/trace "emap-update-map " keychain content)
;; ;;   (let [k (apply keychain-to-key keylinks)]
;; ;;     ;; (log/trace "emap-update-map key: " k)
;; ;;     (let [e (if (keyword? k)
;; ;;               (let [e (Entity. (subs (str k) 1))] ;; key of form :Foo, i.e. a Kind specifier
;; ;;                 (.put (ds/datastore) e)
;; ;;                 e)
;; ;;               (try (.get (ds/datastore) k)
;; ;;                    (catch EntityNotFoundException ex nil)
;; ;;                    (catch DatastoreFailureException ex (throw ex))
;; ;;                    (catch java.lang.IllegalArgumentException ex (throw ex))))]
;; ;;       (if (nil? e)
;; ;;         (emap-new k content)
;; ;;         (emap-old k e content) ; even a new one hits this if id autogenned by keychain-to-key
;; ;;         ))))


;; (defn- emap-update-fn
;;   "Second arg is a function to be applied to the Entity whose key is first arg"
;;   [keylinks f]
;;   (if (nil? (namespace (first keylinks)))
;;     ;; if first link in keychain has no namespace, it cannot be an ancestor node
;;     (let [txn (.beginTransaction (ds/datastore)) ;; else new entity
;;           e (Entity. (name (first keylinks)))
;;           em (PersistentEntityMap. e nil)]
;;       (try
;;         (f em)
;;         (.put (ds/datastore) e)
;;         (.commit txn)
;;         (finally
;;           (if (.isActive txn)
;;             (.rollback txn))))
;;       em)
;;     (let [k (apply keychain-to-key keylinks)
;;           e (try (.get (ds/datastore) k)
;;                  (catch EntityNotFoundException e
;;                    ;;(log/trace (.getMessage e))
;;                    e)
;;                  (catch DatastoreFailureException e
;;                    ;;(log/trace (.getMessage e))
;;                    nil)
;;                  (catch java.lang.IllegalArgumentException e
;;                    ;;(log/trace (.getMessage e))
;;                    nil))]
;;       (if (emap? e) ;; existing entity
;;         (let [txn (.beginTransaction (ds/datastore))]
;;           (try
;;             (f e)
;;             (.commit txn)
;;             (finally
;;               (if (.isActive txn)
;;                 (.rollback txn))))
;;           (PersistentEntityMap. e nil))
;;         (let [txn (.beginTransaction (ds/datastore)) ;; else new entity
;;               e (Entity. k)
;;               em (PersistentEntityMap. e nil)]
;;           (try
;;             (f em)
;;             (.put (ds/datastore) e)
;;             (.commit txn)
;;             (finally
;;               (if (.isActive txn)
;;                 (.rollback txn))))
;;           em)))))

;; >>>>>>> 8a635036bed39e4333e2b5b3d62e69d5ddbde433

;; no put - PersistentEntityMap only
;; (defn emap
;;   [keychain em]
;;   (if (empty? keychain)
;;     (throw (IllegalArgumentException. "key vector must not be empty"))
;;     (let [k (apply keychain-to-key keychain)
;;           e (Entity. k)]
;;       (doseq [[k v] em]
;;         (.setProperty e (subs (str k) 1) (get-val-ds v)))
;;       (PersistentEntityMap. e nil))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Actions and modalities
;;
;; Actions:  + extend, = override, - delete, ! replace, ? find
;;   Actions concern what we do with properties - we can add new ones,
;;   and find/override/delete existing ones.

;; Modalities:  ! necessarily, ? possibly (exception on not found)
;; e.g.
;;   emap?? = find possibly - return if found (ignoring props arg) else throw exception
;;
;;
(defn emap+!
  "Extend only, necessarily.  If entity found, extend only (add new
  props, do not override existing).  If entity not found,
  extend (i.e. create and add props)."
  [keylinks & propmap]
  (log/trace "emap+? " keylinks propmap)
  (if (empty? keylinks)
    (throw (IllegalArgumentException. "key vector must not be empty"))
    (if (every? keylink? keylinks)
      (let [akey (apply keychain-to-key keylinks)
            e (try (.get (ds/datastore) akey)
                   (catch EntityNotFoundException ex (Entity. akey)))]
                   ;; (catch DatastoreFailureException ex (throw ex))
                   ;; (catch java.lang.IllegalArgumentException ex (throw ex)))]
        (if (not (nil? propmap))
          (let [props (first propmap)]
            (doseq [[k v] props]
              (let [p (.getProperty e (subs (str k) 1))]
                ;; (log/trace "prop" k v)
                (if (nil? p)
                  (.setProperty e (subs (str k) 1) (get-val-ds v)))))))
        (.put (ds/datastore) e)
        (migae.datastore.PersistentEntityMap. e nil))
      (throw-bad-keylinks keylinks))))

(defn emap+?
  "Extend only, possibly.  If entity found, extend only.  If entity
  not found, throw exception."
  ([keylinks & propmap]
   (log/trace "emap+?" keylinks propmap)
   (if (empty? keylinks)
     (throw (IllegalArgumentException. "key vector must not be empty"))
     (if (every? keylink? keylinks)
       (let [k (apply keychain-to-key keylinks)
             e ;; (try
                 (.get (ds/datastore) k)]
                    ;; (catch EntityNotFoundException ex (throw ex))
                    ;; (catch DatastoreFailureException ex (throw ex))
                    ;; (catch java.lang.IllegalArgumentException ex (throw ex)))]
        (if (not (nil? propmap))
          (let [props (first propmap)]
            (doseq [[k v] props]
              (let [p (.getProperty e (subs (str k) 1))]
                ;; (log/trace "prop" k v)
                (if (nil? p)
                  (.setProperty e (subs (str k) 1) (get-val-ds v)))))))
        (.put (ds/datastore) e)
        (migae.datastore.PersistentEntityMap. e nil))
       (throw-bad-keylinks keylinks)))))

(defn emap=!
  "Override only, necessarily: if found, override existing props but
  do not extend (add new props); if not found, same - override
  existing nil vals."
  [keylinks & propmap]
  (log/trace "emap=! " keylinks propmap)
  (if (empty? keylinks)
    (throw (IllegalArgumentException. "key vector must not be empty"))
    (if (every? keylink? keylinks)
      (let [akey (apply keychain-to-key keylinks)
            e (try (.get (ds/datastore) akey)
                   (catch EntityNotFoundException ex nil))]
                   ;; (catch DatastoreFailureException ex (throw ex))
                   ;; (catch java.lang.IllegalArgumentException ex (throw ex)))]
        (if (nil? e)
          (let [newe (Entity. akey)]
            (if (not (nil? propmap))
              (let [props (first propmap)]
                (doseq [[k v] props]
                      (.setProperty newe (subs (str k) 1) (get-val-ds v)))))
            (.put (ds/datastore) newe)
            (migae.datastore.PersistentEntityMap. newe nil))
          (if (not (nil? propmap))
            (let [props (first propmap)]
              (doseq [[k v] props]
                (let [p (.getProperty e (subs (str k) 1))]
                  ;; (log/trace "prop" k v)
                  (if (not (nil? p))
                    (.setProperty e (subs (str k) 1) (get-val-ds v)))))
              (.put (ds/datastore) e)
              (migae.datastore.PersistentEntityMap. e nil)))))
      (throw-bad-keylinks keylinks))))

(defn emap=?
  "Override only, possibly: if found, override existing props but do
  not extend (add new props); if not found, throw exception."
  [keylinks & propmap]
  (log/trace "emap=? " keylinks propmap)
  (if (empty? keylinks)
    (throw (IllegalArgumentException. "key vector must not be empty"))
    (if (every? keylink? keylinks)
      (let [akey (apply keychain-to-key keylinks)
            e ;;(try
                (.get (ds/datastore) akey)]
                   ;; (catch EntityNotFoundException ex (throw ex))
                   ;; (catch DatastoreFailureException ex (throw ex))
                   ;; (catch java.lang.IllegalArgumentException ex (throw ex)))]
        (if (not (nil? propmap))
          (let [props (first propmap)]
            (doseq [[k v] props]
              (let [p (.getProperty e (subs (str k) 1))]
                ;; (log/trace "prop" k v)
                (if (not (nil? p))
                  (.setProperty e (subs (str k) 1) (get-val-ds v)))))))
        (.put (ds/datastore) e)
        (migae.datastore.PersistentEntityMap. e nil))
      (throw-bad-keylinks keylinks))))

(defn emap+=!
  "Extend and override, necessarily.  If entity found, add new props and override existing
  props.  If entity not found, create it and all all new props."
  [keylinks & propmap]
  (log/trace "emap+=! " keylinks propmap)
  (if (empty? keylinks)
    (throw (IllegalArgumentException. "key vector must not be empty"))
    (if (every? keylink? keylinks)
      (let [akey (apply keychain-to-key keylinks)
            e (try (.get (ds/datastore) akey)
                   (catch EntityNotFoundException ex (Entity. akey)))]
                   ;; (catch DatastoreFailureException ex (throw ex))
                   ;; (catch java.lang.IllegalArgumentException ex (throw ex)))]
        (if (not (nil? propmap))
          (let [props (first propmap)]
            (doseq [[k v] props]
              (.setProperty e (subs (str k) 1) (get-val-ds v)))))
        (.put (ds/datastore) e)
        (migae.datastore.PersistentEntityMap. e nil))
      (throw-bad-keylinks keylinks))))

(defn emap+=?
  "Extend and override, possibly.  If entity found, add new props and
  override existing props.  If entity not found, throw exception."
  [keylinks & propmap]
  (log/trace "emap+=? " keylinks propmap)
  (if (empty? keylinks)
    (throw (IllegalArgumentException. "key vector must not be empty"))
    (if (every? keylink? keylinks)
      (let [akey (apply keychain-to-key keylinks)
            e ;;(try
                (.get (ds/datastore) akey)]
                   ;; (catch EntityNotFoundException ex  (throw ex))
                   ;; (catch DatastoreFailureException ex (throw ex))
                   ;; (catch java.lang.IllegalArgumentException ex (throw ex)))]
        (if (not (nil? propmap))
          (let [props (first propmap)]
            (doseq [[k v] props]
              (.setProperty e (subs (str k) 1) (get-val-ds v)))))
        (.put (ds/datastore) e)
        (migae.datastore.PersistentEntityMap. e nil))
      (throw-bad-keylinks keylinks))))

(defn emap-!
  "Delete, necessarily.  If found, delete props, if not found, return nil."
  [keylinks props]
  (log/trace "emap+=? " keylinks props)
  (if (empty? keylinks)
    (throw (IllegalArgumentException. "key vector must not be empty"))
    (if (every? keylink? keylinks)
      (let [akey (apply keychain-to-key keylinks)
            e (try (.get (ds/datastore) akey)
                   (catch EntityNotFoundException ex nil))]
                   ;; (catch DatastoreFailureException ex (throw ex))
                   ;; (catch java.lang.IllegalArgumentException ex (throw ex)))]
        (if (nil? e)
          nil
          (do
            (doseq [[k v] props]
              (.removeProperty e (subs (str k) 1)))
            (.put (ds/datastore) e)
            (migae.datastore.PersistentEntityMap. e nil))))
      (throw-bad-keylinks keylinks))))

(defn emap-?
  "Delete, possibly.  If found, delete props; if not found, throw exception."
  [keylinks props]
  (log/trace "emap+=? " keylinks props)
  (if (empty? keylinks)
    (throw (IllegalArgumentException. "key vector must not be empty"))
    (if (every? keylink? keylinks)
      (let [akey (apply keychain-to-key keylinks)
            e ;;(try
                (.get (ds/datastore) akey)]
                   ;; (catch EntityNotFoundException ex (throw ex))
                   ;; (catch DatastoreFailureException ex (throw ex))
                   ;; (catch java.lang.IllegalArgumentException ex (throw ex)))]
        (doseq [[k v] props]
          (.removeProperty e (subs (str k) 1)))
        (.put (ds/datastore) e)
        (migae.datastore.PersistentEntityMap. e nil))
      (throw-bad-keylinks keylinks))))

(defn emap!?
  "Replace, possibly.  If entity found, replace (i.e. discard old &
  create new); otherwise throw exception."
  [keylinks & propmap]
  (log/trace "emap!?" keylinks propmap)
  (if (empty? keylinks)
    (throw (IllegalArgumentException. "key vector must not be empty"))
    (if (every? keylink? keylinks)
      (let [k (apply keychain-to-key keylinks)
            e (.get (ds/datastore) k)] ;; throws EntityNotFoundException
        ;; e was found; replace it
        (let [newe (Entity. k)]
          (if (not (nil? propmap))
            (let [props (first propmap)]
              (doseq [[k v] props]
                (.setProperty newe (subs (str k) 1) (get-val-ds v)))))
          (.put (ds/datastore) newe)
          (migae.datastore.PersistentEntityMap. newe nil)))
      (throw-bad-keylinks keylinks))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; (defn emaps!!
;;   "Replace *some* entities necessarily, discarding what's already there."
  ;;  NOT SUPPORTED: no way to decide which entities to replace, given indef. keychain.
  ;;  Replacement always requires a definite keychain

  ;; [keylinks maps]
  ;; (log/trace "")
  ;; (log/trace "emaps!!" keylinks maps)
  ;; (if (keylink? (last keylinks))
  ;;   (throw (IllegalArgumentException. "emaps!! keylinks must end in a Kind keyword (e.g. :Foo); try emap!!"))
  ;;   ;; (let [s
  ;;   (doseq [emap maps]
  ;;     (do
  ;;       ;; (log/trace "emap" emap)
  ;;       (emap?! keylinks emap)))
  ;;   ;; ]
  ;;     ;; (doseq [item s]
  ;;     ;;   (log/trace "item" (meta item) item)
  ;;     ;;   (log/trace "item entity" (.content item)))
  ;;     ))

  ;; )

(defn emaps?!
  "[keylinks] propmaps

  Find *some* entities necessarily, creating if not found.  Last
  keylink must be kind keyword, i.e. no namespace, e.g. :Foo.

  If propmaps is a vector of maps, create one entity per map.

  If propmaps is a keyword map, it must contain at least one predicate
  clause, e.g. :a '(> 2), in order to form a property filter.  Queries
  for entities matching the filter; if no matches, create entity for
  map, otherwise return matches, ignoring map argument.

  Examples:
      (emaps?! [:Foo] [{:a 1} {:a 2} {:a 3}]) saves three Entities of kind :Foo
      (emaps?! [:Foo] {:a 1 :b '(> 2)}]) returns :Foo entities with :b > 2; if none, creates one.
  "

  [keylinks maps]
  (log/trace "")
  (log/trace "emaps?!" keylinks maps)
  (if (improper-keychain? keylinks)
    (if (map? maps)
      (emap?! keylinks maps)
      (doseq [emap maps]
        (do
          (emap?! keylinks emap))))
    (throw (IllegalArgumentException. "emaps?! keylinks must end in a Kind keyword (e.g. :Foo); try emap!!"))
    ))


(defmacro emaps!
  [kind pred]
  (log/trace pred)
  (let [op (first pred)]
    (case op
      and (log/trace "AND")
      or (log/trace "OR")
      > (log/trace "GT")
      >= `(Query$FilterPredicate. ~(subs (str (second pred)) 1)
                                  Query$FilterOperator/GREATER_THAN_OR_EQUAL
                                  ~(last pred))
      )))

;; <<<<<<< HEAD
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
      ;; (cond
      ;;   (= 1 (count keylinks))
      ;;   (let [kw (first keylinks)]
      ;;     (if (keyword? kw)
      ;;       (if (nil? (namespace kw))
      ;;         'KindVec
      ;;         'KeyVec)
      ;;       (throw (IllegalArgumentException. "keylinks must be keywords"))))
      ;;   (= 2 (count keylinks))
      ;;   (let [kw1 (first keylinks)
      ;;         kw2 (last keylinks)]
      ;;     (if (and (keyword? kw1) (keyword? kw2))
      ;;       (if (nil? (namespace kw2))
      ;;         'Ancestor                   ; e.g. [:Foo/Bar :Baz]
      ;;         'Keychain)
      ;;       ;; 'Multikey)                  ; e.g. #{:Foo/Bar :Baz/Buz}
      ;;       (if (and (key? kw1) (keyword? kw2))
      ;;         (if (nil? (namespace kw2))
      ;;           'Ancestor                   ; e.g. [:Foo/Bar :Baz]
      ;;           'Keychain)
      ;;         (throw (IllegalArgumentException. "both keylinks must be keywords")))))
      ;; (key? keylinks)
      ;; 'Key
      ;; (keyword? keylinks)
      ;; (if (nil? (namespace keylinks))
      ;;   'Kind                           ; e.g.  :Foo
      ;;   'Key)                          ; e.g.  :Foo/Bar
      ;;   :else
      ;;   'Multikey)
      ;; :else
      ;; (throw (IllegalArgumentException. "arg must be key, keyword, or a vector of keywords")))))
;; =======

;; (defmacro & [preds]
;;   )
;; >>>>>>> 8a635036bed39e4333e2b5b3d62e69d5ddbde433

(defmulti emaps??
  (fn [keylinks & pm]
    (log/trace "")
    (log/trace "emaps??" keylinks pm)
    ;; (if (vector? keylinks)
    (if (every? keylink? (butlast keylinks))
      (cond
        (empty? keylinks)
        'Kindless
        (keykind? (last keylinks))
        'Kinded
        (keylink? (last keylinks))
        'Keychain  ;; illegal? only one possible match
        ;; (vector? keylinks)
        ;; (if (keylink? (last keylinks))
        ;;   'Keychain  ;; illegal? only one possible match
        ;;   'Kinded)
        (set? keylinks)
        'Multikey
        :else
        (throw (RuntimeException. "emaps??: bad keychain" keylinks))
        )
      (throw-bad-keylinks (butlast keylinks)))))

(defmethod emaps?? 'Keychain
  [keylinks & pm]             ; e.g. :Foo/Bar
  (log/trace "emaps?? Keychain:" keylinks pm)
;; <<<<<<< HEAD
  (let [dskey (keychain-to-key (first keylinks) (second keylinks))
        e (try (.get (ds/datastore) dskey)
         (catch EntityNotFoundException e (throw e))
         (catch Exception e (throw e)))]
;; =======
;;   (let [dskey (keychain-to-key (first keylinks) (second keylinks))
;;         e ;;(try
;;             (.get (ds/datastore) dskey)]
;;                ;; (catch EntityNotFoundException ex (throw ex))
;;                ;; (catch Exception ex (throw ex)))]
;; >>>>>>> 8a635036bed39e4333e2b5b3d62e69d5ddbde433
    (migae.datastore.PersistentEntityMap. e nil)))

(defn- finish-q
  [q]
  ;; (log/trace "finish-q")
  (let [pq (.prepare (ds/datastore) q)
        it (.asIterator pq)
        em-seq (emap-seq it)]
    ;; (log/trace "em-seq " em-seq)
    em-seq))

(defn- do-filter
  [q filter]
  (log/trace "do-filter" filter)
  )

(defn- kinded-no-ancestor
  [q & filter-map]
  ;; (log/trace "kinded-no-ancestor" q filter-map)
  (if (nil? (first filter-map))
    (finish-q q)
    ;; FIXME:  support multiple filters using
    ;;  Query$CompositeFilterOperator.and and  Query$CompositeFilterOperator.or

    (let [filter (if (map? filter-map)    ; constraint is the filter map
                   filter-map
                   (first filter-map))
          ;; foo (log/trace "kinded-no-ancestor filter:" filter)
          mapentry (first filter)
          fld (subs (str (first mapentry)) 1) ;; strip leading ':'
          ;; foo (log/trace "kinded-no-ancestor fld:" fld)
          constraint (last mapentry)
          ;; foo (log/trace "kinded-no-ancestor constraint:" constraint (type constraint))
          op (if (list? constraint)
               (first constraint)
               '=)
          ;; foo (log/trace "kinded-no-ancestor op:" op)
          operand (if (list? constraint)
                    (last constraint)
                    (last mapentry))
          ;; foo (log/trace "kinded-no-ancestor operand:" operand)
          ]
      (if (not (nil? filter))
;; (do-filter ...
;; FIXME: multiple clauses, e.g. {:study-id 132451 :user-id 43563546}
        (let [filter
              (Query$FilterPredicate. fld
                                      (cond
                                        (= op '<)
                                        Query$FilterOperator/LESS_THAN
                                        (= op '<=)
                                        Query$FilterOperator/LESS_THAN_OR_EQUAL
                                        (= op '=)
                                        Query$FilterOperator/EQUAL
                                        (= op '>)
                                        Query$FilterOperator/GREATER_THAN
                                        (= op '>=)
                                        Query$FilterOperator/GREATER_THAN_OR_EQUAL
                                        :else (throw (IllegalArgumentException.
                                                      (str "illegal predicate op " op))))
                                      operand)
                     ;; (Query$FilterPredicate. (subs (str fld) 1) ; remove ':' prefix
                     ;;                         Query$FilterOperator/EQUAL
                     ;;                         operand))
              q (.setFilter q filter)]))
      (finish-q q))))

      ;; (let [pq (.prepare (ds/datastore) q)
      ;;       it (.asIterator pq)
      ;;       emseq (emap-seq it)]
      ;;   emseq))))

(defmethod emaps?? 'Kinded
  [keylinks & filter-map]
  (log/trace "emaps?? Kinded:" keylinks filter-map (type filter-map))
  (let [kind (name (last keylinks)) ;; we already know last link has form :Foo
        ;; foo (log/trace "kind:" kind (type kind))
        q (Query. kind)
        pfx (butlast keylinks)
        ;; foo (log/trace "pfx" pfx (type pfx))
        ]
    (if (nil? pfx)
      (if (map? filter-map)
        (do
          (kinded-no-ancestor q filter-map))
        (do
          (kinded-no-ancestor q (first filter-map))))
      ;; we have a prefix keychain, so set it as ancestor constraint
      (let [k (apply keychain-to-key pfx)
            ;; kf (log/trace "K:" k (type k))
            qq (.setAncestor q k)]
        (if (not (nil? filter-map))
          (do-filter qq filter-map)
          (finish-q q))))
    ))

(defmethod emaps?? 'Kindless
  [[k] & filter-map]           ; e.g [] {:migae/gt :Foo/Bar}
  "Kindless queries take a null keychain and an optional map specifying a key filter, of the form

    {:migae/gt [:Foo/Bar]}"
  ;; (log/trace "emaps?? Kindless:" k filter-map)
  (if (nil? filter-map)
    (let [q (Query.)
          pq (.prepare (ds/datastore) q)
          it (.asIterator pq)
          emseq (emap-seq it)]
      emseq)
    (if (> (count filter-map) 1)
      (throw (IllegalArgumentException. "only one filter expr, for now"))
      (let [constraint (first filter-map)
            op (first constraint)
            keylinks  (last constraint)]
        (log/trace "constraint:" constraint op keylinks)
        (if (keylink? (last keylinks))
          (let [;f (ffirst filter-map)
;; <<<<<<< HEAD
                ;; k (apply keychain-to-key keychain)
;; =======
                 k (apply keychain-to-key keylinks)
;; >>>>>>> 8a635036bed39e4333e2b5b3d62e69d5ddbde433
                foo (log/trace "KEY: " k)
                filter (Query$FilterPredicate. Entity/KEY_RESERVED_PROPERTY
                                               (cond
                                                 (= op '<)
                                                 Query$FilterOperator/LESS_THAN
                                                 (= op '<=)
                                                 Query$FilterOperator/LESS_THAN_OR_EQUAL
                                                 (= op '=)
                                                 Query$FilterOperator/EQUAL
                                                 (= op '>)
                                                 Query$FilterOperator/GREATER_THAN
                                                 (= op '>=)
                                                 Query$FilterOperator/GREATER_THAN_OR_EQUAL)
                                               ;; (keychain-to-key (first (second op)) (rest (second op))))
                                               k)
                q (.setFilter (Query.) filter)
                pq (.prepare (ds/datastore) q)
                it (.asIterator pq)
                emseq (emap-seq it)]
            emseq)
          (throw (IllegalArgumentException.
                  "emaps?? kindless query key filter: all elements must be keylinks (not kind only)")))
        ))))

;; Kind query
(defmethod emaps?? 'Kind               ; e.g. :Foo
  [^clojure.lang.Keyword kind]
  (log/trace "emaps?? Kind")
  (let [q (Query. (name kind))
        pq (.prepare (ds/datastore) q)
        it (.asIterator pq)
        emseq (emap-seq it)]
    ;; (log/trace "EMSeq: " emseq)
    ;; (log/trace "EMSeq es: " emseq)
        ;; res (seq (.asIterable pq))]
        ;; res (iterator-seq (.asIterator pq))]
    ;; (iterator-seq it)))
    emseq))

(defmethod emaps?? 'KindVec               ; e.g. [:Foo]
  [[^clojure.lang.Keyword kind]]
  (log/trace "emapss?? KindVec:")
  (let [q (Query. (name kind))
        pq (.prepare (ds/datastore) q)
        it (.asIterator pq)
        em-seq (emap-seq it)]
    em-seq))

(defmethod emaps?? 'Key
  ;; [^clojure.lang.Keyword k]             ; e.g. :Foo/Bar
  [^Key k]
  (log/trace "emapss?? Key:")
  (let [;; dskey (key k)
        e ;;(try
            (.get (ds/datastore) k)]
         ;; (catch EntityNotFoundException ex (throw ex))
         ;; (catch Exception ex (throw ex)))]
    (migae.datastore.PersistentEntityMap. e nil)))

(defmethod emaps?? 'Multikey
  [key-set]
  (log/trace "emapss?? Multikey:")
  )

;; <<<<<<< HEAD
;; =======
(defn alter!
  "Replace existing entity, or create a new one."
  [keylinks content]
  ;; if second arg is a map, treat it ...
  ;; if second arg is a function, ...
  (if (empty? keylinks)
    (throw (IllegalArgumentException. "key vector must not be empty"))
    (let [k (apply keychain-to-key keylinks)
          e (Entity. k)]
      (doseq [[k v] content]
        (.setProperty e (subs (str k) 1) v))
      (.put (ds/datastore) e)
      ;; (log/trace "created and put entity " e)
      (PersistentEntityMap. e nil))))

;; (defn assoc!
;;   "unsafe assoc with save but no txn for DS Entities"
;;   [m k v & kvs]
;;   ;; (log/trace "assoc! " m k v  "&" kvs)
;;    (let [txn (.beginTransaction (ds/datastore))
;;          coll (if (emap? m)
;;                 (.content m)
;;                 (if (= (class m) Entity)
;;                   m
;;                   (do (log/trace "HELP: assoc!") (flush))))]
;;      (try
;;        (.setProperty coll (subs (str k) 1) v)
;;        (if (nil? (first kvs))
;;          (try
;;            (.put (ds/datastore) coll)
;;            (.commit txn)
;;            (finally
;;              (if (.isActive txn)
;;                (.rollback txn))))
;;          (do ;; (log/trace "recur on assoc!")
;;              (assoc! coll (first kvs) (second kvs) (nnext kvs))))
;;        (finally
;;          (if (.isActive txn)
;;                (.rollback txn))))
;;        coll))

(defn assoc!!
  "safe assoc with save and txn for DS Entities"
  [m k v & kvs]
  {:pre [(nil? (namespace k))]}
   (let [txn (.beginTransaction (ds/datastore))
         coll (if (emap? m)
                (.content m)
                (if (= (class m) Entity)
                  m
                  (log/trace "HELP: assoc!!")))]
     (try
       (.setProperty coll (subs (str k) 1) v)
       (if (nil? (first kvs))
         (try
           (.put (ds/datastore) coll)
           (.commit txn)
           (finally
             (if (.isActive txn)
               (.rollback txn))))
         (assoc!! coll (first kvs) (second kvs) (nnext kvs)))
       (finally
         (if (.isActive txn)
           (.rollback txn))))
     (if (emap? coll)
       coll
       (if (= (class coll) Entity)
         (PersistentEntityMap. coll nil)
         (log/trace "EXCEPTION assoc!!")))))


  ;; (if (empty? keylinks)
  ;;   (do
  ;;     (log/trace "emap?? predicate-map filter" filter-map (type filter-map))
  ;;     (let [ks (keylinks filter-map)
  ;;           vs (vals filter-map)
  ;;           k  (subs (str (first ks)) 1)
  ;;           v (last vs)
  ;;           f (Query$FilterPredicate. k Query$FilterOperator/EQUAL v)]
  ;;       (log/trace (format "key: %s, val: %s" k v))))
  ;;   (let [k (if (coll? keylinks)
  ;;             (apply keychain-to-key keylinks)
  ;;             (apply keychain-to-key [keylinks]))
  ;;         ;; foo (log/trace "emap?? kw keylinks: " k)
  ;;         e (try (.get (ds/datastore) k)
  ;;                (catch EntityNotFoundException ex (throw ex))
  ;;                (catch DatastoreFailureException ex (throw ex))
  ;;                (catch java.lang.IllegalArgumentException ex (throw ex)))]
  ;;     (PersistentEntityMap. e nil))))

;; >>>>>>> 8a635036bed39e4333e2b5b3d62e69d5ddbde433
;; Filter propertyFilter =
;;   new FilterPredicate("height",
;;                       FilterOperator.GREATER_THAN_OR_EQUAL,
;;                       minHeight);
;; Query q = new Query("Person").setFilter(propertyFilter);


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; (defn- get-keychain
;;   [^Key k]
;;   (let [parent (.getParent k)
;;         kind (.getKind k)
;;         nm (.getName k)
;;         id (.getId k)
;;         kw (if (nil? nm)
;;              (keyword kind (str id))
;;               (keyword kind nm))]
;;     (if (nil? parent)
;;       [kw]
;;       (into [kw]
;;              (get-keychain parent)))))

;; FIXME:  since we've implemented IPersistentMap etc. these are no longer needed:
(defmulti to-edn
  (fn [arg & args]
    [(type arg) (type args)]))

(defmethod to-edn [clojure.lang.Keyword nil]
  ([^clojure.lang.Keyword k])
  )

(defmethod to-edn [migae.datastore.PersistentEntityMap nil]
  [^migae.datastore.PersistentEntityMap e]
  e)

(defmethod to-edn [Entity nil]
  ;;[com.google.appengine.api.datastore.Entity nil]
  [^Entity e]
  (let [k (.getKey e)
        kch (keychain k)
        props (into {} (.getProperties e))
        em (into {} {:kind_ (.getKind k) :ident_ (identifier k)})]
        ;; em (into {} {:key kch})]
    (into em (for [[k v] props] [(keyword k) v]))))

;; taken verbatim from 1.7.0
;; (defn into
;;   "Returns a new coll consisting of to-coll with all of the items of
;;   from-coll conjoined. A transducer may be supplied."
;;   {:added "1.0"
;;    :static true}
;;   ([to from]
;;    (log/trace "ds/into")
;;      (if (instance? clojure.lang.IEditableCollection to)
;;        ;; (with-meta (persistent! (reduce conj! (transient to) from)) (meta to))
;;        (let [res (persistent! (reduce conj! (transient to) from))]
;;          (log/trace "ds/into result w/o meta:" (epr res))
;;          (let [metameta (merge (meta to) (meta from))] ; the fix
;;            (log/trace "ds/into metameta:" (epr metameta))
;;            (let [result (with-meta res metameta)]
;;              (log/trace "ds/into result w/meta:" (epr result))
;;              result)))
;;        (reduce conj to from)))
;;   ([to xform from]
;;      (if (instance? clojure.lang.IEditableCollection to)
;;        (with-meta (persistent! (transduce xform conj! (transient to) from)) (meta to))
;;        (transduce xform conj to from))))

;; for Clojure < 1.7 we need to borrow from 1.7:
;; (defn reduce  ;; from 1.7 lang/core.clj
;;   ([f coll]
;;    (log/trace "ds/reduce v1.7")
;;      (if (instance? clojure.lang.IReduce coll)
;;        (.reduce ^clojure.lang.IReduce coll f)
;;        (clojure.core.protocols/coll-reduce coll f)))
;;   ([f val coll]
;;      ;; (if (instance? clojure.lang.IReduceInit coll)
;;        ;; (.reduce ^clojure.lang.IReduceInit coll f val)
;;      (if (instance? clojure.lang.IReduce coll)
;;        (.reduce ^clojure.lang.IReduce coll f val)
;;        (clojure.core.protocols/coll-reduce coll f val))))

;; (defn into  ;; from 1.7 lang/core.clj
;;   "Returns a new coll consisting of to-coll with all of the items of
;;   from-coll conjoined. A transducer may be supplied."
;;   {:added "1.0"
;;    :static true}
;;   ([to from]
;;    (log/trace "ds/into v1.7")
;;      (if (instance? clojure.lang.IEditableCollection to)
;;        (with-meta (persistent! (reduce conj! (transient to) from)) (meta to))
;;        (reduce conj to from))))
;;   ;; ([to xform from]
;;   ;;    (if (instance? clojure.lang.IEditableCollection to)
;;   ;;      (with-meta (persistent! (transduce xform conj! (transient to) from)) (meta to))
;;   ;;      (transduce xform conj to from))))
