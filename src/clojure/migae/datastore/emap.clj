(in-ns 'migae.datastore)

  ;; (:refer-clojure :exclude [name hash key])
  ;; (:import [com.google.appengine.api.datastore])
  ;;           ;; Entity
  ;;           ;; Key])
  ;; (:require [clojure.tools.logging :as log :only [trace debug info]]))

;;(load "datastore/entity_map")
;;(load "datastore/service")

;; (declare get-val-clj get-val-ds)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;  single '?'  means predicate
;;  double '??' means expression as query (= sync expression against datastore)
;; examples:
;;  (emap?? []) everything
;;  (emap?? [:Foo])  ; every :Foo
;;  (emap?? [:Foo] {:a 1})  ; kind+property filter
;;  (emap?? [:Foo/Bar]) key filter
;;  (emap?? {:a 1})  ; property filter
;;  (emap?? [:Foo/Bar :Baz] ; ancestor filter
;;  (emap?? [:Foo/Bar :migae/*] ; kindless ancestor filter: all childs of :Foo/Bar
;;  (emap?? [:Foo/Bar :migae/**] ; kindless ancestor filter: all descendants of :Foo/Bar

(defn emap?
  [em]
  (= (type em) migae.datastore.EntityMap))

(defn entity?
  [e]
  (= (type e) Entity))

;; (defn empty?
;;   [em]
;;   (= (count em) 0))

;; no put - EntityMap only
(defn entity-map
  "create EntityMap object"
  [keychain em]
  (if (clj/empty? keychain)
    (throw (IllegalArgumentException. "key vector must not be empty"))
    (let [k (apply keychain-to-key keychain)
          e (Entity. k)]
      (doseq [[k v] em]
        (.setProperty e (subs (str k) 1) (get-val-ds v)))
      (EntityMap. e))))

;; OBSOLETE - use entity-map for consistency with hash-map, array-map, etc
(defn emap
  "create EntityMap object"
  [keychain em]
  (if (clj/empty? keychain)
    (throw (IllegalArgumentException. "key vector must not be empty"))
    (let [k (apply keychain-to-key keychain)
          e (Entity. k)]
      (doseq [[k v] em]
        (.setProperty e (subs (str k) 1) (get-val-ds v)))
      (EntityMap. e))))

;; (defn emap!
;;   "Like emap!! but works with partial keychain, so will always create new"
;;   [keylinks & propmap]
;;   (log/trace "emap!" keylinks propmap)
;;   (if (clj/empty? keylinks)
;;     (throw (IllegalArgumentException. "key vector must not be empty"))
;;     (if (every? keylink? (butlast keylinks))
;;       (if (and (keyword? (last keylinks)) (nil? (namespace (last keylinks))))
;;         (let [k (if (> (count keylinks) 1)
;;                   (apply keychainer (butlast keylinks)))
;;               e (if (> (count keylinks) 1)
;;                   (Entity. (subs (str (last keylinks)) 1)  k)
;;                   (Entity. (subs (str (last keylinks)) 1)))]
;;           (if (not (nil? propmap))
;;             (let [props (first propmap)]
;;               (doseq [[k v] props]
;;                 (.setProperty e (subs (str k) 1) (get-val-ds v)))))
;;           (.put (datastore) e)
;;           (EntityMap. e))
;;         (throw (IllegalArgumentException. "last element of key vector must be name only (i.e. kind keyword), e.g. :Customer")))
;;       (throw-bad-keylinks keylinks))))

;; without override - discard body if entity already exists
(defn emap!
  ([keychain em]
   ;; (log/trace "emap! 2 args " keychain em)
   (if (clj/empty? keychain)
     (throw (IllegalArgumentException. "key vector must not be empty"))
     (let [k (apply keychain-to-key keychain)
           e (try (.get (datastore) k)
                  (catch EntityNotFoundException e nil)
                  (catch DatastoreFailureException e (throw e))
                  (catch java.lang.IllegalArgumentException e (throw e)))
           ]
       ;; (log/trace "2 got e " e (type e))
       (if (nil? e)
         (let [e (Entity. k)]
           (doseq [[k v] em]
             (.setProperty e (subs (str k) 1) (get-val-ds v)))
           (.put (datastore) e)
           ;; (log/trace "created and put entity " e)
           (EntityMap. e))
         (do
           ;; (log/trace "found entity " e)
           ;; if em content not null throw exception
           (EntityMap. e))))))
  ;; emap!
  ([keychain]
   ;; (log/trace "emap! 1 " keychain)
   (if (clj/empty? keychain)
     (throw (IllegalArgumentException. "key vector must not be empty"))
     (let [k (apply keychain-to-key keychain)
           e (try (.get (datastore) k)
                  (catch EntityNotFoundException e
                    ;;(log/trace (.getMessage e))
                    e)
                  (catch DatastoreFailureException e
                    ;;(log/trace (.getMessage e))
                    nil)
                  (catch java.lang.IllegalArgumentException e
                    ;;(log/trace (.getMessage e))
                    nil))
           ]
       ;; (log/trace "emap! got e: " e)
       (if (nil? e)
         (let [e (Entity. k)]
           (.put (datastore) e)
           (EntityMap. e))
         (EntityMap. e))))))

(defn- emap-new
  [^Key k content]
  {:pre [(map? content)]}
  ;; (log/trace "emap-new " k content)
  (let [e (Entity. k)]
    (doseq [[k v] content]
      (let [prop (subs (str k) 1)
            val (get-val-ds v)]
        ;; (log/trace "emap-new setting prop: " k prop v val)
        (.setProperty e prop val)))
    (.put (datastore) e)
    (EntityMap. e)))

(defn- emap-old
  [^Key k ^Entity e content]
  {:pre [(map? content)]}
  ;; (log/trace "emap old " k content)
  (if (clj/empty? content)
    (EntityMap. e)
    (do
      (doseq [[k v] content]
        (let [prop (subs (str k) 1)]        ; FIXME - don't exclude ns!
          (if (.hasProperty e prop)
            (let [pval (.getProperty e prop)
                  propval (get-val-ds pval)]
              (if (instance? java.util.Collection propval)
                ;; if its already a collection, add the new val
                (do
                  (.add propval v)
                  (.setProperty e prop propval)
                  ;;(log/trace "added val to collection prop")
                  )
                ;; if its not a collection, make a collection and add both vals
                (let [newval (ArrayList.)]
                  (.add newval propval)
                  (.add newval v)
                  (.setProperty e (str prop) newval)
                  ;;(log/trace "created new collection prop")
                  ))
              ;;(log/trace "modified entity " e)
              (EntityMap. e))
            ;; new property
            (let [val (get-val-ds v)]
              ;; (log/trace "setting val" val (type val))
              ;; (flush)
              (.setProperty e prop val)))))
      (.put (datastore) e)
      ;; (log/trace "saved entity " e)
      (EntityMap. e))))

(defn- emap-update-empty
  [keychain]
  (let [k (apply keychain-to-key keychain)
        e (try (.get (datastore) k)
               (catch EntityNotFoundException e nil)
               (catch DatastoreFailureException e (throw e))
               (catch java.lang.IllegalArgumentException e (throw e)))]
        (if (emap? e)
          (EntityMap. e)
          (let [e (Entity. k)]
            (.put (datastore) e)
            (EntityMap. e)))))

;; TODO: support embedded maps, e.g. (ds/emap!! [:Foo/bar] {:a 1, :b {:c 3, :d 4}})
;; technique: store them as embedded entities
(defn- emap-update-map
  [keychain content]
  ;; (log/trace "emap-update-map " keychain content)
  (let [k (apply keychain-to-key keychain)]
    ;; (log/trace "emap-update-map key: " k)
    (let [e (if (keyword? k)
              (let [e (Entity. (subs (str k) 1))] ;; key of form :Foo, i.e. a Kind specifier
                (.put (datastore) e)
                e)
              (try (.get (datastore) k)
                   (catch EntityNotFoundException e nil)
                   (catch DatastoreFailureException e (throw e))
                   (catch java.lang.IllegalArgumentException e (throw e))))]
      (if (nil? e)
        (emap-new k content)
        (emap-old k e content) ; even a new one hits this if id autogenned by keychain-to-key
        ))))

(defn- emap-update-fn
  "Second arg is a function to be applied to the Entity whose key is first arg"
  [keychain f]
  (if (nil? (clj/namespace (first keychain)))
    ;; if first link in keychain has no namespace, it cannot be an ancestor node
    (let [txn (.beginTransaction (datastore)) ;; else new entity
          e (Entity. (clj/name (first keychain)))
          em (EntityMap. e)]
      (try
        (f em)
        (.put (datastore) e)
        (.commit txn)
        (finally
          (if (.isActive txn)
            (.rollback txn))))
      em)
    (let [k (apply keychain-to-key keychain)
          e (try (.get (datastore) k)
                 (catch EntityNotFoundException e
                   ;;(log/trace (.getMessage e))
                   e)
                 (catch DatastoreFailureException e
                   ;;(log/trace (.getMessage e))
                   nil)
                 (catch java.lang.IllegalArgumentException e
                   ;;(log/trace (.getMessage e))
                   nil))]
      (if (emap? e) ;; existing entity
        (let [txn (.beginTransaction (datastore))]
          (try
            (f e)
            (.commit txn)
            (finally
              (if (.isActive txn)
                (.rollback txn))))
          (EntityMap. e))
        (let [txn (.beginTransaction (datastore)) ;; else new entity
              e (Entity. k)
              em (EntityMap. e)]
          (try
            (f em)
            (.put (datastore) e)
            (.commit txn)
            (finally
              (if (.isActive txn)
                (.rollback txn))))
          em)))))

;; (defn emap!!
;;   "Replace, necessarily.  Create new, discarding old even if found (so
;;   don't bother searching)."
;;   [keylinks & propmap]
;;   (log/trace "emap!!" keylinks propmap)
;;   (if (clj/empty? keylinks)
;;     (throw (IllegalArgumentException. "key vector must not be empty"))
;;     (if (keylink? (last keylinks))
;;       (if (every? keylink? (butlast keylinks))
;;         (emap-definite!! keylinks propmap)
;;         (throw-bad-keylinks (butlast keylinks)))
;;       (if (keykind? (last keylinks))
;;         (apply emap-indefinite!! keylinks propmap)
;;         (apply throw-bad-keykind (butlast keylinks))))))

(defn emap!!
  "Syntax:  (emap!! [<keychain>] content)

  Modify existing entity, or create a new one.  If the existing emap
  contains a property that is specified in <content>, make it a
  collection and add the new value.

  If there is no second arg, an empty entity will be created and stored.

  If the second arg is a map, it will be treated as an entity map, and
  the entity identified by the first (keychain) arg will be updated to
  match the emap.  This will be done in a transaction.

  If the second arg is a function, it must take one arg, which will be
  the entity.  The function's job is to update the entity.  The
  machinery ensures that this will be done in a transaction."
  [keychain & content]
  ;; content may be a map or a function taking one arg, which is the entitye whose key is ^keychain
  ;; map: update absolutely; current state of entity irrelevant
  ;; function: use if updating depends on current state
  ;; (log/trace "args: " keychain content)
  (if (clj/empty? keychain)
    (throw (IllegalArgumentException. "key vector must not be empty"))
    (if (clj/empty? content)
      (emap-update-empty keychain)
      (if (map? (first content))
        (emap-update-map keychain (first content))
        (if (fn? (first content))
          (emap-update-fn keychain (first content))
          (throw (IllegalArgumentException. "content must be map or function")))))))

;; (defn emap??
;;   "Find, possibly.  If entity found, return without change, otherwise
;;   throw exception."
;;   ;; [keylinks & propmap]
;;   [keylinks]
;;    (log/trace "emap??" keylinks)
;;    (if (clj/empty? keylinks)
;;      (throw (IllegalArgumentException. "key vector must not be empty"))
;;      (if (every? keylink? keylinks)
;;        (let [k (apply keychainer keylinks)
;;              e ;;(try
;;                  (.get (datastore) k)]
;;                     ;; (catch EntityNotFoundException ex (throw ex))
;;                     ;; (catch DatastoreFailureException ex (throw ex))
;;                     ;; (catch java.lang.IllegalArgumentException ex (throw ex)))]
;;          (EntityMap. e))
;;        (throw-bad-keylinks keylinks))))

(defn emap??                            ;FIXME: use a multimethod?
  "return matching emaps"
  [keylinks & filter-map]  ; keychain and predicate-map
  ;; {:pre []} ;; check types
  (log/trace "emap?? keylinks" keylinks (type keylinks))
  (log/trace "emap?? filter-map" filter-map (type filter-map))
;; )
  (if (clj/empty? keylinks)
    (do
      (log/trace "emap?? predicate-map filter" filter-map (type filter-map))
      )
      ;; (let [ks (clj/keylinks filter-map)
      ;;       vs (vals filter-map)
      ;;       k  (subs (str (first ks)) 1)
      ;;       v (last vs)
      ;;       f (Query$FilterPredicate. k Query$FilterOperator/EQUAL v)]
      ;;   (log/trace (format "key: %s, val: %s" k v))))
    (let [k (if (coll? keylinks)
              (apply keychain-to-key keylinks)
              (apply keychain-to-key [keylinks]))
          ;; foo (log/trace "emap?? kw keylinks: " k)
          e (try (.get (datastore) k)
                 (catch EntityNotFoundException e (throw e))
                 (catch DatastoreFailureException e (throw e))
                 (catch java.lang.IllegalArgumentException e (throw e)))]
      (EntityMap. e))))
