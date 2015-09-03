(in-ns 'migae.datastore)

  ;; (:refer-clojure :exclude [name hash key])
  ;; (:import [com.google.appengine.api.datastore])
  ;;           ;; Entity
  ;;           ;; Key])
  ;; (:require [clojure.tools.logging :as log :only [trace debug info]]))

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

(defn entity-map?
  [em]
  (= (type em) migae.datastore.PersistentEntityMap))

(defn emap? ;; OBSOLETE - use entity-map?
  [em]
  (entity-map? em))

(defn entity?
  [e]
  (= (type e) Entity))

;; (defn empty?
;;   [em]
;;   (= (count em) 0))

;; no put - PersistentEntityMap only
(defn entity-map
  "create PersistentEntityMap object"
  [keychain em]
  ;; (log/trace "keychain: " keychain " em: " em)
  (if (clj/empty? keychain)
    (throw (IllegalArgumentException. "keychain vector must not be empty"))
    (let [k (ekey/keychain-to-key keychain)
          ;; foo (log/trace "k: " k)
          e (Entity. k)]
      (doseq [[k v] em]
        (.setProperty e (subs (str k) 1) (get-val-ds v)))
      (PersistentEntityMap. e))))

(defn get-kinded-emap
  [keychain & data]
  ;; precon: improper keychain is validated
  (log/trace "get-kinded-emap " keychain)
  ;; (log/trace "keychain" keychain)
  ;; (log/trace "data" data)
  ;; (let [em (first data)
  ;;       kind (clj/name (last keychain))
  ;;       ;; foo (log/trace "kind" kind)
  ;;       parent-keychain (vec (butlast keychain))
  ;;       e (if (nil? parent-keychain)
  ;;           (Entity. kind)
  ;;           (do #_(log/trace "parent-keychain" parent-keychain)
  ;;               #_(log/trace "parent-key" (ekey/keychain-to-key parent-keychain))
  ;;               (Entity. kind  (ekey/keychain-to-key parent-keychain))
  ;;               ))]
  ;;   (when (not (empty? em))
  ;;     (doseq [[k v] (seq em)]
  ;;       (.setProperty e (subs (str k) 1) (get-val-ds v))))
  ;;   (.put (ds/datastore) e)
  ;;   (PersistentEntityMap. e)))
  )
;;  (throw (IllegalArgumentException. (str "kinded keychain" keychain))))

(defn get-proper-emap
  [keychain & data]
  ;; precon: keychain has already been validated
  (log/trace "get-proper-emap" keychain)
  ;; (log/trace "keychain" keychain)
  ;; (log/trace "data" data)
  ;; (let [k (ekey/keychain-to-key keychain)
  ;;       em (first data)
  ;;       e (Entity. k)]
  ;;   (when (not (empty? em))
  ;;     (doseq [[k v] em]
  ;;       (.setProperty e (subs (str k) 1) (get-val-ds v))))
  ;;   (.put (ds/datastore) e)
  ;;   (PersistentEntityMap. e)))
  )

;; OBSOLETE - use entity-map for consistency with hash-map, array-map, etc
(defn emap
  "create PersistentEntityMap object"
  [keychain em]
  (entity-map keychain em))
  ;; (if (clj/empty? keychain)
  ;;   (throw (IllegalArgumentException. "keychain vector must not be empty"))
  ;;   (let [k (apply keychain-to-key keychain)
  ;;         e (Entity. k)]
  ;;     (doseq [[k v] em]
  ;;       (.setProperty e (subs (str k) 1) (get-val-ds v)))
  ;;     (PersistentEntityMap. e))))

;; getter
(defn get-ds
  [keychain & em]
  (log/trace "get-ds " keychain)
   (cond
     (ekey/improper-keychain? keychain)
     (get-kinded-emap keychain)
     (ekey/proper-keychain? keychain)
     (get-proper-emap keychain)
     :else
     (throw (IllegalArgumentException. (str "Invalid keychain" keychain)))))

   ;; (if (clj/empty? keychain)
   ;;   (throw (IllegalArgumentException. "keychain vector must not be empty"))
   ;;   (let [k (ekey/keychain-to-key keychain)
   ;;         e (.get (ds/datastore) k)]
   ;;     (log/trace "key: " k)
   ;;     (log/trace "e: " e)
   ;;     ;; throw  EntityNotFoundException
   ;;                ;; (catch EntityNotFoundException e nil)
   ;;                ;; (catch DatastoreFailureException e (throw e))
   ;;                ;; (catch java.lang.IllegalArgumentException e (throw e)))]
   ;;     (PersistentEntityMap. e))))

;; pull constructor
(defmulti entity-map*
  "Pull constructor.  Retrieve entities with exact, complete match; if
  no matches, throw exception."
  ;; FIXME:  implement map matching
  ;; FIXME:  throw notfound exception
    (fn [keychain & valmap]
      ;; (log/trace "keychain: " keychain)
      ;; (log/trace "type keychain: " (type keychain))
      ;; (log/trace "valmap: " valmap)
      ;; (log/trace "type valmap: " (type valmap))
      ;; (log/trace "first valmap: " (first valmap))
      ;; (log/trace "type first valmap: " (type (first valmap)))
      ;; (flush)
      [(type keychain) (type (first valmap))])) ;; (first valmap) should be the val map
(defmethod entity-map* [clojure.lang.PersistentVector nil]
  [keychain & args]
  (get-ds keychain))
(defmethod entity-map* [clojure.lang.PersistentVector clojure.lang.PersistentArrayMap]
  [keychain & valmap]
  (get-ds keychain))

;; FIXME: custom exception
(defn into-ds
  ([keychain em]
   "non-destructively update datastore; fail if entity already exists"
   ;; (log/trace "emap! 2 args " keychain em)
   (if (clj/empty? keychain)
     (throw (IllegalArgumentException. "keychain vector must not be empty"))
     (let [k (apply ekey/keychain-to-key keychain)
           e (try (.get (ds/datastore) k)
                  (catch EntityNotFoundException e nil))
                  ;; (catch DatastoreFailureException e (throw e))
                  ;; (catch java.lang.IllegalArgumentException e (throw e)))
           ]
       ;; FIXME: test for exception type instead?
       ;; (case e
       ;;   PersistentEntityMap (throw (RuntimeException. (str "key already exists: " keychain)))
       ;;   EntityNotFoundException ...
       ;;   (throw ...)) ;; default case
       (if (nil? e) ;; not found, so go ahead and construct
         (let [e (Entity. k)]
           (doseq [[k v] em]
             (.setProperty e (subs (str k) 1) (get-val-ds v)))
           (.put (ds/datastore) e)
           (PersistentEntityMap. e))
         (throw (RuntimeException. (str "key already exists: " keychain)))))))
  ([keychain] ;; create empty entity
   (if (clj/empty? keychain)
     (throw (IllegalArgumentException. "keychain vector must not be empty"))
     (let [k (apply ekey/keychain-to-key keychain)
           e (try (.get (ds/datastore) k)
                  (catch EntityNotFoundException e nil)
                  (catch DatastoreFailureException e (throw e))
                  (catch java.lang.IllegalArgumentException e (throw e)))]
       (if (nil? e) ;; not found
         (let [e (Entity. k)]
           (.put (ds/datastore) e)
           (PersistentEntityMap. e))
         (throw (RuntimeException. "key already exists: " keychain)))))))


     ;; (let [k (apply ekey/keychain-to-key keychain)
     ;;       e (try (.get (ds/datastore) k)
     ;;              (catch EntityNotFoundException e
     ;;                ;;(log/trace (.getMessage e))
     ;;                e)
     ;;              (catch DatastoreFailureException e
     ;;                ;;(log/trace (.getMessage e))
     ;;                nil)
     ;;              (catch java.lang.IllegalArgumentException e
     ;;                ;;(log/trace (.getMessage e))
     ;;                nil))
     ;;       ]
     ;;   ;; (log/trace "emap! got e: " e)
     ;;   (if (nil? e)
     ;;     (let [e (Entity. k)]
     ;;       (.put (ds/datastore) e)
     ;;       (PersistentEntityMap. e))
     ;;     (PersistentEntityMap. e))))))


;; (defn emap??
;;   "Find, possibly.  If entity found, return without change, otherwise
;;   throw exception."
;;   ;; [keylinks & propmap]
;;   [keylinks]
;;    (log/trace "emap??" keylinks)
;;    (if (clj/empty? keylinks)
;;      (throw (IllegalArgumentException. "keychain vector must not be empty"))
;;      (if (every? keylink? keylinks)
;;        (let [k (apply keychainer keylinks)
;;              e ;;(try
;;                  (.get (ds/datastore) k)]
;;                     ;; (catch EntityNotFoundException ex (throw ex))
;;                     ;; (catch DatastoreFailureException ex (throw ex))
;;                     ;; (catch java.lang.IllegalArgumentException ex (throw ex)))]
;;          (PersistentEntityMap. e))
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
              (apply ekey/keychain-to-key keylinks)
              (apply ekey/keychain-to-key [keylinks]))
          ;; foo (log/trace "emap?? kw keylinks: " k)
          e (try (.get (ds/datastore) k)
                 (catch EntityNotFoundException e (throw e))
                 (catch DatastoreFailureException e (throw e))
                 (catch java.lang.IllegalArgumentException e (throw e)))]
      (PersistentEntityMap. e))))

(declare find-definite-necessarily find-indefinite-necessarily
         throw-bad-keylinks)

;; FIXME:  remove this. instead use entity-map! :force, etc
(defn entity-map?!
  "Find, necessarily.  If keychain is improper (ends in a Kind
  keyword), will create new entity; i.e., ?! means 'find indefinite -
  find _some_ entity of this kind'.  If keychain is proper (all
  keylinks), then ?! means 'find exactly _this_ entity' gets the
  entity; if found, returns it, otherwise creates and returns new
  entity."
  [keylinks & propmap]
  (log/trace "emap?!" keylinks propmap)
  (if (clj/empty? keylinks)
    (throw (IllegalArgumentException. "key vector must not be empty"))
    (if (keyword? (last keylinks))
      (if (nil? (namespace (last keylinks)))
        (apply find-indefinite-necessarily keylinks propmap)
        (apply find-definite-necessarily keylinks propmap))
    (throw-bad-keylinks keylinks))))

(defn emap?!
  [keylinks & propmap]
  (log/trace "emap?!" keylinks propmap)
  (entity-map?! keylinks propmap))
