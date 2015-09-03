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

