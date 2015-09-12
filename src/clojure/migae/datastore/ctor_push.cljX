(ns migae.datastore.ctor-push
;;(in-ns 'migae.datastore)
  (:import [com.google.appengine.api.datastore
            Entity ;; EmbeddedEntity EntityNotFoundException
            ;;Key KeyFactory KeyFactory$Builder
            ])
  (:require
   [clojure.core :as clj]
;;   [migae.datastore.adapter :refer :all]
   [migae.datastore.keychain :as ekey]
  ))

(println "reloading ctor-push")

(defn put-kinded-emap
  [keychain & data]
  ;; precon: improper keychain is validated
  ;; (log/trace "keychain" keychain)
  ;; (log/trace "data" data)
  (let [em (first data)
        kind (clj/name (last keychain))
        parent-keychain (vec (butlast keychain))
        ;; foo (log/trace "foo" parent-keychain)
        e (if (empty? parent-keychain)
            (Entity. kind)
            (do #_(log/trace "parent-keychain" parent-keychain)
                #_(log/trace "parent-key" (ekey/keychain-to-key parent-keychain))
                (Entity. kind  (ekey/keychain-to-key parent-keychain))
                ))]
    (when (not (empty? em))
      (doseq [[k v] (seq em)]
        (.setProperty e (subs (str k) 1) (get-val-ds v))))
    (.put (ds/datastore) e)
    (PersistentEntityMap. e nil)))

(defn- put-proper-emap
  [& {:keys [keychain propmap force] :or {force false,}}]
  ;; precon: keychain has already been validated
  ;; (log/trace "keychain" keychain)
  ;; (log/trace "propmap" propmap)
  ;; (log/trace "force" force)
  (let [k (ekey/keychain-to-key keychain)
        e (if (not force)
            (do #_(log/trace "not force")
            (let [ent (try (.get (ds/datastore) k)
                           (catch EntityNotFoundException ex ex))]
              (if (= (type ent) Entity) ;; found
                (throw (RuntimeException. "Key already used"))
                (Entity. k))))
            (do #_(log/trace "force push")
                (Entity. k)))]
    (when (not (empty? propmap))
      (doseq [[k v] propmap]
        (.setProperty e (subs (str k) 1) (get-val-ds v))))
    (.put (ds/datastore) e)
    (PersistentEntityMap. e nil)))

(declare into-ds!)
;; FIXME:  replace emap! with entity-map!
;; TODO: support nil data map, e.g. (entity-map! [:A/B])

(defn into-ds!
  ;; FIXME: convert to [keychain & em]???
  ([arg]
   (cond
     (map? arg)
     (do
       (log/trace "FOOBAR")
       ;; edn, e.g.  (entity-map! ^{:migae [:a/b]} {:a 1})
       (let [k (:migae/keychain (meta arg))]
         (log/debug "edn key: " k " improper?" (ekey/improper-keychain? k))
         (cond
           (ekey/improper-keychain? k)
           (put-kinded-emap k arg)
           (ekey/proper-keychain? k)
           (put-proper-emap :keychain k :propmap arg :force true)
           :else (throw (IllegalArgumentException. (str "INVALID KEYCHAIN!: " k)))))
       )
     (keychain? arg)
     (do
       (cond
         (ekey/improper-keychain? arg)
         (put-kinded-emap arg {})
         (ekey/proper-keychain? arg)
         (put-proper-emap :keychain arg :propmap {} :force true)
         :else (throw (IllegalArgumentException. (str "Invalid keychain" arg)))))
     :else (throw (IllegalArgumentException.))))
  ([keychain em]
  "Put entity-map to datastore unless already there; use :force true to replace existing"
  ;; (log/trace "into-ds! " keychain em)
  ;; (if (clj/empty? keychain)
  ;;   (throw (IllegalArgumentException. "keychain vector must not be empty"))
  (cond
    (ekey/improper-keychain? keychain)
    (put-kinded-emap keychain em)
    (ekey/proper-keychain? keychain)
    (put-proper-emap :keychain keychain :propmap em)
    :else
    (throw (IllegalArgumentException. (str "Invalid keychain : " keychain)))))
  ([mode keychain em]
  "Modally put entity-map to datastore.  Modes: :force, :multi"
  ;; (log/trace "force into-ds! " force keychain em)
  ;; (if (clj/empty? keychain)
  ;;   (throw (IllegalArgumentException. "keychain vector must not be empty"))
  ;; (if (not= force :force)
  ;;   (throw (IllegalArgumentException. force)))
  (cond
    (= mode :force)
    (cond
      (ekey/improper-keychain? keychain)
      (put-kinded-emap keychain em)
      (ekey/proper-keychain? keychain)
      (put-proper-emap :keychain keychain :propmap em :force true)
      :else
      (throw (IllegalArgumentException. (str "Invalid keychain" keychain))))
    (= mode :multi)
    (do
      ;; (log/trace "entity-map! :multi processing...")
      (if (ekey/improper-keychain? keychain)
        (if (vector? em)
          (do
            (for [emap em]
              (do
                ;; (log/trace "ctoring em" (print-str emap))
                (entity-map! keychain emap))))
          (throw (IllegalArgumentException. ":multi ctor requires vector of maps")))
        (throw (IllegalArgumentException. ":multi ctor requires improper keychain"))))
    :else
      (throw (IllegalArgumentException. (str "Invalid mode keyword:" force))))
    ))


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
     ;;       (PersistentEntityMap. e nil))
     ;;     (PersistentEntityMap. e nil))))))

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
    (.put (ds/datastore) e)
    (PersistentEntityMap. e nil)))

(defn- emap-old
  [^Key k ^Entity e content]
  {:pre [(map? content)]}
  ;; (log/trace "emap old " k content)
  (if (clj/empty? content)
    (PersistentEntityMap. e nil)
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
              (PersistentEntityMap. e nil))
            ;; new property
            (let [val (get-val-ds v)]
              ;; (log/trace "setting val" val (type val))
              ;; (flush)
              (.setProperty e prop val)))))
      (.put (ds/datastore) e)
      ;; (log/trace "saved entity " e)
      (PersistentEntityMap. e nil))))

(defn- emap-update-empty
  [keychain]
  (let [k (apply ekey/keychain-to-key keychain)
        e (try (.get (ds/datastore) k)
               (catch EntityNotFoundException e nil)
               (catch DatastoreFailureException e (throw e))
               (catch java.lang.IllegalArgumentException e (throw e)))]
        (if (emap? e)
          (PersistentEntityMap. e nil)
          (let [e (Entity. k)]
            (.put (ds/datastore) e)
            (PersistentEntityMap. e nil)))))

;; TODO: support embedded maps, e.g. (ds/emap!! [:Foo/bar] {:a 1, :b {:c 3, :d 4}})
;; technique: store them as embedded entities
(defn- emap-update-map
  [keychain content]
  ;; (log/trace "emap-update-map " keychain content)
  (let [k (apply ekey/keychain-to-key keychain)]
    ;; (log/trace "emap-update-map key: " k)
    (let [e (if (keyword? k)
              (let [e (Entity. (subs (str k) 1))] ;; key of form :Foo, i.e. a Kind specifier
                (.put (ds/datastore) e)
                e)
              (try (.get (ds/datastore) k)
                   (catch EntityNotFoundException e nil)
                   (catch DatastoreFailureException e (throw e))
                   (catch java.lang.IllegalArgumentException e (throw e))))]
      (if (nil? e)
        (emap-new k content)
        (emap-old k e content) ; even a new one hits this if id autogenned by ekey/keychain-to-key
        ))))

(defn- emap-update-fn
  "Second arg is a function to be applied to the Entity whose key is first arg"
  [keychain f]
  (if (nil? (clj/namespace (first keychain)))
    ;; if first link in keychain has no namespace, it cannot be an ancestor node
    (let [txn (.beginTransaction (ds/datastore)) ;; else new entity
          e (Entity. (clj/name (first keychain)))
          em (PersistentEntityMap. e nil)]
      (try
        (f em)
        (.put (ds/datastore) e)
        (.commit txn)
        (finally
          (if (.isActive txn)
            (.rollback txn))))
      em)
    (let [k (apply ekey/keychain-to-key keychain)
          e (try (.get (ds/datastore) k)
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
        (let [txn (.beginTransaction (ds/datastore))]
          (try
            (f e)
            (.commit txn)
            (finally
              (if (.isActive txn)
                (.rollback txn))))
          (PersistentEntityMap. e nil))
        (let [txn (.beginTransaction (ds/datastore)) ;; else new entity
              e (Entity. k)
              em (PersistentEntityMap. e nil)]
          (try
            (f em)
            (.put (ds/datastore) e)
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
;;     (throw (IllegalArgumentException. "keychain vector must not be empty"))
;;     (if (keylink? (last keylinks))
;;       (if (every? keylink? (butlast keylinks))
;;         (emap-definite!! keylinks propmap)
;;         (throw-bad-keylinks (butlast keylinks)))
;;       (if (keykind? (last keylinks))
;;         (apply emap-indefinite!! keylinks propmap)
;;         (apply throw-bad-keykind (butlast keylinks))))))

;; FIXME: rename to into-ds!
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
    (throw (IllegalArgumentException. "keychain vector must not be empty"))
    (if (clj/empty? content)
      (emap-update-empty keychain)
      (if (map? (first content))
        (emap-update-map keychain (first content))
        (if (fn? (first content))
          (emap-update-fn keychain (first content))
          (throw (IllegalArgumentException. "content must be map or function")))))))
