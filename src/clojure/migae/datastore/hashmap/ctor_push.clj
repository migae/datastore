(in-ns 'migae.datastore)

(defn put-kinded-emap
  [keychain & data]
  ;; precon: improper keychain is validated
  ;; (log/trace "keychain" keychain)
  ;; (log/trace "data" data)
  (let [em (first data)
        kind (clj/name (last keychain))
        ;; foo (log/trace "kind" kind)
        parent-keychain (vec (butlast keychain))
        e (if (nil? parent-keychain)
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
(defn entity-map!
  ([keychain]
   (into-ds! keychain))
  ([keychain em]
   (into-ds! keychain em))
  ([force keychain em]
   (into-ds! force keychain em)))

(defn into-ds!
  ;; FIXME: convert to [keychain & em]
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
    (throw (IllegalArgumentException. (str "Invalid keychain" keychain)))))
  ([force keychain em]
  "Forcibly put entity-map to datastore, replacing anything there"
  ;; (log/trace "force into-ds! " force keychain em)
  ;; (if (clj/empty? keychain)
  ;;   (throw (IllegalArgumentException. "keychain vector must not be empty"))
  (if (not= force :force)
    (throw (IllegalArgumentException. force)))
  (cond
    (ekey/improper-keychain? keychain)
    (put-kinded-emap keychain em)
    (ekey/proper-keychain? keychain)
    (put-proper-emap :keychain keychain :propmap em :force true)
    :else
    (throw (IllegalArgumentException. (str "Invalid keychain" keychain))))))


;;   ([keychain] ;; erase: replace with empty entity
;;    (if (clj/empty? keychain)
;;      (throw (IllegalArgumentException. "keychain vector must not be empty"))
;;      (let [k (try (apply ekey/keychain-to-key keychain)
;;                   (catch java.lang.RuntimeException ex
;;                     (log/trace (.getMessage ex)) ex)
;; ;; FIXME use java.lang.IllegalArgumentException instead of custom exceptions?
;;                   (catch IllegalArgumentException ex
;;                     ;; (log/trace (.getMessage ex))
;;                     ex)
;;                   (catch IllegalArgumentException ex
;;                     ;; (log/trace (.getMessage ex))
;;                     ex)
;;                   )]
;;        (cond
;;          (nil? k) ;; bad keychain?
;;          nil
;;          (= (type k) IllegalArgumentException)
;;          ;;(do (log/trace "invalid keychain: " keychain) k)
;;          k
;;          (= (type k) IllegalArgumentException)
;;          ;;(do (log/trace "partial keychain: " keychain) k)
;;          k
;;          :else
;;          (let [e (Entity. k)]
;;            (.put (ds/datastore) e)
;;            (PersistentEntityMap. e nil)))))))

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
  [^Key k val]
  {:pre [(map? val)]}
  ;; (log/trace "emap-new " k val)
  (let [e (Entity. k)]
    (doseq [[k v] val]
      (let [prop (subs (str k) 1)
            val (get-val-ds v)]
        ;; (log/trace "emap-new setting prop: " k prop v val)
        (.setProperty e prop val)))
    (.put (ds/datastore) e)
    (PersistentEntityMap. e nil)))

(defn- emap-old
  [^Key k ^Entity e val]
  {:pre [(map? val)]}
  ;; (log/trace "emap old " k val)
  (if (clj/empty? val)
    (PersistentEntityMap. e nil)
    (do
      (doseq [[k v] val]
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
  [keychain val]
  ;; (log/trace "emap-update-map " keychain val)
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
        (emap-new k val)
        (emap-old k e val) ; even a new one hits this if id autogenned by ekey/keychain-to-key
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
  "Syntax:  (emap!! [<keychain>] val)

  Modify existing entity, or create a new one.  If the existing emap
  contains a property that is specified in <val>, make it a
  collection and add the new value.

  If there is no second arg, an empty entity will be created and stored.

  If the second arg is a map, it will be treated as an entity map, and
  the entity identified by the first (keychain) arg will be updated to
  match the emap.  This will be done in a transaction.

  If the second arg is a function, it must take one arg, which will be
  the entity.  The function's job is to update the entity.  The
  machinery ensures that this will be done in a transaction."
  [keychain & val]
  ;; val may be a map or a function taking one arg, which is the entitye whose key is ^keychain
  ;; map: update absolutely; current state of entity irrelevant
  ;; function: use if updating depends on current state
  ;; (log/trace "args: " keychain val)
  (if (clj/empty? keychain)
    (throw (IllegalArgumentException. "keychain vector must not be empty"))
    (if (clj/empty? val)
      (emap-update-empty keychain)
      (if (map? (first val))
        (emap-update-map keychain (first val))
        (if (fn? (first val))
          (emap-update-fn keychain (first val))
          (throw (IllegalArgumentException. "val must be map or function")))))))
