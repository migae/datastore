(in-ns 'migae.datastore)

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
      (PersistentEntityMap. e nil))))

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
  ;;     (PersistentEntityMap. e nil))))

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
           (PersistentEntityMap. e nil))
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
           (PersistentEntityMap. e nil))
         (throw (RuntimeException. "key already exists: " keychain)))))))

