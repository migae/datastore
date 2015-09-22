(in-ns 'migae.datastore)

;(clojure.core/println "loading ctor-push")

(declare into-ds!)

;; (defn entity-map!
;;   ([keychain]
;;    (into-ds! keychain))
;;   ([keychain em]
;;    {:pre [(map? em)
;;           (vector? keychain)
;;           (not (empty? keychain))
;;           (every? keyword? keychain)]}
;;    (do
;;      (into-ds! keychain em)
;;      ))
;;   ([force keychain em]
;;    {:pre [(or (map? em) (vector? em))
;;           (vector? keychain)
;;           (not (empty? keychain))
;;           (every? keyword? keychain)]}
;;    (into-ds! force keychain em)))

;; (defn put-kinded-emap
;;   [keyvec & data]
;;   ;; precon: improper keychain is validated
;;   ;; (println (str "put-kinded-emap: " keyvec data))
;;   ;; (log/debug "data" data)
;;   (let [em (first data)
;;         kind (name (last keyvec))
;;         parent-keychain (vec (butlast keyvec))
;;         ;; foo (log/debug "foo" parent-keychain)
;;         e (if (empty? parent-keychain)
;;             (Entity. kind)
;;             (do #_(log/debug "parent-keychain" parent-keychain)
;;                 #_(log/debug "parent-key" (keychain-to-key parent-keychain))
;;                 (Entity. kind  (keychain-to-key parent-keychain))
;;                 ))]
;;     (when (not (empty? em))
;;       (doseq [[k v] (seq em)]
;;         (.setProperty e (subs (str k) 1) (get-val-ds v))))
;;     ;; (.put (.content store-map) e)
;;     (let [pem (PersistentEntityMap. e nil)
;;           ds (into store-map pem)]
;;       pem)))

;; (defn- put-proper-emap
;;   [& {:keys [keyvec propmap force] :or {force false,}}]
;;   ;; precon: keychain has already been validated
;;   ;; (log/debug "keychain" keyvec)
;;   ;; (log/debug "propmap" propmap)
;;   ;; (log/debug "force" force)
;;   (let [k (keychain-to-key keyvec)
;;         e (if (not force)
;;             (do #_(log/debug "not force")
;;             (let [ent (try (.get store-map k)
;;                            (catch EntityNotFoundException ex ex))]
;;               (if (= (type ent) Entity) ;; found
;;                 (throw (RuntimeException. "Key already used"))
;;                 (Entity. k))))
;;             (do #_(log/debug "force push")
;;                 (Entity. k)))]
;;     (when (not (empty? propmap))
;;       (doseq [[k v] propmap]
;;         (.setProperty e (subs (str k) 1) (get-val-ds v))))
;;     (let [pem (PersistentEntityMap. e nil)
;;           ds (.content store-map)]
;;       ;; (log/debug "ctor-push.put-proper-emap ds: " ds (type ds))
;;       (into store-map pem)
;;       pem)))

      ;; (.put ds e))))

;; (defn into-ds!
;;   ;; FIXME: convert to [keychain & em]???
;;   ([arg]
;;    (do
;;      (log/debug "into-ds! 1" arg)
;;      (cond
;;        (map? arg)
;;        (do
;;          ;; edn, e.g.  (entity-map! ^{:migae/keychain [:a/b]} {:a 1})
;;          (let [k (:migae/keychain (meta arg))]
;;            ;; (log/debug "edn key: " k " improper?" (improper-keychain? k))
;;            (cond
;;              (improper-keychain? k)
;;              (put-kinded-emap k arg)
;;              (proper-keychain? k)
;;              (put-proper-emap :keyvec k :propmap arg :force true)
;;              :else (throw (IllegalArgumentException. (str "INVALID KEYCHAIN!: " k)))))
;;          )
;;        (keychain? arg)
;;        (do
;;          (cond
;;            (improper-keychain? arg)
;;            (put-kinded-emap arg {})
;;            (proper-keychain? arg)
;;            (put-proper-emap :keyvec arg :propmap {} :force true)
;;            :else (throw (IllegalArgumentException. (str "Invalid keychain" arg)))))
;;        :else (throw (IllegalArgumentException.)))))
;;   ([keychain em]
;;   "Put entity-map to datastore unless already there; use :force true to replace existing"
;;   (do
;;     ;; (log/debug "into-ds! 2" keychain em)
;;     ;; (if (empty? keychain)
;;     ;;   (throw (IllegalArgumentException. "keychain vector must not be empty"))
;;     (cond
;;       (improper-keychain? keychain)
;;       (put-kinded-emap keychain em)
;;       (proper-keychain? keychain)
;;       (put-proper-emap :keyvec keychain :propmap em)
;;       :else
;;       (throw (IllegalArgumentException. (str "Invalid keychain : " keychain))))))
;;   ([mode keychain em]
;;   "Modally put entity-map to datastore.  Modes: :force, :multi"
;;   (do
;;     ;; (log/debug "force into-ds! 3" force keychain em)
;;     ;; (if (empty? keychain)
;;     ;;   (throw (IllegalArgumentException. "keychain vector must not be empty"))
;;     ;; (if (not= force :force)
;;     ;;   (throw (IllegalArgumentException. force)))
;;     (cond
;;       (= mode :force)
;;       (cond
;;         (improper-keychain? keychain)
;;         (put-kinded-emap keychain em)
;;         (proper-keychain? keychain)
;;         (put-proper-emap :keyvec keychain :propmap em :force true)
;;         :else
;;         (throw (IllegalArgumentException. (str "Invalid keychain" keychain))))
;;       (= mode :multi)
;;       (do
;;         ;; (log/debug "entity-map! :multi processing...")
;;         (if (improper-keychain? keychain)
;;           (if (vector? em)
;;             (do
;;               (for [emap em]
;;                 (do
;;                   ;; (log/debug "ctoring em" (print-str emap))
;;                   (entity-map! keychain emap))))
;;             (throw (IllegalArgumentException. ":multi ctor requires vector of maps")))
;;           (throw (IllegalArgumentException. ":multi ctor requires improper keychain"))))
;;       :else
;;       (throw (IllegalArgumentException. (str "Invalid mode keyword:" force))))
;;     )))

;; (defn- emap-new
;;   [^Key k content]
;;   {:pre [(map? content)]}
;;   ;; (log/debug "emap-new " k content)
;;   (let [e (Entity. k)]
;;     (doseq [[k v] content]
;;       (let [prop (subs (str k) 1)
;;             val (get-val-ds v)]
;;         ;; (log/debug "emap-new setting prop: " k prop v val)
;;         (.setProperty e prop val)))
;;     (.put (.content store-map) e)
;;     (PersistentEntityMap. e nil)))

;; (defn- emap-old
;;   [^Key k ^Entity e content]
;;   {:pre [(map? content)]}
;;   ;; (log/debug "emap old " k content)
;;   (if (empty? content)
;;     (PersistentEntityMap. e nil)
;;     (do
;;       (doseq [[k v] content]
;;         (let [prop (subs (str k) 1)]        ; FIXME - don't exclude ns!
;;           (if (.hasProperty e prop)
;;             (let [pval (.getProperty e prop)
;;                   propval (get-val-ds pval)]
;;               (if (instance? java.util.Collection propval)
;;                 ;; if its already a collection, add the new val
;;                 (do
;;                   (.add propval v)
;;                   (.setProperty e prop propval)
;;                   ;;(log/debug "added val to collection prop")
;;                   )
;;                 ;; if its not a collection, make a collection and add both vals
;;                 (let [newval (ArrayList.)]
;;                   (.add newval propval)
;;                   (.add newval v)
;;                   (.setProperty e (str prop) newval)
;;                   ;;(log/debug "created new collection prop")
;;                   ))
;;               ;;(log/debug "modified entity " e)
;;               (PersistentEntityMap. e nil))
;;             ;; new property
;;             (let [val (get-val-ds v)]
;;               ;; (log/debug "setting val" val (type val))
;;               ;; (flush)
;;               (.setProperty e prop val)))))
;;       (.put (.content store-map) e)
;;       ;; (log/debug "saved entity " e)
;;       (PersistentEntityMap. e nil))))

;; (defn- emap-update-empty
;;   [keychain]
;;   (let [k (apply keychain-to-key keychain)
;;         e (try (.get store-map k)
;;                (catch EntityNotFoundException e nil)
;;                (catch DatastoreFailureException e (throw e))
;;                (catch java.lang.IllegalArgumentException e (throw e)))]
;;         (if (entity-map? e)
;;           (PersistentEntityMap. e nil)
;;           (let [e (Entity. k)]
;;             (.put (.content store-map) e)
;;             (PersistentEntityMap. e nil)))))

;; ;; TODO: support embedded maps, e.g. (ds/emap!! [:Foo/bar] {:a 1, :b {:c 3, :d 4}})
;; ;; technique: store them as embedded entities
;; (defn- emap-update-map
;;   [keychain content]
;;   ;; (log/debug "emap-update-map " keychain content)
;;   (let [k (apply keychain-to-key keychain)]
;;     ;; (log/debug "emap-update-map key: " k)
;;     (let [e (if (keyword? k)
;;               (let [e (Entity. (subs (str k) 1))] ;; key of form :Foo, i.e. a Kind specifier
;;                 (.put (.content store-map) e)
;;                 e)
;;               (try (.get store-map k)
;;                    (catch EntityNotFoundException e nil)
;;                    (catch DatastoreFailureException e (throw e))
;;                    (catch java.lang.IllegalArgumentException e (throw e))))]
;;       ;; (if (nil? e)
;;       ;;   (emap-new k content)
;;       ;;   (emap-old k e content) ; even a new one hits this if id autogenned by keychain-to-key
;;         )))

;; (defn- emap-update-fn
;;   "Second arg is a function to be applied to the Entity whose key is first arg"
;;   [keychain f]
;;   (if (nil? (namespace (first keychain)))
;;     ;; if first link in keychain has no namespace, it cannot be an ancestor node
;;     (let [txn (.beginTransaction store-map) ;; else new entity
;;           e (Entity. (name (first keychain)))
;;           em (PersistentEntityMap. e nil)]
;;       (try
;;         (f em)
;;         (.put (.content store-map) e)
;;         (.commit txn)
;;         (finally
;;           (if (.isActive txn)
;;             (.rollback txn))))
;;       em)
;;     (let [k (apply keychain-to-key keychain)
;;           e (try (.get store-map k)
;;                  (catch EntityNotFoundException e
;;                    ;;(log/debug (.getMessage e))
;;                    e)
;;                  (catch DatastoreFailureException e
;;                    ;;(log/debug (.getMessage e))
;;                    nil)
;;                  (catch java.lang.IllegalArgumentException e
;;                    ;;(log/debug (.getMessage e))
;;                    nil))]
;;       (if (entity-map? e) ;; existing entity
;;         (let [txn (.beginTransaction store-map)]
;;           (try
;;             (f e)
;;             (.commit txn)
;;             (finally
;;               (if (.isActive txn)
;;                 (.rollback txn))))
;;           (PersistentEntityMap. e nil))
;;         (let [txn (.beginTransaction store-map) ;; else new entity
;;               e (Entity. k)
;;               em (PersistentEntityMap. e nil)]
;;           (try
;;             (f em)
;;             (.put (.content store-map) e)
;;             (.commit txn)
;;             (finally
;;               (if (.isActive txn)
;;                 (.rollback txn))))
;;           em)))))

;; ;; (defn emap!!
;; ;;   "Replace, necessarily.  Create new, discarding old even if found (so
;; ;;   don't bother searching)."
;; ;;   [keylinks & propmap]
;; ;;   (log/debug "emap!!" keylinks propmap)
;; ;;   (if (empty? keylinks)
;; ;;     (throw (IllegalArgumentException. "keychain vector must not be empty"))
;; ;;     (if (keylink? (last keylinks))
;; ;;       (if (every? keylink? (butlast keylinks))
;; ;;         (emap-definite!! keylinks propmap)
;; ;;         (throw-bad-keylinks (butlast keylinks)))
;; ;;       (if (keykind? (last keylinks))
;; ;;         (apply emap-indefinite!! keylinks propmap)
;; ;;         (apply throw-bad-keykind (butlast keylinks))))))

;; ;; FIXME: rename to into-ds!
;; (defn emap!!
;;   "Syntax:  (emap!! [<keychain>] content)

;;   Modify existing entity, or create a new one.  If the existing emap
;;   contains a property that is specified in <content>, make it a
;;   collection and add the new value.

;;   If there is no second arg, an empty entity will be created and stored.

;;   If the second arg is a map, it will be treated as an entity map, and
;;   the entity identified by the first (keychain) arg will be updated to
;;   match the emap.  This will be done in a transaction.

;;   If the second arg is a function, it must take one arg, which will be
;;   the entity.  The function's job is to update the entity.  The
;;   machinery ensures that this will be done in a transaction."
;;   [keychain & content]
;;   ;; content may be a map or a function taking one arg, which is the entitye whose key is ^keychain
;;   ;; map: update absolutely; current state of entity irrelevant
;;   ;; function: use if updating depends on current state
;;   ;; (log/debug "args: " keychain content)
;;   (if (empty? keychain)
;;     (throw (IllegalArgumentException. "keychain vector must not be empty"))
;;     (if (empty? content)
;;       (emap-update-empty keychain)
;;       (if (map? (first content))
;;         (emap-update-map keychain (first content))
;;         (if (fn? (first content))
;;           (emap-update-fn keychain (first content))
;;           (throw (IllegalArgumentException. "content must be map or function")))))))
