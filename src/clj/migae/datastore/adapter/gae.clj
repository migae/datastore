(clojure.core/println "Start load of migae.datastore.adapter.gae")

;; this should never return unwrapped Entity or Key objects

(ns migae.datastore.adapter.gae
  (:import [java.lang IllegalArgumentException RuntimeException]
           [java.util
            Collection
            Collections
            ArrayList
            HashMap HashSet
            Map Map$Entry
            Vector]
           [com.google.appengine.api.datastore
            Blob
            DatastoreFailureException
            DatastoreService
            DatastoreServiceFactory
            DatastoreServiceConfig
            DatastoreServiceConfig$Builder
            Email
            Entity EmbeddedEntity EntityNotFoundException
            FetchOptions$Builder
            ImplicitTransactionManagementPolicy
            Key KeyFactory KeyFactory$Builder
            Link
            PhoneNumber
            ReadPolicy ReadPolicy$Consistency
            Query Query$SortDirection
            Query$FilterOperator Query$FilterPredicate
            Query$CompositeFilter Query$CompositeFilterOperator
            ShortBlob
            Text
            Transaction]
           [migae.datastore PersistentEntityMap DuplicateKeyException InvalidKeychainException]
           )
  (:require [clojure.tools.logging :as log :only [debug info]]
            [clojure.tools.reader.edn :as edn]
            [migae.datastore.keys :as k]
            [migae.datastore :refer [->PersistentEntityMap]]
            ;; [migae.datastore.types.entity-map :refer [->PersistentEntityMap]]
            ;; [migae.datastore.types.entity-map-seq]
            ;; [migae.datastore.api :as ds]
            ))

;; TODO: make each adapter a typedef implementing spi protocol?
;; use reify?

(clojure.core/println "loading migae.datastore.adapter.gae")

(def ds  ;; store-map
  (let [dsm (DatastoreServiceFactory/getDatastoreService)
;;        psm (ds/->PersistentStoreMap dsm nil nil)
        ]
    dsm))

(declare into-ds! entity-key)
(declare get-val-ds get-val-ds-coll proper-keychain? keyword-to-key add-child-keylink)
(declare get-prefix-matches pull-all get-kinded-emap get-proper-emap)

(defn equality-query
  [keychain pattern]
  (log/trace "equality-query" keychain pattern)
  ;; FIXME: assumption is that keychain is kinded, maybe with prefix?
  (if (> (count keychain) 1)
    (throw (RuntimeException. (str "ancestor eq filters not yet supported"))))
  (let [filter (into [] (for [[k v] pattern]
                         (do
                           (log/trace "\tpredicate: " k " = " v)
                           (Query$FilterPredicate.
                            (subs (str k) 1) Query$FilterOperator/EQUAL v))))
        k (str (subs (str (first keychain)) 1))
        log (log/trace "\tkind: " k)
        q (Query. k)
        fq (doseq [pred filter]
             (log/trace "\tpred: " pred)
             (.setFilter q pred))
        pq (.prepare ds q)
        iterator (.asIterator pq)
        seq (iterator-seq iterator)
        pemseq (migae.datastore.PersistentEntityMapSeq. seq)]
    (log/trace "\tfilter: " filter)
    (log/trace (str "\tresult seq: " (type seq) seq))
    (log/trace (str "\tpemseq: " pemseq))
    pemseq))

(defn get-ds
  ([keychain]
   (log/trace "get-ds " keychain)
   (cond
     (k/proper-keychain? keychain)
     (do
       (log/trace "proper keychain: " keychain)
       (let [r (get-proper-emap keychain)]
         r))

     (k/improper-keychain? keychain)
     (do
       (log/trace "improper-keychain: " keychain)
       (let [r (get-kinded-emap keychain)]
         (log/trace "get-kinded-emap res: " r)
         r))

     (empty? keychain)
     (do (log/trace "empty keychain, pulling all")
         (pull-all))
     :else
     (throw (IllegalArgumentException. (str "Invalid keychain" keychain)))))

  ([keychain pattern]
  (log/trace "get-ds " keychain pattern)
   (cond
     (= :prefix keychain)
     (if (k/proper-keychain? pattern)
       (get-prefix-matches pattern))

     ;; (empty? keychain)
     ;; (pull-all)

     (k/improper-keychain? keychain)
     (do
       (log/trace "improper-keychain with pattern" )
       (if (empty? pattern)
         (let [r (get-kinded-emap keychain)]
           (log/trace "get-kinded-emap res: " r)
           r)
         (do ;; we have a kinded keychain with a filter pattern
           ;; FIXME: validate query pattern
           (log/trace "pattern: " (first pattern))
           (if (empty? (meta pattern))
             (equality-query keychain pattern)
             (log/trace "inequality filter not yet implemented")))
           ))

     (k/proper-keychain? keychain)
     (if (empty? pattern)
       (do (log/trace "proper keychain with empty pattern")
           (get-proper-emap keychain))
       (log/trace "proper keychain with pattern"))

     :else
     (throw (IllegalArgumentException. (str "bad args to get-ds"))))))

(defn get-proper-emap
  [keychain & data]
  ;; precon: keychain has already been validated
  (log/trace "get-proper-emap" keychain)
  (let [k (k/entity-key keychain)
        e (.get ds k)]
    (log/trace "e: " e (type e))
    (->PersistentEntityMap e nil)))

(defn get-kinded-emap
  [keychain & data]
  ;; precon: improper keychain is validated
  (log/trace "get-kinded-emap " keychain data)
  ;; (log/trace "store-map " (.content store-map))
  (if (> (count keychain) 1)
    (do
      (log/trace "kinded descendant query")
      (let [ancestor-key (k/entity-key (vec (butlast keychain)))
            kind (name (last keychain))
            q (Query. kind ancestor-key)
            prepared-query (.prepare ds q)
            iterator (.asIterator prepared-query)
            seq (iterator-seq iterator)
            res (migae.datastore.PersistentEntityMapSeq. seq)]
        (log/trace "seq1: " (type seq))
        res))
    ;; kinded query
    (do
      (log/trace "kinded query, no ancestor: " keychain)
      (let [kind (name (last keychain))
            q (Query. kind)
            ;; log (log/trace "query: " q)
            pq (.prepare ds q) ;; FIXME
            ;; log (log/trace "p query: " pq)
            ;; log (log/trace "p query count: " (.countEntities pq (FetchOptions$Builder/withDefaults)))
            iterator (.asIterator pq)
            seq (iterator-seq iterator)
            res (migae.datastore.PersistentEntityMapSeq. seq)]
          (log/trace "get-kinded-emap seq: " seq (type seq))
          (log/trace "get-kinded-emap res: " (type res) (str res))
        (doseq [em res]
          (log/trace "get-kinded-emap map: " em (type em)))
        res))))

(defn pull-all
  []
  ;; (log/trace "pull-all")
  (let [q  (Query.)
        prepared-query (.prepare ds q) ;; FIXME
        iterator (.asIterator prepared-query)
        ;; res (migae.datastore.PersistentEntityMapSeq. (iterator-seq iterator))
        seq (iterator-seq iterator)
        res (migae.datastore.PersistentEntityMapSeq. seq)
        ;; res (iterator-seq iterator)
        ]
    ;; (log/trace "iter res: " (type res) " count:" (count res))
    res))

(defn fetch
  [^Key k]
  ;; FIXME: validate k
  (.get ds k))

(defn- make-embedded-entity
  [m]
  {:pre [(map? m)]}
  (let [embed (EmbeddedEntity.)]
    (doseq [[k v] m]
      ;; FIXME:  (if (map? v) then recur
      (.setProperty embed (subs (str k) 1) (get-val-ds v)))
    embed))

(defn- symbol-to-ds
  [sym]
   (KeyFactory/createKey "Symbol" (str sym)))

(defn- keyword-to-ds
  [kw]
   (KeyFactory/createKey "Keyword"
                         ;; remove leading ':'
                         (subs (str kw) 1)))

(defn- get-val-ds-coll
  "Type conversion: clojure to java.  The datastore supports a limited
  number of Java classes (see
  https://cloud.google.com/appengine/docs/java/datastore/entities#Java_Properties_and_value_types);
  e.g. no BigInteger, no HashMap, etc.  Before we can store a
  collection we have to convert its elements to acceptable types.  In
  particular, maps must be converted to EmbeddedEntity objects"
  ;; {:tag "[Ljava.lang.Object;"
  ;;  :added "1.0"
  ;;  :static true}
  [coll]
  ;; (log/trace "get-val-ds-coll" coll (type coll))
  (cond
    (list? coll) (let [a (ArrayList.)]
                     (doseq [item coll]
                       (do
                         ;; (log/trace "vector item:" item (type item))
                         (.add a (get-val-ds item))))
                     ;; (log/trace "ds converted:" coll " -> " a)
                     a)

    (map? coll) (make-embedded-entity coll)

    (set? coll) (let [s (HashSet.)]
                  (doseq [item coll]
                    (let [val (get-val-ds item)]
                      ;; (log/trace "set item:" item (type item))
                      (.add s (get-val-ds item))))
                  ;; (log/trace "ds converted:" coll " -> " s)
                  s)

    (vector? coll) (let [a (Vector.)]
                     (doseq [item coll]
                       (do
                         ;; (log/trace "vector item:" item (type item))
                         (.add a (get-val-ds item))))
                     ;; (log/trace "ds converted:" coll " -> " a)
                     a)

    :else (do
            (log/trace "HELP" coll)
            coll))
    )

(defn get-val-ds
  [v]
  ;; (log/trace "get-val-ds" v (type v))
  (let [val (cond (integer? v) v
                  (string? v) (str v)
                  (coll? v) (get-val-ds-coll v)
                  (= (type v) clojure.lang.Keyword) (keyword-to-ds v)
                  (= (type v) clojure.lang.Symbol) (symbol-to-ds v)
                  (= (type v) EmbeddedEntity) v
                  (= (type v) Link) v
                  (= (type v) Email) v
                  (= (type v) Key) v
                  (= (type v) java.lang.Double) v
                  (= (type v) java.lang.Long) v
                  (= (type v) java.lang.Boolean) v
                  (= (type v) java.util.Date) v
                  (= (type v) java.util.ArrayList) v ;; (into [] v)
                  :else (do
                          (log/trace "ELSE: get val type" v (type v))
                          v))]
    ;; (log/trace "get-val-ds result:" v " -> " val "\n")
    val))

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

(defn put-kinded-emap
  ([em]
   ;; assumption: input is validated entity-map
   ;; (log/trace "put-kinded-emap 1: " em (type em))
   (let [keychain (:migae/keychain (meta em))
         ;; log (log/trace "keychain" keychain)
         kind (name (last keychain))
         parent-keychain (vec (butlast keychain))
         e (if (empty? parent-keychain)
             (Entity. kind)
             (do #_(log/trace "parent-keychain" parent-keychain)
                 #_(log/trace "parent-key" (entity-key parent-keychain))
                 (Entity. kind  (k/entity-key parent-keychain))
                 ))]
     (when (not (empty? em))
       (doseq [[k v] (seq em)]
         (.setProperty e (subs (str k) 1) (get-val-ds v))))
     (.put ds e)
     (->PersistentEntityMap e nil)))
     ;; e))
  ([keyvec & data]
  ;; precon: improper keychain is validated
  ;; (log/trace "put-kinded-emap 2:" keyvec " | " data)
  (let [em (first data)
        kind (name (last keyvec))
        parent-keychain (vec (butlast keyvec))
        ;; foo (log/trace "foo" parent-keychain)
        e (if (empty? parent-keychain)
            (Entity. kind)
            (do #_(log/trace "parent-keychain" parent-keychain)
                #_(log/trace "parent-key" (entity-key parent-keychain))
                (Entity. kind  (k/entity-key parent-keychain))
                ))]
    (when (not (empty? em))
      (doseq [[k v] (seq em)]
        (.setProperty e (subs (str k) 1) (get-val-ds v))))
    (.put ds e)
    ;; (PersistentEntityMap. e nil))))
    e)))
    ;; (let [pem (PersistentEntityMap. e nil)
    ;;       ds (into store-map pem)]
    ;;   pem)))

(defn put-proper-emap
  [& {:keys [keyvec propmap force] :or {force false}}]
  ;; precon: keychain has already been validated
  (log/trace "put-proper-emap" keyvec propmap force)
  ;; (log/trace "keychain" keyvec)
  ;; (log/trace "propmap" propmap)
  ;; (log/trace "force" force)
  (let [k (k/entity-key keyvec)
        e (if (not force)
            (do (log/trace "not force: " k)
                (let [ent (try (.get ds k)
                               (catch EntityNotFoundException ex ex))]
                  (log/trace "ent: " (instance? EntityNotFoundException ent))
                  ;; (if (= (type ent) Entity) ;; found
                  ;;   (throw (RuntimeException. "Key already used"))
                  (if (instance? EntityNotFoundException ent)
                    (Entity. k)
                    (throw (DuplicateKeyException. (str keyvec))))))
            (do (log/trace "force push")
                (Entity. k)))]
    (when (not (empty? propmap))
      (doseq [[k v] propmap]
        (.setProperty e (subs (str k) 1) (get-val-ds v))))
    ;; (let [pem (PersistentEntityMap. e nil)
    ;;       ds ds]
    ;;   ;; (log/trace "ctor-push.put-proper-emap ds: " ds (type ds))
    ;;   (into store-map pem)
    ;;   pem)))
    (.put ds e)
    (->PersistentEntityMap e nil)))

(defn get-prefix-matches
  [keychain]
  ;; precon: keychain has already been validated
  (log/trace "get-prefix-matches" keychain)
  (let [prefix (apply k/entity-key keychain)
        q (.setAncestor (Query.) prefix)
        prepared-query (.prepare ds q)
        iterator (.asIterator prepared-query (FetchOptions$Builder/withDefaults))
        seq  (iterator-seq iterator)
        res (migae.datastore.PersistentEntityMapSeq. seq)]
    res))

;; (defn into-ds!
;;   ;; FIXME: convert to [keychain & em]???
;;   ([arg]
;;    (do
;;      (log/trace "into-ds! 1" arg)
;;      (cond
;;        (map? arg)
;;        (do
;;          ;; edn, e.g.  (entity-map! ^{:migae/keychain [:a/b]} {:a 1})
;;          (let [k (:migae/keychain (meta arg))]
;;            ;; (log/trace "edn key: " k " improper?" (improper-keychain? k))
;;            (cond
;;              (k/improper-keychain? k)
;;              (put-kinded-emap k arg)
;;              (k/proper-keychain? k)
;;              (put-proper-emap :keyvec k :propmap arg :force true)
;;              :else (throw (IllegalArgumentException. (str "INVALID KEYCHAIN!: " k)))))
;;          )
;;        (k/keychain? arg)
;;        (do
;;          (cond
;;            (k/improper-keychain? arg)
;;            (put-kinded-emap arg {})
;;            (k/proper-keychain? arg)
;;            (put-proper-emap :keyvec arg :propmap {} :force true)
;;            :else (throw (IllegalArgumentException. (str "Invalid keychain" arg)))))
;;        :else (throw (IllegalArgumentException.)))))
;;   ([keychain em]
;;   "Put entity-map to datastore unless already there; use :force true to replace existing"
;;   (do
;;     ;; (log/trace "into-ds! 2" keychain em)
;;     ;; (if (empty? keychain)
;;     ;;   (throw (IllegalArgumentException. "keychain vector must not be empty"))
;;     (cond
;;       (k/improper-keychain? keychain)
;;       (put-kinded-emap keychain em)
;;       (k/proper-keychain? keychain)
;;       (put-proper-emap :keyvec keychain :propmap em)
;;       :else
;;       (throw (IllegalArgumentException. (str "Invalid keychain : " keychain))))))
;;   ([mode keychain em]
;;   "Modally put entity-map to datastore.  Modes: :force, :multi"
;;   (do
;;     ;; (log/trace "force into-ds! 3" force keychain em)
;;     ;; (if (empty? keychain)
;;     ;;   (throw (IllegalArgumentException. "keychain vector must not be empty"))
;;     ;; (if (not= force :force)
;;     ;;   (throw (IllegalArgumentException. force)))
;;     (cond
;;       (= mode :force)
;;       (cond
;;         (k/improper-keychain? keychain)
;;         (put-kinded-emap keychain em)
;;         (k/proper-keychain? keychain)
;;         (put-proper-emap :keyvec keychain :propmap em :force true)
;;         :else
;;         (throw (IllegalArgumentException. (str "Invalid keychain" keychain))))
;;       (= mode :multi)
;;       (do
;;         ;; (log/trace "entity-map! :multi processing...")
;;         (if (k/improper-keychain? keychain)
;;           (if (vector? em)
;;             (do
;;               (for [emap em]
;;                 (do
;;                   ;; (log/trace "ctoring em" (print-str emap))
;;                   (entity-map! keychain emap))))
;;             (throw (IllegalArgumentException. ":multi ctor requires vector of maps")))
;;           (throw (IllegalArgumentException. ":multi ctor requires improper keychain"))))
;;       :else
;;       (throw (IllegalArgumentException. (str "Invalid mode keyword:" force))))
;;     )))

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
    (.put ds e)
    ;; (PersistentEntityMap. e nil)))
    e))

;; (defn- emap-old
;;   [^Key k ^Entity e content]
;;   {:pre [(map? content)]}
;;   ;; (log/trace "emap old " k content)
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
;;                   ;;(log/trace "added val to collection prop")
;;                   )
;;                 ;; if its not a collection, make a collection and add both vals
;;                 (let [newval (ArrayList.)]
;;                   (.add newval propval)
;;                   (.add newval v)
;;                   (.setProperty e (str prop) newval)
;;                   ;;(log/trace "created new collection prop")
;;                   ))
;;               ;;(log/trace "modified entity " e)
;;               (PersistentEntityMap. e nil))
;;             ;; new property
;;             (let [val (get-val-ds v)]
;;               ;; (log/trace "setting val" val (type val))
;;               ;; (flush)
;;               (.setProperty e prop val)))))
;;       (.put ds e)
;;       ;; (log/trace "saved entity " e)
;;       (PersistentEntityMap. e nil))))

;; (defn- emap-update-empty
;;   [keychain]
;;   (let [k (apply entity-key keychain)
;;         e (try (.get ds k)
;;                (catch EntityNotFoundException e nil)
;;                (catch DatastoreFailureException e (throw e))
;;                (catch java.lang.IllegalArgumentException e (throw e)))]
;;     (if (ds/entity-map? e)
;;           (PersistentEntityMap. e nil)
;;           (let [e (Entity. k)]
;;             (.put ds e)
;;             (PersistentEntityMap. e nil)))))

;; TODO: support embedded maps, e.g. (ds/emap!! [:Foo/bar] {:a 1, :b {:c 3, :d 4}})
;; technique: store them as embedded entities
(defn- emap-update-map
  [keychain content]
  ;; (log/trace "emap-update-map " keychain content)
  (let [k (apply entity-key keychain)]
    ;; (log/trace "emap-update-map key: " k)
    (let [e (if (keyword? k)
              (let [e (Entity. (subs (str k) 1))] ;; key of form :Foo, i.e. a Kind specifier
                (.put ds e)
                e)
              (try (.get ds k)
                   (catch EntityNotFoundException e nil)
                   (catch DatastoreFailureException e (throw e))
                   (catch java.lang.IllegalArgumentException e (throw e))))]
      ;; (if (nil? e)
      ;;   (emap-new k content)
      ;;   (emap-old k e content) ; even a new one hits this if id autogenned by entity-key
        )))

;;;; FIXME: migrate logic to api impl, not adapter
;; (defn- emap-update-fn
;;   "Second arg is a function to be applied to the Entity whose key is first arg"
;;   [keychain f]
;;   (if (nil? (namespace (first keychain)))
;;     ;; if first link in keychain has no namespace, it cannot be an ancestor node
;;     (let [txn (.beginTransaction ds) ;; else new entity
;;           e (Entity. (name (first keychain)))
;;           em (PersistentEntityMap. e nil)]
;;       (try
;;         (f em)
;;         (.put ds e)
;;         (.commit txn)
;;         (finally
;;           (if (.isActive txn)
;;             (.rollback txn))))
;;       em)
;;     (let [k (apply entity-key keychain)
;;           e (try (.get ds k)
;;                  (catch EntityNotFoundException e
;;                    ;;(log/trace (.getMessage e))
;;                    e)
;;                  (catch DatastoreFailureException e
;;                    ;;(log/trace (.getMessage e))
;;                    nil)
;;                  (catch java.lang.IllegalArgumentException e
;;                    ;;(log/trace (.getMessage e))
;;                    nil))]
;;       (if (ds/entity-map? e) ;; existing entity
;;         (let [txn (.beginTransaction ds)]
;;           (try
;;             (f e)
;;             (.commit txn)
;;             (finally
;;               (if (.isActive txn)
;;                 (.rollback txn))))
;;           (PersistentEntityMap. e nil))
;;         (let [txn (.beginTransaction ds) ;; else new entity
;;               e (Entity. k)
;;               em (PersistentEntityMap. e nil)]
;;           (try
;;             (f em)
;;             (.put ds e)
;;             (.commit txn)
;;             (finally
;;               (if (.isActive txn)
;;                 (.rollback txn))))
;;           em)))))

;; (defn emap!!
;;   "Replace, necessarily.  Create new, discarding old even if found (so
;;   don't bother searching)."
;;   [keylinks & propmap]
;;   (log/trace "emap!!" keylinks propmap)
;;   (if (empty? keylinks)
;;     (throw (IllegalArgumentException. "keychain vector must not be empty"))
;;     (if (keylink? (last keylinks))
;;       (if (every? keylink? (butlast keylinks))
;;         (emap-definite!! keylinks propmap)
;;         (throw-bad-keylinks (butlast keylinks)))
;;       (if (keykind? (last keylinks))
;;         (apply emap-indefinite!! keylinks propmap)
;;         (apply throw-bad-keykind (butlast keylinks))))))

;; FIXME: rename to into-ds!
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
;;   ;; (log/trace "args: " keychain content)
;;   (if (empty? keychain)
;;     (throw (IllegalArgumentException. "keychain vector must not be empty"))
;;     (if (empty? content)
;;       (emap-update-empty keychain)
;;       (if (map? (first content))
;;         (emap-update-map keychain (first content))
;;         (if (fn? (first content))
;;           (emap-update-fn keychain (first content))
;;           (throw (IllegalArgumentException. "content must be map or function")))))))
