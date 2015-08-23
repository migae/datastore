(ns migae.datastore
  (:refer-clojure :exclude [empty? get into key name reduce])
  (:import [java.lang IllegalArgumentException]
           [java.util
            Collection
            Collections
            ;; Collections$UnmodifiableMap
            ;; Collections$UnmodifiableMap$UnmodifiableEntrySet
            ;; Collections$UnmodifiableMap$UnmodifiableEntrySet$UnmodifiableEntry
            ArrayList
            HashMap HashSet
            Map Map$Entry
            Vector]
           [clojure.lang MapEntry]
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
           [com.google.appengine.api.blobstore BlobKey])
  (:require [clojure.core :as clj]
            [clojure.walk :as walk]
            [clojure.stacktrace :refer [print-stack-trace]]
            [clojure.tools.reader.edn :as edn]
            ;; [migae.datastore.service :as dss]
            ;; [migae.datastore.dsmap :as dsm]
            ;; [migae.datastore.emap :as emap]
            ;; [migae.datastore.entity :as dse]
            ;; [migae.datastore.key :as dskey]
            ;; [migae.datastore.query :as dsqry]
            [migae.infix :as infix]
            [clojure.tools.logging :as log :only [trace debug info]]))

(load "datastore/service")
(load "datastore/dsmap")
(load "datastore/ekey")
(load "datastore/emap")

(declare epr)

(defn ds-filter
  [pred]
  (emap?? pred))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; (defonce ^{:dynamic true} *datastore-service* (atom nil))

;; (defn datastore []
;;   (when (nil? @*datastore-service*)
;;     (do ;; (prn "datastore ****************")
;;         (reset! *datastore-service* (DatastoreServiceFactory/getDatastoreService))
;;         ))
;;   @*datastore-service*)

;; (defn init []
;;   (when (nil? @DSMap)
;;     (do
;;         (reset! DSMap (DatastoreMap. *datastore-service*))
;;         ))
;;   @DSMap)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; (def default-contents {:_kind :DSEntity
;;                        :_key nil})
;; whatever contents are provided at construction time will be
;; augmented with the default values
;; (defn augment-contents [contents]
;;   contents)
;  (merge default-contents contents))

;; (defprotocol IDSEntity
;;   (getKey [e]))

(declare get-next-emap-prop)

;; (defmethod clojure.core/print-method ::EntityMap
;;   [em writer]
;;   (.write writer (str (:number piece) (:letter piece)) 0 2))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;


(defn emap-seq? [arg]
  (and
   (seq? arg))
  (= (type arg) migae.datastore.EntityMapCollIterator))

(defn emap-seq [ds-iter]
  "Returns a seq on a com.google.appengine.api.datastore.QueryResultIterator"
  (let [em-iter (EntityMapCollIterator. ds-iter)
        clj-iter (clojure.lang.IteratorSeq/create em-iter)]
    (if (nil? clj-iter)
      nil
      (with-meta clj-iter {:type EntityMapCollIterator}))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defmulti kind class)
(defmethod kind Entity
  [^Entity e]
  (keyword (.getKind e)))
(defmethod kind EntityMap
  [^EntityMap e]
  (keyword (.getKind (.entity e))))

(defmulti name class)
(defmethod name Entity
  [^Entity e]
  (.getName (.getKey e)))
(defmethod name EntityMap
  [^EntityMap e]
  (.getName (.getKey (.entity e))))

(defmulti id class)
(defmethod id clojure.lang.PersistentVector
  [ks]
   (let [keylink (last ks)]
     (.getId keylink)))
(defmethod id Key
  [^Key k]
  (.getId k))
(defmethod id Entity
  [^Entity e]
  (.getId (.getKey e)))
(defmethod id EntityMap
  [^EntityMap e]
  (.getId (.getKey (.entity e))))

;;(declare keychain-to-key emap? keychain=)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;  emap stuff

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
      ;;   (log/trace "item entity" (.entity item)))
      ))

(defn alter!
  "Replace existing entity, or create a new one."
  [keychain content]
  ;; if second arg is a map, treat it ...
  ;; if second arg is a function, ...
  (if (clj/empty? keychain)
    (throw (IllegalArgumentException. "key vector must not be empty"))
    (let [k (apply keychain-to-key keychain)
          e (Entity. k)]
      (doseq [[k v] content]
        (.setProperty e (subs (str k) 1) v))
      (.put (datastore) e)
      ;; (log/trace "created and put entity " e)
      (EntityMap. e))))

;; (defn assoc!
;;   "unsafe assoc with save but no txn for DS Entities"
;;   [m k v & kvs]
;;   ;; (log/trace "assoc! " m k v  "&" kvs)
;;    (let [txn (.beginTransaction (datastore))
;;          coll (if (emap? m)
;;                 (.entity m)
;;                 (if (= (class m) Entity)
;;                   m
;;                   (do (log/trace "HELP: assoc!") (flush))))]
;;      (try
;;        (.setProperty coll (subs (str k) 1) v)
;;        (if (nil? (first kvs))
;;          (try
;;            (.put (datastore) coll)
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
  {:pre [(nil? (clj/namespace k))]}
   (let [txn (.beginTransaction (datastore))
         coll (if (emap? m)
                (.entity m)
                (if (= (class m) Entity)
                  m
                  (log/trace "HELP: assoc!!")))]
     (try
       (.setProperty coll (subs (str k) 1) v)
       (if (nil? (first kvs))
         (try
           (.put (datastore) coll)
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
         (EntityMap. coll)
         (log/trace "EXCEPTION assoc!!")))))

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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
      ;; (cond
      ;;   (= 1 (count keylinks))
      ;;   (let [kw (first keylinks)]
      ;;     (if (keyword? kw)
      ;;       (if (nil? (clj/namespace kw))
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
      ;; (if (nil? (clj/namespace keylinks))
      ;;   'Kind                           ; e.g.  :Foo
      ;;   'Key)                          ; e.g.  :Foo/Bar
      ;;   :else
      ;;   'Multikey)
      ;; :else
      ;; (throw (IllegalArgumentException. "arg must be key, keyword, or a vector of keywords")))))

(defmulti emaps??
  (fn [keylinks & pm]
    (log/trace "")
    (log/trace "emaps??" keylinks pm)
    (if (vector? keylinks)
      (cond
        (empty? keylinks)
        'Kindless
        (vector? keylinks)
        (if (keylink? (last keylinks))
          'Keychain
          'Kinded)
        (set? keylinks)
        'Multikey
        :else
        (throw (IllegalArgumentException. "first arg must be a vector or set of keywords"))))))

(defmethod emaps?? 'Keychain
  [keylinks & pm]             ; e.g. :Foo/Bar
  (log/trace "emaps?? Keychain:" keylinks pm)
  (let [dskey (keychain-to-key (first keylinks) (second keylinks))
        e (try (.get (datastore) dskey)
         (catch EntityNotFoundException e (throw e))
         (catch Exception e (throw e)))]
    (EntityMap. e)))

(defn- finish-q
  [q]
  (log/trace "finish-q")
  (let [pq (.prepare (datastore) q)
        it (.asIterator pq)
        em-seq (emap-seq it)]
    ;; (log/trace "em-seq " em-seq)
    em-seq))

(defn- do-filter
  [q filter]
  (log/trace "do-filter" filter)
  )

(defn- kinded-no-ancestor
  [q filter-map]
  (log/trace "kinded-no-ancestor" q filter-map)

  (if (nil? filter-map)
    (finish-q q)
    ;; FIXME:  support multiple filters using
    ;;  Query$CompositeFilterOperator.and and  Query$CompositeFilterOperator.or

    (let [filter (last filter-map)    ; constraint is the filter map
          foo (log/trace "kinded-no-ancestor filter:" filter)
          mapentry (first filter)
          fld (first mapentry)
          foo (log/trace "kinded-no-ancestor fld:" fld)
          constraint (last mapentry)
          foo (log/trace "kinded-no-ancestor constraint:" constraint)
          op (first constraint)
          foo (log/trace "kinded-no-ancestor op:" op)
          operand (last constraint)
          foo (log/trace "kinded-no-ancestor operand:" operand)
          ]
      (let [filter (Query$FilterPredicate. (subs (str fld) 1) ; remove ':' prefix
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
            q (.setFilter q filter)
            pq (.prepare (datastore) q)
            it (.asIterator pq)
            emseq (emap-seq it)]
        emseq))))

(defmethod emaps?? 'Kinded
  [keylinks & filter-map]
  ;; (log/trace "emaps?? Kinded:" keylinks filter-map)
  (let [kind (clj/name (last keylinks)) ;; we already know last link has form :Foo
        ;; foo (log/trace "kind:" kind (type kind))
        q (Query. kind)
        pfx (butlast keylinks)
        ;; foo (log/trace "pfx" pfx (type pfx))
        ]
    (if (nil? pfx)
      (kinded-no-ancestor q filter-map)
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
          pq (.prepare (datastore) q)
          it (.asIterator pq)
          emseq (emap-seq it)]
      emseq)
    (if (> (count filter-map) 1)
      (throw (IllegalArgumentException. "only one filter expr, for now"))
      (let [constraint (first filter-map)
            op (first constraint)
            keychain  (last constraint)]
        (log/trace "constraint:" constraint op keychain)
        (if (keylink? (last keychain))
          (let [;f (ffirst filter-map)
                k (apply keychain-to-key keychain)
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
                pq (.prepare (datastore) q)
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
  (let [q (Query. (clj/name kind))
        pq (.prepare (datastore) q)
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
  (let [q (Query. (clj/name kind))
        pq (.prepare (datastore) q)
        it (.asIterator pq)
        em-seq (emap-seq it)]
    em-seq))

(defmethod emaps?? 'Key
  [^clojure.lang.Keyword k]             ; e.g. :Foo/Bar
  (log/trace "emapss?? Key:")
  (let [dskey (key k)
        e (try (.get (datastore) dskey)
         (catch EntityNotFoundException e (throw e))
         (catch Exception e (throw e)))]
    (EntityMap. e)))

(defmethod emaps?? 'Multikey
  [key-set]
  (log/trace "emapss?? Multikey:")
  )

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
;;       (clj/into [kw]
;;              (get-keychain parent)))))

;; FIXME:  since we've implemented IPersistentMap etc. these are no longer needed:
(defmulti to-edn
  (fn [arg & args]
    [(type arg) (type args)]))

(defmethod to-edn [clojure.lang.Keyword nil]
  ([^clojure.lang.Keyword k])
  )

(defmethod to-edn [EntityMap nil]
  [^EntityMap e]
  e)

(defmethod to-edn [Entity nil]
  ;;[com.google.appengine.api.datastore.Entity nil]
  [^Entity e]
  (let [k (.getKey e)
        kch (to-keychain k)
        props (clj/into {} (.getProperties e))
        em (clj/into {} {:kind_ (.getKind k) :ident_ (identifier k)})]
        ;; em (clj/into {} {:key kch})]
    (clj/into em (for [[k v] props] [(keyword k) v]))))

;; taken verbatim from 1.7.0
(defn into
  "Returns a new coll consisting of to-coll with all of the items of
  from-coll conjoined. A transducer may be supplied."
  {:added "1.0"
   :static true}
  ([to from]
   (log/trace "ds/into")
     (if (instance? clojure.lang.IEditableCollection to)
       ;; (with-meta (persistent! (reduce conj! (transient to) from)) (meta to))
       (let [res (persistent! (clj/reduce conj! (transient to) from))]
         (log/trace "ds/into result w/o meta:" (epr res))
         (let [metameta (merge (meta to) (meta from))] ; the fix
           (log/trace "ds/into metameta:" (epr metameta))
           (let [result (with-meta res metameta)]
             (log/trace "ds/into result w/meta:" (epr result))
             result)))
       (clj/reduce conj to from)))
  ([to xform from]
     (if (instance? clojure.lang.IEditableCollection to)
       (with-meta (persistent! (transduce xform conj! (transient to) from)) (meta to))
       (transduce xform conj to from))))

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
;;        (reduce clj/conj to from))))
;;   ;; ([to xform from]
;;   ;;    (if (instance? clojure.lang.IEditableCollection to)
;;   ;;      (with-meta (persistent! (transduce xform conj! (transient to) from)) (meta to))
;;   ;;      (transduce xform clj/conj to from))))

(defn epr
  [^EntityMap em]
  (binding [*print-meta* true]
    (pr-str em)))

(defn eprn
  [^EntityMap em]
  (binding [*print-meta* true]
    (prn-str em)))
