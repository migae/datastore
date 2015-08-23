(ns migae.datastore
  (:refer-clojure :exclude [empty? filter get into key name reduce])
  (:import [java.lang IllegalArgumentException RuntimeException]
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

<<<<<<< HEAD
(load "datastore/service")
(load "datastore/dsmap")
(load "datastore/ekey")
(load "datastore/emap")

(declare epr)
=======
(declare emap?? emaps?? emap-seq epr keychainer emap? keychain=)
>>>>>>> 8a635036bed39e4333e2b5b3d62e69d5ddbde433

(defn filter                            ;defmacro?
  [keypred & valpred]
  (apply emaps?? keypred valpred))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
<<<<<<< HEAD
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
=======
(deftype DatastoreMap [ds]

  clojure.lang.Associative
  (containsKey [_ keychain]
    "returns boolean"
    (log/trace "DatastoreMap containsKey " keychain)
    (log/trace "DatastoreMap the ds " @ds)
    (let [k (apply keychainer keychain)
          e (try (.get @ds k)
                 (catch EntityNotFoundException ex
                   nil))]
      (log/trace "e" e)
      (if e true false)))
  (entryAt [this keychain]
    (log/trace "DatastoreMap entryAt " keychain)
    )

  clojure.lang.IFn
  (invoke [_ k]
    {:pre [(keyword? k)]}
    (log/trace "IFn invoke" k)
    (emap?? k))

  clojure.lang.ILookup
  (valAt [_ k]
    (log/trace "valAt " k)
    (emap?? k))
  (valAt [_ k not-found]
    (log/trace "valAt w/notfound: " k)
    )

  clojure.lang.Seqable
  (seq [this]
    ;; seq is called by: into, merge, "print", e.g. (log/trace em)
    ;; also called by filter
    (log/trace "DatastoreMap seq")
    (let [q (Query.)
          pq (.prepare @ds q)
          it (.asIterator pq)
          emseq (emap-seq it)]
      emseq)
    )
  ) ;; deftype DatastoreMap

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defonce ^{:dynamic true} *datastore-service* (atom nil))
(defonce ^{:dynamic true} DSMap (atom nil))

(defn datastore []
  (when (nil? @*datastore-service*)
    (do ;; (prn "datastore ****************")
        (reset! *datastore-service* (DatastoreServiceFactory/getDatastoreService))
        ))
  @*datastore-service*)

(defn init []
  (when (nil? @DSMap)
    (do
        (reset! DSMap (DatastoreMap. *datastore-service*))
        ))
  @DSMap)
>>>>>>> 8a635036bed39e4333e2b5b3d62e69d5ddbde433

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

<<<<<<< HEAD
=======
(deftype EntityMapIterator [ds-iter]
  java.util.Iterator
  (hasNext [this]
    (do
      (log/trace "emap-iter hasNext")
      (.hasNext ds-iter)))
  (next    [this]                       ;
    (let [r (get-next-emap-prop this)
          k (.getKey r)
          v (.getValue r)
          res {(keyword k) v}]
      (log/trace "emap-iter next" res)
      res))
;      {(keyword k) v}))

  ;; (remove  [this])
)


(declare get-val-ds get-val-clj keychain)

(defn- props-to-map
  [props]
  (clj/into {} (for [[k v] props]
                 (let [prop (keyword k)
                       val (get-val-clj v)]
                   {prop val}))))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;;  EntityMap - design goal is to have DS entities behave just like
;;  ordinary Clojure maps.  E.g. for ent.getProperty("foo") we want to
;;  write (ent :foo); instead of ent.setProperty("foo", val) we want
;;  either (assoc ent :foo val), (merge ent :foo val), dissoc, etc.
;;
;;  One strategy: use ordinary clj maps with key in metadata, then
;;  define funcs to convert to Entities at save time.  In this case
;;  the map is pure clojure, and "glue" functions talk to gae/ds.
;;  This would require something like dss/getEntity, dss/setEntity.  It
;;  would also require conversion of the entire Entity each time, all
;;  at once.  I.e. getting an entity would require gae/ds code to
;;  fetch the entity, then iterate over all its properties in order to
;;  create the corresponding map.  This seems both inefficient and
;;  error prone.  We might be interested in a single property of an
;;  entity that contains dozens of them - translating all of them
;;  would be a waste.
;;
;;  Strategy two: deftype a class with support for common map funcs so
;;  it will behave more or less like a map.  In this case the data
;;  struct itself wraps gae/ds functionality.  Access to actual data
;;  would be on-demand (JIT) - we don't convert until we have an
;;  actual demand.
;;
;;  SEE http://david-mcneil.com/post/16535755677/clojure-custom-map
;;
;;  deftype "dynamically generates compiled bytecode for a named class
;;  with a set of given fields, and, optionally, methods for one or
;;  more protocols and/or interfaces. They are suitable for dynamic
;;  and interactive development, need not be AOT compiled, and can be
;;  re-evaluated in the course of a single session.  So we use deftype
;;  with a single data field (holding a map) and the protocols needed
;;  to support a map-like interface.
;;
;;  EntityMap: implements protocols/interfaces to make it behave like
;;  a clojure map:
;;   clojure.lang.IPersistentCollection
;;   clojure.lang.IPersistentMap
;;   java.lang.Iterable
;;   clojure.lang.Associative
;;   clojure.lang.Seqable
;;   clojure.lang.ILookup
;;
;;  The problem is that there doesn't seem to be a way to support
;;  metadata, which we need for the key.  Also the doc warns sternly
;;  against mutable fields.  But do we really need metadata?  Can't we
;;  just designate a privileged :key field?  The only drawback is that
;;  this would become unavailable for use by clients - but so what?
;;  Call it :_key?
;;
;;  CORRECTION: we don't need any metadata.  Just store the Entity in
;;  a field ("entity"!).
;;
;;  NB: defrecord won't work - no way to override clojure interfaces
;;  NB: gen-class won't work - Entity is final
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(deftype EntityMap [entity]
  java.lang.Iterable
  (iterator [this]
    (log/trace "Iterable iterator" (.entity this))
    (let [props (.getProperties entity) ;; java.util.Map<java.lang.String,java.lang.Object>
          entry-set (.entrySet props)
          e-iter (.iterator entry-set)
          em-iter (EntityMapIterator. e-iter) ]
    ;; (log/trace "Iterable res:" em-iter)
    em-iter))

  ;; FIXME: put :^EntityMap in every EntityMap
  clojure.lang.IMeta
  (meta [this]
    ;; (log/trace "IMeta meta")
    {:migae/key (keychain this)
     :type EntityMap})

  clojure.lang.IObj
  (withMeta [this md]
    (log/trace "IObj withMeta" md))
 ;; (EntityMap. (with-meta m md)))

  clojure.lang.IFn
  (invoke [_ k]
    {:pre [(keyword? k)]}
    (log/trace "IFn invoke")
    (let [prop (.getProperty entity (clj/name k))]
      (get-val-clj prop)))
  ;; (applyTo [_ arglist])
  ;; (invokePrim [_ ??] )

  java.util.Map$Entry
  (getKey [this]
    (log/trace "java.util.Map$Entry getKey" (epr this))
    (let [k (.getKey entity)]
      (keychain k)))
      ;;     kind (.getKind k)
      ;;     nm (.getName k)]
      ;; [(keyword kind (if nm nm (.getId k)))]))
  (getValue [this]
    (log/trace "java.util.Map$Entry getValue" (epr this))
    (props-to-map (.getProperties entity)))
  ;; (equals [_]
  ;;   )
  ;; (hashCode [_]
  ;;   )
  ;; (setValue [_]
  ;;   )

  ;; FIXME: make result of (into {} em) support Map$Entry so it behaves like an em
  ;; this doesn't work since clojure.lang.PersistentArrayMap cannot be cast to java.util.Map$Entry
  ;; NB:  IMapEntry extends java.util.Map$Entry
  ;; can we use defprotocol and extend to do this?

  clojure.lang.IMapEntry
  (key [this]
    (log/trace "IMapEntry key")
    )
  (val [_]
    (log/trace "IMapEntry val")
    )

  ;; clojure.lang.Counted
  ;; (count [_]
  ;;   (.size (.getProperties entity)))


  clojure.lang.IPersistentCollection
  (count [_]
    (log/trace "count")
    (.size (.getProperties entity)))
  (cons [this o] ;; o should be a MapEntry?
    (log/trace "cons this:" (epr this))
    ;; (log/trace "cons arg:" o (type o))
    (cond
      (nil? o)
      this
      (= (type o) clojure.lang.MapEntry)
      (do
        ;; (log/trace "cons clj map entry to emap")
        (.setProperty entity (subs (str (first o)) 1) (get-val-ds (second o)))
        ;; (.put (datastore) entity)
        this)
      (= (type o) clojure.lang.PersistentArrayMap)
      (do
        (doseq [[k v] o]
          (.setProperty entity (subs (str k) 1) (get-val-ds v)))
        this)
      ;; (do
      ;;   ;; (log/trace "cons clj map to emap")
      ;;   (doseq [[k v] o]
      ;;     (let [nm (subs (str k) 1)
      ;;           val (get-val-ds v)]
      ;;       ;; (log/trace "cons key: " nm (type nm))
      ;;       ;; (log/trace "cons val: " val (type val))
      ;;       (.setProperty entity nm val)))
      ;;   ;; (.put (datastore) entity)
      ;;   this)
      (= (type o) EntityMap)
      (let [props (.getProperties (.entity o))]
        ;; (log/trace "cons emap to emap")
        (doseq [[k v] props]
          (.setProperty entity k v)) ;(get-val-ds v)))
        ;; (.put (datastore) entity)
        ;; clone and return a new EM?
        this)
      ;; (= (type o) java.util.Collections$UnmodifiableMap$UnmodifiableEntrySet$UnmodifiableEntry)
      (= (type o) java.util.Map$Entry)
      (do
        ;; (log/trace "cons entity prop to emap")
        (.setProperty entity (.getKey o) (.getValue o))
        ;; (.put (datastore) entity)
        this)
      :else (log/trace "cons HELP?" o (type o))))
      ;; (do
      ;;   (doseq [item o]
      ;;     (do (log/trace "item " item)
      ;;         (doseq [[k v] item]
      ;;           (do (log/trace "k/v: " k v)
      ;;               (.setProperty entity (clj/name k) v)))))
      ;;   (.put (datastore) entity)
      ;;   this)))
  ;; (empty [_]
  ;;   (= (.size (.getProperties entity)) 0))
  (equiv [this o]
    (.equals this o))
    ;; (.equals entity (.entity o)))

    ;; (and (isa? (class o) EntityMap)
    ;;      (.equiv (augment-contents entity) (.(augment-contents entity) o))))

  clojure.lang.IReduce
  (reduce [this f]
    (log/trace "HELP! reduce") (flush)
    this)
  (reduce [this f to-map]
    (log/trace "reduce f to-map " to-map " from-coll " (.entity this))
    (cond
      (= (class to-map) EntityMap)
      ;; f = cons, so we can just use the native clj/into
      (let [from-props (.getProperties entity)
            from-coll (clj/into {} (for [[k v] from-props]
                                     (let [prop (keyword k)
                                           val (get-val-clj v)]
                                       {prop val})))
            to-props (.getProperties (.entity to-map))
            to-coll (clj/into {} (for [[k v] to-props]
                                   (let [prop (keyword k)
                                         val (get-val-clj v)]
                                     {prop val})))
            to-keychain (keychain to-map)
            res (with-meta (clj/into to-coll from-coll)
                  {:migae/keychain to-keychain
                   :type EntityMap})]
        (log/trace "to-coll: " res (type res))
        res)
      (= (class to-map) clojure.lang.PersistentArrayMap$TransientArrayMap)
      ;; we use a ghastly hack in order to retain metadata
      ;; FIXME: handle case where to-map is a clj-emap (map with ^:EntityMap metadata)
      (let [from-props (.getProperties entity)
            from-coll (clj/into {} (for [[k v] from-props]
                                     (let [prop (keyword k)
                                           val (get-val-clj v)]
                                       {prop val})))
            to-ent (Entity. (.getKey entity))
            ;; to-coll (clj/into {} (for [[k v] to-props]
            ;;                        (let [prop (keyword k)
            ;;                              val (get-val-clj v)]
            ;;                          {prop val})))
            to-keychain (if (nil? (:migae/keychain (meta to-map)))
                          (keychain entity)
                          (:migae/keychain (meta to-map)))]
        (doseq [[k v] from-coll]
          (assoc! to-map k v))
        (let [p (persistent! to-map)]
          (doseq [[k v] p]
            (.setProperty to-ent (subs (str k) 1) (get-val-ds v))))
        ;; (let [m1 (persistent! to-map)
        ;;       m2 (with-meta m1 {:migae/keychain to-keychain
        ;;                         :type EntityMap})
        ;;       to-coll (transient m2)]
        ;;   (log/trace "m2: " (meta m2) m2 (class m2))
        ;;   (log/trace "to-coll: " (meta to-coll) to-coll (class to-coll))
        (EntityMap. to-ent))

      (= (class to-map) clojure.lang.PersistentArrayMap)
      to-map
      :else (log/trace "HELP! reduce!" (class to-map)))
      )


  clojure.lang.ITransientCollection
  (conj [this args]
    (log/trace "ITransientCollection conj")
    (let [item (first args)
          k (clj/name (clj/key item))
          v (clj/val item)
          val (if (number? v) v
                  (if (string? v) v
                      (edn/read-string v)))]
      ;; (log/trace "ITransientCollection conj: " args item k v)
      (.setProperty entity k v)
      this))
  (persistent [this]
    (log/trace "ITransientCollection persistent")
    ;; (try (/ 1 0) (catch Exception ex (print-stack-trace e)))
    (let [props (.getProperties entity)
          kch (keychain this)
          coll (clj/into {} (for [[k v] props]
                              (let [prop (keyword k)
                                    val (get-val-clj v)]
                                {prop val})))
          res (with-meta coll {:migae/keychain kch
                               :type EntityMap})]
      (log/trace "persistent result: " (meta res) res (class res))
      res))


  ;; clojure.lang.ITransientMap
  ;; (assoc [this k v]                     ; both assoc! and assoc (?)
  ;;   (let [prop (clj/name k)]
  ;;     (log/trace "ITransientMap assoc: setting prop " k "->" prop v)
  ;;     (.setProperty entity prop v)
  ;;     this))
  ;; (without [this k]                     ; = dissoc!, return new datum with k removed
  ;;   (let [prop (clj/name k)]
  ;;     (log/trace "without: removing prop " k "->" prop)
  ;;     (.removeProperty entity prop)
  ;;     this))
  ;; (persistent [this]                    ; persistent!
  ;;     (log/trace "ITransientMap persistent")
  ;;   )

  clojure.lang.IPersistentMap
  (assoc [this k v]
    (let [prop (subs (str k) 1)]
      (log/trace "IPersistentMap assoc: " k v "(" prop v ")")
      ;; (.setProperty entity prop v)
      ;; this))
      (let [to-props (.getProperties entity)
            to-coll (clj/into {} (for [[k v] to-props]
                                     (let [prop (keyword k)
                                           val (get-val-clj v)]
                                       {prop val})))
            key-chain (keychain this)
            res (clj/assoc to-coll k v)]
      (log/trace "IPersistentMap assoc res: " res)
      (with-meta res {:migae/keychain key-chain}))))
  (assocEx [_ k v]
    (log/trace "assocEx")
    (EntityMap. (.assocEx entity k v)))
  (without [this k]                     ; = dissoc!, return new datum with k removed
    (let [prop (clj/name k)]
      (log/trace "without: removing prop " k "->" prop)
      (.removeProperty entity prop)
      this))

  clojure.lang.Associative
  (containsKey [_ k]
    (log/trace "containsKey " k)
    (let [prop (clj/name k)
          r    (.hasProperty entity prop)]
      r))
  (entryAt [this k]
    (let [val (.getProperty entity (clj/name k))
          entry (MapEntry. k val)]
      (log/trace "entryAt " k val entry)
      entry))

  ;; clojure.lang.IObj
  ;; (withMeta [_ m])
  ;; (meta [_])

  clojure.lang.Seqable
  (seq [this]
    ;; seq is called by: into, merge, "print", e.g. (log/trace em)
    ;; (log/trace "seq" (.hashCode this) (type entity))
    (let [props (.getProperties entity)
          ;; foo (doseq [[k v] props] (log/trace "entry:" k " -> " v))
          kprops (clj/into {}
                           (for [[k v] props]
                             (do
                             ;; (log/trace "v: " v)
                             (let [prop (keyword k)
                                   val (get-val-clj v)]
                               ;; (log/trace "prop " prop " val " val)
                               {prop val}))))
          res (clj/seq kprops)]
      ;; (log/trace "seq result:" entity " -> " res)
      (flush)
      res))

  clojure.lang.Indexed                  ; extends Counted
  ;; (count [this]                         ; Counted
  ;;   (log/trace "count"))
  (nth [this i]                         ; called by get(int index)
    (log/trace "nth"))
;;    (next em-iter)) ;; HACK
  (nth [this i not-found]
    )

  clojure.lang.ILookup
  (valAt [_ k]
    (let [prop (clj/name k)
          v  (.getProperty entity prop)
          val (get-val-clj v)]
      ;; (log/trace "valAt " k ": " prop val)
      val))
  (valAt [_ k not-found]
    (log/trace "valAt w/notfound: " k)
    (.getProperty entity (str k) not-found)))

>>>>>>> 8a635036bed39e4333e2b5b3d62e69d5ddbde433
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

<<<<<<< HEAD
;;(declare keychain-to-key emap? keychain=)
=======
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- keyword-to-ds
  [kw]
   (KeyFactory/createKey "Keyword"
                         ;; remove leading ':'
                         (subs (clj/str kw) 1)))

(defn- symbol-to-ds
  [sym]
   (KeyFactory/createKey "Symbol" (clj/str sym)))

(defn- make-embedded-entity
  [m]
  {:pre [(map? m)]}
  (let [embed (EmbeddedEntity.)]
    (doseq [[k v] m]
      ;; FIXME:  (if (map? v) then recur
      (.setProperty embed (subs (str k) 1) (get-val-ds v)))
    embed))

(defn- get-val-clj-coll
  "Type conversion: java to clojure"
  [coll]
  ;; (log/trace "get-val-clj-coll" coll (type coll))
  (cond
    (= (type coll) java.util.ArrayList) (clj/into '() (for [item coll]
                                                       (get-val-clj item)))
    (= (type coll) java.util.HashSet)  (clj/into #{} (for [item coll]
                                                       (get-val-clj item)))
    (= (type coll) java.util.Vector)  (clj/into [] (for [item coll]
                                                       (get-val-clj item)))
    :else (log/trace "EXCEPTION: unhandled coll " coll)
    ))

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

    (set? coll) (let [s (java.util.HashSet.)]
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

;; this is for values to be printed (i.e. from ds to clojure)
(defn- get-val-clj
  [v]
  ;; (log/trace "get-val-clj" v (type v) (class v))
  (let [val (cond (integer? v) v
                  (string? v) (str v)
                  (= (class v) java.lang.Double) (.toString v)
                  (= (class v) java.lang.Boolean) v
                  (= (class v) java.util.Date) v
                  (instance? java.util.Collection v) (get-val-clj-coll v)
                  (= (type v) Link) (.toString v)
                  (= (type v) Email) (.getEmail v)
                  (= (type v) EmbeddedEntity) ;; (let [e (Entity. (.getKey v))]
                                              ;;  (.setPropertiesFrom e v)
                                                (EntityMap. v) ;; )
                  (= (type v) Key) (let [kind (.getKind v)]
                                     (if (= kind "Keyword")
                                       (keyword (.getName v))
                                       ;; (symbol (.getName v))))
                                       (str \' (.getName v))))
                  :else (do
                          (log/trace "HELP: get-val-clj else " v (type v))
                          (edn/read-string v)))]
    ;; (log/trace "get-val-clj result:" v val)
    val))

;; this is for values to be stored (i.e. from clojure to ds java types)
(defn- get-val-ds
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
                  (= (type v) java.util.ArrayList) v ;; (clj/into [] v)
                  :else (do
                          (log/trace "ELSE: get val type" v (type v))
                          v))]
    ;; (log/trace "get-val-ds result:" v " -> " val "\n")
    val))

(defn key? [^com.google.appengine.api.datastore.Key k]
  (= (type k) com.google.appengine.api.datastore.Key))

(defmulti key class)
(defmethod key Key
  [^Key k]
  k)
(defmethod key migae.datastore.EntityMap
  [^EntityMap e]
  (.getKey (.entity e)))
(defmethod key com.google.appengine.api.datastore.Entity
  [^Entity e]
  (.getKey e))
(defmethod key clojure.lang.Keyword
  [^clojure.lang.Keyword k]
  (keychainer k))
(defmethod key clojure.lang.PersistentVector
  [kchain]
  (keychainer kchain))

(defn key=
  [em1 em2]
  (if (emap? em1)
    (if (emap? em2)
      (.equals (.entity em1) (.entity em2))
      (keychain= em1 em2))
    (if (map? em1)
      (keychain= em1 em2)
      (log/trace "EXCEPTION: key= applies only to maps and emaps"))))

(defn keylink?
  [k]
  ;; (log/trace "keylink?" k (and (keyword k)
  ;;                              (not (nil? (clj/namespace k)))))
  (and (keyword? k) (not (nil? (clj/namespace k)))))

(defn keykind?
  [k]
  ;; (log/trace "keykind?" k (and (keyword k)
  ;;                              (not (nil? (clj/namespace k)))))
  (and (keyword? k) (nil? (clj/namespace k))))

(defn keychain? [k]
  ;; k is vector of DS Keys and clojure keywords
  (every? #(or (keyword? %) (= (type %) Key)) k))

(defn keychain=
  [k1 k2]
  (let [kch1 (if (emap? k1)
               ;; recur with .getParent
               (if (map? k1)
                 (:migae/key (meta k1))))
        kch2 (if (emap? k2)
               ;; recur with .getParent
               (if (map? k2)
                 (:migae/key (meta k2))))]
    ))

(defmulti keychain
  "keychain converts a DS Key to a vector of Clojure keywords"
   class)

(defmethod keychain nil
  [x]
  nil)

(defmethod keychain Key
  [^Key k]
  ;; (log/trace "keychain Key: " k)
  (if (nil? k)
    nil
    (let [kind (.getKind k)
          nm (.getName k)
          id (str (.getId k))
          this (keyword kind (if nm nm id))
          res (if (.getParent k)
                  (conj (list this) (keychain (.getParent k)))
                  (list this))]
      ;; (log/trace "kind" kind "nm " nm " id " id " parent " (.getParent k))
      ;; (log/trace "res: " res)
      ;; (log/trace "res2: " (vec (flatten res)))
      (vec (flatten res)))))

(defmethod keychain Entity
  [^Entity e]
  ;; (log/trace "keychain Entity: " e)
  (keychain (.getKey e)))

(defmethod keychain EntityMap
  [^EntityMap em]
  ;; (log/trace "keychain EntityMap: " em)
  (keychain (.getKey (.entity em))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defmulti identifier class)

(defmethod identifier Key
  [^Key k]
  ;; (log/trace "Key identifier" k)
  (let [nm (.getName k)
        id (.getId k)]
    (if (nil? nm) id nm)))

(defmethod identifier EntityMap
  [^EntityMap em]
  ;; (log/trace "EM identifier" (.entity em))
  (let [k (.getKey (.entity em))
        nm (.getName k)
        id (.getId k)]
    (if (nil? nm) id nm)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn add-child-keylink
  [^KeyFactory$Builder builder chain]
  ;; (log/trace "add-child-keylink builder:" builder)
  ;; (log/trace "add-child-keylink chain:" chain)
  (doseq [sym chain]
    (if (nil? sym)
      nil
      (let [k (keychainer sym)]
        ;; (log/trace "Keychainer: " sym " -> " k)
        (if (keyword? k) ;; sym is indefinite kw e.g. :Foo
          (let [parent (.getKey builder)
                e (Entity. (clj/name k) parent) ; k of form :Foo
                v (.put (datastore) e)
                k (.getKey e)]
            ;; (log/trace "created entity " e)
            (.addChild builder (.getKind k) (.getId k)))
          (.addChild builder
                     (.getKind k)
                     (identifier k)))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defmulti keychainer
  "Make a datastore Key from a Clojure keyword or vector of keywords.
  For numeric IDs with keywords use e.g. :Foo/d123 (decimal)
  or :Foo/x0F (hex).  NOTE: if passed an indefinite keychain (ending
  in a kind kw, e.g. :Foo), keychainer will create and put a new
  entity in order to get a complete Key.  SUBJECT TO REVISION"
  (fn [arg & args]
    ;; (log/trace "keychainer" arg args)
    [(type arg) (type args)]))
;    (type arg)))

;; (defmethod key [java.lang.String nil]
;;   ([^java.lang.String kind]
;;    (KeyFactory/createKey kind)))

;; (defmethod key [java.lang.String java.lang.String]
;;   ([^java.lang.String kind ^java.lang.String nm]
;;    )

;; (defmethod key [java.lang.String java.lang.Long]
;;   ([^java.lang.String kind ^java.lang.Long id]
;;    )

;; TODO: vector of keywords
;; (defmethod key [clojure.lang.Keyword clojure.lang.PersistenVector]
;;    )

(defmethod keychainer [Key nil]
  [k] k)

(defmethod keychainer [nil nil]
  [a b]
  nil)
;; (defmethod keychainer [clojure.lang.PersistentVector nil]
;;   ([^clojure.lang.Keyword k]
;;    (

;;    ))

(defmethod keychainer [clojure.lang.Keyword nil]
  ([^clojure.lang.Keyword k]
   {:pre [(= (type k) clojure.lang.Keyword)]}
   ;; (log/trace "keychainer [Keyword nil]" k) (flush)
   (let [kind (clojure.core/namespace k)
         ident (edn/read-string (clj/name k))]
     ;; (log/trace (format "keychainer kw nil: kind=%s, ident=%s" kind ident))
     (cond
       (nil? kind)
       k ;; Keyword arg is of form :Foo, interpreted as Kind
       ;; (let [e (Entity. (str ident))
       ;;       k (.put (datastore) e)]
       ;;   (log/trace "created entity with key " k)
       ;;   k)
       (integer? ident)
       (KeyFactory/createKey kind ident)
       (symbol? ident)                  ;; edn reader makes symbols
       (let [s (str ident)]
         (cond
           (= (first s) \d)
           (let [id (edn/read-string (apply str (rest s)))]
             (if (= (type id) java.lang.Long)
               (KeyFactory/createKey kind id)
               (KeyFactory/createKey kind s)))
           (= (first s) \x)
           (let [id (edn/read-string (str "0" s))]
             (if (= (type id) java.lang.Long)
               (KeyFactory/createKey kind id)
               (KeyFactory/createKey kind s)))
           :else
           (KeyFactory/createKey kind s)))))))

(defmethod keychainer [clojure.lang.Keyword clojure.lang.PersistentVector$ChunkedSeq]
  ;; vector of keywords, string pairs, or both
  ([head & chain]
   (log/trace "keychainer Keyword ChunkedSeq" head chain)
   (flush)
   ;; (let [root (KeyFactory$Builder. (clj/namespace head)
   ;;                                 ;; FIXME: check for IDs too, e.g. :Foo/d99, :Foo/x0F
   ;;                                 (clj/name head))]
   (let [k (keychainer head)
         root (KeyFactory$Builder. k)]
     (.getKey (doto root (add-child-keylink chain))))))

(defmethod keychainer [clojure.lang.Keyword clojure.lang.ArraySeq]
  ;; vector of keywords, string pairs, or both
  ([head & chain]
   ;; (log/trace "kch Keyword ArraySeq" head chain)
   ;; (let [root (KeyFactory$Builder. (clj/namespace head)
   ;;                                 ;; FIXME: check for IDs too, e.g. :Foo/d99, :Foo/x0F
   ;;                                 (clj/name head))]
   (let [k (keychainer head)
         root (KeyFactory$Builder. k)]
     (.getKey (doto root (add-child-keylink chain))))))


   ;; (if (empty? (first (seq chain)))
   ;;   head
   ;;   (key (first chain) (rest chain)))))

(defmethod keychainer [clojure.lang.MapEntry nil]
  [^clojure.lang.MapEntry k]
  (log/trace "keychainer MapEntry nil" k) (flush)
  (let [kind (clojure.core/namespace k)
        ident (edn/read-string (clojure.core/name k))]
    ;; (log/trace (format "keychainer 1: kind=%s, ident=%s" kind ident))
    (cond
      (nil? kind)
      k ;; Keyword arg is of form :Foo, interpreted as Kind
      ;; (let [e (Entity. (str ident))
      ;;       k (.put (datastore) e)]
      ;;   (log/trace "created entity with key " k)
      ;;   k)
      (integer? ident)
      (KeyFactory/createKey kind ident)
      (symbol? ident)                  ;; edn reader makes symbols
      (let [s (str ident)]
        (cond
          (= (first s) \d)
          (let [id (edn/read-string (apply str (rest s)))]
            (if (= (type id) java.lang.Long)
              (KeyFactory/createKey kind id)
              (KeyFactory/createKey kind s)))
          (= (first s) \x)
          (let [id (edn/read-string (str "0" s))]
            (if (= (type id) java.lang.Long)
              (KeyFactory/createKey kind id)
              (KeyFactory/createKey kind s)))
          :else
          (KeyFactory/createKey kind s))))))

(defmethod keychainer [com.google.appengine.api.datastore.Key clojure.lang.ArraySeq]
  ;; vector of keywords, string pairs, or both
  ([head & chain]
   ;; (log/trace "keychainer Key ArraySeq" head chain)
   (let [root (KeyFactory$Builder. head)
         k (if (> (count chain) 1)
             (.getKey (doto root (add-child-keylink chain)))
             (.getKey (doto root (add-child-keylink chain))))]
             ;; (add-child-keylink root chain))]
     ;; (log/trace "keychainer Key ArraySeq result: " k)
     k)))
  ;; (let [k (first chain)]
       ;;   (if

(defmethod keychainer [java.lang.String clojure.lang.ArraySeq]
  ;; vector of keywords, string pairs, or both
  ([head & chain]
   (log/trace "str str")))

(defmethod keychainer [clojure.lang.ArraySeq nil]
  ;; vector of keywords, string pairs, or both
  ([head & chain]
   (log/trace "seq nil" head chain)))

(defmethod keychainer [clojure.lang.PersistentVector$ChunkedSeq nil]
  ;; vector of keywords, string pairs, or both
  ([head & chain]
   (log/trace "keychainer ChunkedSeq nil:" head chain)
   (keychainer (first head) (rest head))))

(defmethod keychainer [clojure.lang.PersistentList$EmptyList clojure.lang.ArraySeq]
  ([head & chain]
   (log/trace "emptylist arrayseq: " head chain)
   ))


   ;; (let [kind (edn/read-string fst)
   ;;       name (edn/read-string )]
   ;;   (KeyFactory/createKey (str kind) ident))))

  ;; ([^String k ^Long ident]
  ;;  (let [kind (edn/read-string k)
  ;;        name (edn/read-string ident)]
  ;;    (KeyFactory/createKey (str kind) ident))))

     ;; (if (= (type name) java.lang.Long)
     ;;       (KeyFactory/createKey ns n)))
     ;;   (KeyFactory/createKey ns n)))))

    ;; (log/trace "ns " ns " n: " n ", first n: " (first n))
>>>>>>> 8a635036bed39e4333e2b5b3d62e69d5ddbde433

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;  emap stuff

<<<<<<< HEAD
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
=======
(defn emap?
  [em]
  (log/trace "emap?" em)
  (= (type em) migae.datastore.EntityMap))

(defn entity?
  [e]
  (= (type e) Entity))

(defn empty?
  [em]
  (= (count em) 0))

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
  (let [k (apply keychainer keylinks)
        e  (Entity. k)
        propmap (first props)]
    (if (not (nil? propmap))
      (let [props (first propmap)]
        (doseq [[k v] props]
          (.setProperty e (subs (str k) 1) (get-val-ds v)))))
    (.put (datastore) e)
    (EntityMap. e)))

(defn- emap-indefinite!!
  [keylinks & props]
  ;; (log/trace "emap-indefinite!!" keylinks props)
  (let [e (if (> (count keylinks) 1)
            (let [parent (apply keychainer (butlast keylinks))]
              (Entity. (clj/name (last keylinks)) parent))
            (Entity. (clj/name (first keylinks))))
        propmap (first props)]
    (if (not (nil? propmap))
      (let [propmap (first props)]
        ;; (log/trace "props" propmap)
        (doseq [[k v] propmap]
          (.setProperty e (subs (str k) 1) (get-val-ds v)))))
    (.put (datastore) e)
    (EntityMap. e)))


(defn- find-definite-necessarily
  [keylinks & propmap]
  (log/trace "find-definite-necessarily" keylinks propmap)
  (if (every? keylink? keylinks)
    (let [akey (apply keychainer keylinks)
          e (try (.get (datastore) akey)
                 (catch EntityNotFoundException ex nil))]
      (if (nil? e)
        (let [newe  (Entity. akey)]
          (if (not (nil? propmap))
            (let [props (first propmap)]
              (doseq [[k v] props]
                (let [p (subs (str k) 1)
                      val (get-val-ds v)]
                  ;; (log/trace "PROP" p val)
                  (.setProperty newe p val)))))
          (.put (datastore) newe)
          (EntityMap. newe))
        (let [em (EntityMap. e)]
          (log/trace "already found" (epr em))
          em)))
    (throw-bad-keylinks keylinks)))

(defn- find-indefinite-necessarily
  [keylinks & propmap]
  {:pre [(every? keylink? (butlast keylinks))
         (nil? (namespace (last keylinks)))]}
  (log/trace "find-indefinite-necessarily" keylinks propmap)

  ;; if propmap not nil, construct property-filter query

  (let [e (if (nil? (butlast keylinks))
            (Entity. (clj/name (last keylinks)))
            (let [parent (apply keychainer (butlast keylinks))]
              ;; (log/trace "parent" parent)
              (Entity. (clj/name (last keylinks)) parent)))]
        (if (not (nil? propmap))
          (let [props (first propmap)]
            ;; (log/trace "props" props)
            (doseq [[k v] props]
              (let [p (subs (str k) 1)
                    val (get-val-ds v)]
                ;; (log/trace "PROP" p val)
                (.setProperty e p val)))))
        (.put (datastore) e)
        (EntityMap. e)))
;; (throw-bad-keylinks keylinks)))


;; (defn- emap-new
;;   [^Key k content]
;;   {:pre [(map? content)]}
;;   ;; (log/trace "emap-new " k content)
;;   (let [e (Entity. k)]
;;     (doseq [[k v] content]
;;       (let [prop (subs (str k) 1)
;;             val (get-val-ds v)]
;;         ;; (log/trace "emap-new setting prop: " k prop v val)
;;         (.setProperty e prop val)))
;;     (.put (datastore) e)
;;     (EntityMap. e)))

;; (defn- emap-old
;;   [^Key k ^Entity e content]
;;   {:pre [(map? content)]}
;;   ;; (log/trace "emap old " k content)
;;   (if (clj/empty? content)
;;     (EntityMap. e)
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
;;               (EntityMap. e))
;;             ;; new property
;;             (let [val (get-val-ds v)]
;;               ;; (log/trace "setting val" val (type val))
;;               ;; (flush)
;;               (.setProperty e prop val)))))
;;       (.put (datastore) e)
;;       ;; (log/trace "saved entity " e)
;;       (EntityMap. e))))

;; (defn- emap-update-empty
;;   [keylinks]
;;   (let [k (apply keychainer keylinks)
;;         e (try (.get (datastore) k)
;;                (catch EntityNotFoundException ex nil)
;;                (catch DatastoreFailureException ex (throw ex))
;;                (catch java.lang.IllegalArgumentException ex (throw ex)))]
;;         (if (emap? e)
;;           (EntityMap. e)
;;           (let [e (Entity. k)]
;;             (.put (datastore) e)
;;             (EntityMap. e)))))

;; ;; TODO: support embedded maps, e.g. (ds/emap!! [:Foo/bar] {:a 1, :b {:c 3, :d 4}})
;; ;; technique: store them as embedded entities
;; (defn- emap-update-map
;;   [keylinks content]
;;   ;; (log/trace "emap-update-map " keychain content)
;;   (let [k (apply keychainer keylinks)]
;;     ;; (log/trace "emap-update-map key: " k)
;;     (let [e (if (keyword? k)
;;               (let [e (Entity. (subs (str k) 1))] ;; key of form :Foo, i.e. a Kind specifier
;;                 (.put (datastore) e)
;;                 e)
;;               (try (.get (datastore) k)
;;                    (catch EntityNotFoundException ex nil)
;;                    (catch DatastoreFailureException ex (throw ex))
;;                    (catch java.lang.IllegalArgumentException ex (throw ex))))]
;;       (if (nil? e)
;;         (emap-new k content)
;;         (emap-old k e content) ; even a new one hits this if id autogenned by keychainer
;;         ))))


(defn- emap-update-fn
  "Second arg is a function to be applied to the Entity whose key is first arg"
  [keylinks f]
  (if (nil? (clj/namespace (first keylinks)))
    ;; if first link in keychain has no namespace, it cannot be an ancestor node
    (let [txn (.beginTransaction (datastore)) ;; else new entity
          e (Entity. (clj/name (first keylinks)))
          em (EntityMap. e)]
      (try
        (f em)
        (.put (datastore) e)
        (.commit txn)
        (finally
          (if (.isActive txn)
            (.rollback txn))))
      em)
    (let [k (apply keychainer keylinks)
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

>>>>>>> 8a635036bed39e4333e2b5b3d62e69d5ddbde433

;; no put - EntityMap only
(defn emap
  [keychain em]
  (if (clj/empty? keychain)
    (throw (IllegalArgumentException. "key vector must not be empty"))
    (let [k (apply keychain-to-key keychain)
          e (Entity. k)]
      (doseq [[k v] em]
        (.setProperty e (subs (str k) 1) (get-val-ds v)))
      (EntityMap. e))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Actions and modalities
;;
;; Actions:  + extend, = override, - delete, ! replace, ? find
;;   Actions concern what we do with properties - we can add new ones,
;;   and find/override/delete existing ones.

;; Modalities:  ! necessarily, ? possibly (exception on not found)
;; e.g.
;;   emap?! = find necessarily - return if found (ignoring props arg)
;;   else create, i.e. either find what's already there or "find"
;;   what's passed as props arg.

;;   emap?? = find possibly - return if found (ignoring props arg) else throw exception
;;
;;
(defn emap+!
  "Extend only, necessarily.  If entity found, extend only (add new
  props, do not override existing).  If entity not found,
  extend (i.e. create and add props)."
  [keylinks & propmap]
  (log/trace "emap+? " keylinks propmap)
  (if (clj/empty? keylinks)
    (throw (IllegalArgumentException. "key vector must not be empty"))
    (if (every? keylink? keylinks)
      (let [akey (apply keychainer keylinks)
            e (try (.get (datastore) akey)
                   (catch EntityNotFoundException ex (Entity. akey)))]
                   ;; (catch DatastoreFailureException ex (throw ex))
                   ;; (catch java.lang.IllegalArgumentException ex (throw ex)))]
        (if (not (nil? propmap))
          (let [props (first propmap)]
            (doseq [[k v] props]
              (let [p (.getProperty e (subs (str k) 1))]
                (log/trace "prop" k v)
                (if (nil? p)
                  (.setProperty e (subs (str k) 1) (get-val-ds v)))))))
        (.put (datastore) e)
        (EntityMap. e))
      (throw-bad-keylinks keylinks))))

(defn emap+?
  "Extend only, possibly.  If entity found, extend only.  If entity
  not found, throw exception."
  ([keylinks & propmap]
   (log/trace "emap+?" keylinks propmap)
   (if (clj/empty? keylinks)
     (throw (IllegalArgumentException. "key vector must not be empty"))
     (if (every? keylink? keylinks)
       (let [k (apply keychainer keylinks)
             e ;; (try
                 (.get (datastore) k)]
                    ;; (catch EntityNotFoundException ex (throw ex))
                    ;; (catch DatastoreFailureException ex (throw ex))
                    ;; (catch java.lang.IllegalArgumentException ex (throw ex)))]
        (if (not (nil? propmap))
          (let [props (first propmap)]
            (doseq [[k v] props]
              (let [p (.getProperty e (subs (str k) 1))]
                (log/trace "prop" k v)
                (if (nil? p)
                  (.setProperty e (subs (str k) 1) (get-val-ds v)))))))
        (.put (datastore) e)
        (EntityMap. e))
       (throw-bad-keylinks keylinks)))))

(defn emap=!
  "Override only, necessarily: if found, override existing props but
  do not extend (add new props); if not found, same - override
  existing nil vals."
  [keylinks & propmap]
  (log/trace "emap=! " keylinks propmap)
  (if (clj/empty? keylinks)
    (throw (IllegalArgumentException. "key vector must not be empty"))
    (if (every? keylink? keylinks)
      (let [akey (apply keychainer keylinks)
            e (try (.get (datastore) akey)
                   (catch EntityNotFoundException ex nil))]
                   ;; (catch DatastoreFailureException ex (throw ex))
                   ;; (catch java.lang.IllegalArgumentException ex (throw ex)))]
        (if (nil? e)
          (let [newe (Entity. akey)]
            (if (not (nil? propmap))
              (let [props (first propmap)]
                (doseq [[k v] props]
                      (.setProperty newe (subs (str k) 1) (get-val-ds v)))))
            (.put (datastore) newe)
            (EntityMap. newe))
          (if (not (nil? propmap))
            (let [props (first propmap)]
              (doseq [[k v] props]
                (let [p (.getProperty e (subs (str k) 1))]
                  (log/trace "prop" k v)
                  (if (not (nil? p))
                    (.setProperty e (subs (str k) 1) (get-val-ds v)))))
              (.put (datastore) e)
              (EntityMap. e)))))
      (throw-bad-keylinks keylinks))))

(defn emap=?
  "Override only, possibly: if found, override existing props but do
  not extend (add new props); if not found, throw exception."
  [keylinks & propmap]
  (log/trace "emap=? " keylinks propmap)
  (if (clj/empty? keylinks)
    (throw (IllegalArgumentException. "key vector must not be empty"))
    (if (every? keylink? keylinks)
      (let [akey (apply keychainer keylinks)
            e ;;(try
                (.get (datastore) akey)]
                   ;; (catch EntityNotFoundException ex (throw ex))
                   ;; (catch DatastoreFailureException ex (throw ex))
                   ;; (catch java.lang.IllegalArgumentException ex (throw ex)))]
        (if (not (nil? propmap))
          (let [props (first propmap)]
            (doseq [[k v] props]
              (let [p (.getProperty e (subs (str k) 1))]
                (log/trace "prop" k v)
                (if (not (nil? p))
                  (.setProperty e (subs (str k) 1) (get-val-ds v)))))))
        (.put (datastore) e)
        (EntityMap. e))
      (throw-bad-keylinks keylinks))))

(defn emap+=!
  "Extend and override, necessarily.  If entity found, add new props and override existing
  props.  If entity not found, create it and all all new props."
  [keylinks & propmap]
  (log/trace "emap+=! " keylinks propmap)
  (if (clj/empty? keylinks)
    (throw (IllegalArgumentException. "key vector must not be empty"))
    (if (every? keylink? keylinks)
      (let [akey (apply keychainer keylinks)
            e (try (.get (datastore) akey)
                   (catch EntityNotFoundException ex (Entity. akey)))]
                   ;; (catch DatastoreFailureException ex (throw ex))
                   ;; (catch java.lang.IllegalArgumentException ex (throw ex)))]
        (if (not (nil? propmap))
          (let [props (first propmap)]
            (doseq [[k v] props]
              (.setProperty e (subs (str k) 1) (get-val-ds v)))))
        (.put (datastore) e)
        (EntityMap. e))
      (throw-bad-keylinks keylinks))))

(defn emap+=?
  "Extend and override, possibly.  If entity found, add new props and
  override existing props.  If entity not found, throw exception."
  [keylinks & propmap]
  (log/trace "emap+=? " keylinks propmap)
  (if (clj/empty? keylinks)
    (throw (IllegalArgumentException. "key vector must not be empty"))
    (if (every? keylink? keylinks)
      (let [akey (apply keychainer keylinks)
            e ;;(try
                (.get (datastore) akey)]
                   ;; (catch EntityNotFoundException ex  (throw ex))
                   ;; (catch DatastoreFailureException ex (throw ex))
                   ;; (catch java.lang.IllegalArgumentException ex (throw ex)))]
        (if (not (nil? propmap))
          (let [props (first propmap)]
            (doseq [[k v] props]
              (.setProperty e (subs (str k) 1) (get-val-ds v)))))
        (.put (datastore) e)
        (EntityMap. e))
      (throw-bad-keylinks keylinks))))

(defn emap-!
  "Delete, necessarily.  If found, delete props, if not found, return nil."
  [keylinks props]
  (log/trace "emap+=? " keylinks props)
  (if (clj/empty? keylinks)
    (throw (IllegalArgumentException. "key vector must not be empty"))
    (if (every? keylink? keylinks)
      (let [akey (apply keychainer keylinks)
            e (try (.get (datastore) akey)
                   (catch EntityNotFoundException ex nil))]
                   ;; (catch DatastoreFailureException ex (throw ex))
                   ;; (catch java.lang.IllegalArgumentException ex (throw ex)))]
        (if (nil? e)
          nil
          (do
            (doseq [[k v] props]
              (.removeProperty e (subs (str k) 1)))
            (.put (datastore) e)
            (EntityMap. e))))
      (throw-bad-keylinks keylinks))))

(defn emap-?
  "Delete, possibly.  If found, delete props; if not found, throw exception."
  [keylinks props]
  (log/trace "emap+=? " keylinks props)
  (if (clj/empty? keylinks)
    (throw (IllegalArgumentException. "key vector must not be empty"))
    (if (every? keylink? keylinks)
      (let [akey (apply keychainer keylinks)
            e ;;(try
                (.get (datastore) akey)]
                   ;; (catch EntityNotFoundException ex (throw ex))
                   ;; (catch DatastoreFailureException ex (throw ex))
                   ;; (catch java.lang.IllegalArgumentException ex (throw ex)))]
        (doseq [[k v] props]
          (.removeProperty e (subs (str k) 1)))
        (.put (datastore) e)
        (EntityMap. e))
      (throw-bad-keylinks keylinks))))

(defn emap!!
  "Replace, necessarily.  Create new, discarding old even if found (so
  don't bother searching)."
  [keylinks & propmap]
  (log/trace "emap!!" keylinks propmap)
  (if (clj/empty? keylinks)
    (throw (IllegalArgumentException. "key vector must not be empty"))
    (if (keylink? (last keylinks))
      (if (every? keylink? (butlast keylinks))
        (emap-definite!! keylinks propmap)
        (throw-bad-keylinks (butlast keylinks)))
      (if (keykind? (last keylinks))
        (apply emap-indefinite!! keylinks propmap)
        (apply throw-bad-keykind (butlast keylinks))))))

(defn emap!
  "Like emap!! but works with partial keychain, so will always create new"
  [keylinks & propmap]
  (log/trace "emap!" keylinks propmap)
  (if (clj/empty? keylinks)
    (throw (IllegalArgumentException. "key vector must not be empty"))
    (if (every? keylink? (butlast keylinks))
      (if (and (keyword? (last keylinks)) (nil? (namespace (last keylinks))))
        (let [k (if (> (count keylinks) 1)
                  (apply keychainer (butlast keylinks)))
              e (if (> (count keylinks) 1)
                  (Entity. (subs (str (last keylinks)) 1)  k)
                  (Entity. (subs (str (last keylinks)) 1)))]
          (if (not (nil? propmap))
            (let [props (first propmap)]
              (doseq [[k v] props]
                (.setProperty e (subs (str k) 1) (get-val-ds v)))))
          (.put (datastore) e)
          (EntityMap. e))
        (throw (IllegalArgumentException. "last element of key vector must be name only (i.e. kind keyword), e.g. :Customer")))
      (throw-bad-keylinks keylinks))))

(defn emap!?
  "Replace, possibly.  If entity found, replace (i.e. discard old &
  create new); otherwise throw exception."
  [keylinks & propmap]
  (log/trace "emap!?" keylinks propmap)
  (if (clj/empty? keylinks)
    (throw (IllegalArgumentException. "key vector must not be empty"))
    (if (every? keylink? keylinks)
      (let [k (apply keychainer keylinks)
            e (.get (datastore) k)] ;; throws EntityNotFoundException
        ;; e was found; replace it
        (let [newe (Entity. k)]
          (if (not (nil? propmap))
            (let [props (first propmap)]
              (doseq [[k v] props]
                (.setProperty newe (subs (str k) 1) (get-val-ds v)))))
          (.put (datastore) newe)
          (EntityMap. newe)))
      (throw-bad-keylinks keylinks))))

(defn emap?!
  "Find, necessarily.  If keychain is partial (ends in a Kind
  keyword), will create new entity; i.e., ?! means 'find indefinite -
  find _some_ entity of this kind'.  If keychain is full (all
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

(defn emap??
  "Find, possibly.  If entity found, return without change, otherwise
  throw exception."
  ;; [keylinks & propmap]
  [keylinks]
   (log/trace "emap??" keylinks)
   (if (clj/empty? keylinks)
     (throw (IllegalArgumentException. "key vector must not be empty"))
     (if (every? keylink? keylinks)
       (let [k (apply keychainer keylinks)
             e ;;(try
                 (.get (datastore) k)]
                    ;; (catch EntityNotFoundException ex (throw ex))
                    ;; (catch DatastoreFailureException ex (throw ex))
                    ;; (catch java.lang.IllegalArgumentException ex (throw ex)))]
         (EntityMap. e))
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
  ;;     ;;   (log/trace "item entity" (.entity item)))
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
  (if (keylink? (last keylinks))
    (throw (IllegalArgumentException. "emaps?! keylinks must end in a Kind keyword (e.g. :Foo); try emap!!"))
    (if (map? maps)
      (emap?! keylinks maps)
      (doseq [emap maps]
        (do
          (emap?! keylinks emap))))
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

<<<<<<< HEAD
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
=======

(defmacro & [preds]
  )
>>>>>>> 8a635036bed39e4333e2b5b3d62e69d5ddbde433

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
        (throw (RuntimeException. "emaps??: bad keychain" keychain))
        )
      (throw-bad-keylinks (butlast keylinks)))))

(defmethod emaps?? 'Keychain
  [keylinks & pm]             ; e.g. :Foo/Bar
  (log/trace "emaps?? Keychain:" keylinks pm)
<<<<<<< HEAD
  (let [dskey (keychain-to-key (first keylinks) (second keylinks))
        e (try (.get (datastore) dskey)
         (catch EntityNotFoundException e (throw e))
         (catch Exception e (throw e)))]
=======
  (let [dskey (keychainer (first keylinks) (second keylinks))
        e ;;(try
            (.get (datastore) dskey)]
               ;; (catch EntityNotFoundException ex (throw ex))
               ;; (catch Exception ex (throw ex)))]
>>>>>>> 8a635036bed39e4333e2b5b3d62e69d5ddbde433
    (EntityMap. e)))

(defn- finish-q
  [q]
  ;; (log/trace "finish-q")
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

      ;; (let [pq (.prepare (datastore) q)
      ;;       it (.asIterator pq)
      ;;       emseq (emap-seq it)]
      ;;   emseq))))

(defmethod emaps?? 'Kinded
  [keylinks & filter-map]
  (log/trace "emaps?? Kinded:" keylinks filter-map (type filter-map))
  (let [kind (clj/name (last keylinks)) ;; we already know last link has form :Foo
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
          pq (.prepare (datastore) q)
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
<<<<<<< HEAD
                k (apply keychain-to-key keychain)
=======
                k (apply keychainer keylinks)
>>>>>>> 8a635036bed39e4333e2b5b3d62e69d5ddbde433
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
        e ;;(try
            (.get (datastore) dskey)]
         ;; (catch EntityNotFoundException ex (throw ex))
         ;; (catch Exception ex (throw ex)))]
    (EntityMap. e)))

(defmethod emaps?? 'Multikey
  [key-set]
  (log/trace "emapss?? Multikey:")
  )

<<<<<<< HEAD
=======
(defn alter!
  "Replace existing entity, or create a new one."
  [keylinks content]
  ;; if second arg is a map, treat it ...
  ;; if second arg is a function, ...
  (if (clj/empty? keylinks)
    (throw (IllegalArgumentException. "key vector must not be empty"))
    (let [k (apply keychainer keylinks)
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


  ;; (if (empty? keylinks)
  ;;   (do
  ;;     (log/trace "emap?? predicate-map filter" filter-map (type filter-map))
  ;;     (let [ks (clj/keylinks filter-map)
  ;;           vs (vals filter-map)
  ;;           k  (subs (str (first ks)) 1)
  ;;           v (last vs)
  ;;           f (Query$FilterPredicate. k Query$FilterOperator/EQUAL v)]
  ;;       (log/trace (format "key: %s, val: %s" k v))))
  ;;   (let [k (if (coll? keylinks)
  ;;             (apply keychainer keylinks)
  ;;             (apply keychainer [keylinks]))
  ;;         ;; foo (log/trace "emap?? kw keylinks: " k)
  ;;         e (try (.get (datastore) k)
  ;;                (catch EntityNotFoundException ex (throw ex))
  ;;                (catch DatastoreFailureException ex (throw ex))
  ;;                (catch java.lang.IllegalArgumentException ex (throw ex)))]
  ;;     (EntityMap. e))))

>>>>>>> 8a635036bed39e4333e2b5b3d62e69d5ddbde433
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
