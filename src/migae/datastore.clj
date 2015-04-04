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
            [migae.datastore.entity :as dse]
            [migae.datastore.key :as dskey]
            [migae.datastore.query :as dsqry]
            [migae.infix :as infix]
            [clojure.tools.logging :as log :only [trace debug info]]))

(declare emap?? epr)

(defn ds-filter
  [pred]
  (emap?? pred))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(deftype DatastoreMap [ds]

  clojure.lang.Associative
  (containsKey [_ k]
    (log/trace "DatastoreMap containsKey " k)
    )
  (entryAt [this k]
    (log/trace "DatastoreMap entryAt " k)
    )

  clojure.lang.IFn
  (invoke [_ k]
    {:pre [(keyword? k)]}
    (log/trace "IFn invoke" k)
    (emap?? k))

  clojure.lang.ILookup
  (valAt [_ k]
    ;; (log/trace "valAt " k)
    (emap?? k))
  (valAt [_ k not-found]
    (log/trace "valAt w/notfound: " k)
    )

  clojure.lang.Seqable
  (seq [this]
    ;; seq is called by: into, merge, "print", e.g. (log/trace em)
    (log/trace "DatastoreMap seq")

    ;; (let [props (.getProperties entity)
    ;;       kprops (clj/into {}
    ;;                        (for [[k v] props]
    ;;                          (do
    ;;                            ;; (log/trace "v: " v)
    ;;                            (let [prop (keyword k)
    ;;                                  val (get-val-clj v)]
    ;;                              ;; (log/trace "prop " prop " val " val)
    ;;                              {prop val}))))
    ;;       res (clj/seq kprops)]
    ;;   ;; (log/trace "seq result:" entity " -> " res)
    ;;   (flush)
    ;;   res))
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
  (meta [_]
    ;; (log/trace "IMeta meta")
    {:migae/key (keychain entity)
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
    ;; (log/trace "java.util.Map$Entry getKey")
    (let [k (.getKey entity)]
      (keychain k)))
      ;;     kind (.getKind k)
      ;;     nm (.getName k)]
      ;; [(keyword kind (if nm nm (.getId k)))]))
  (getValue [_]
    ;; (log/trace "java.util.Map$Entry getVal")
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
    (log/trace "cons: " o (type o))
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
      o
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
          (.setProperty entity (subs (str k) 1) (get-val-ds v)))
        ;; (.put (datastore) entity)
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
    ;; (try (/ 1 0) (catch Exception e (print-stack-trace e)))
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
    ;; (log/trace "seq" (.hashCode this) (.getKey entity))
    (let [props (.getProperties entity)
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
      (log/trace "valAt " k ": " prop val)
      val))
  (valAt [_ k not-found]
    (log/trace "valAt w/notfound: " k)
    (.getProperty entity (str k) not-found)))

;; (defmethod clojure.core/print-method ::EntityMap
;;   [em writer]
;;   (.write writer (str (:number piece) (:letter piece)) 0 2))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn get-next-emap-prop [this]
  ;; (log/trace "get-next-emap-prop" (.ds-iter this))
  (let [r (.next (.ds-iter this))]
    ;; (log/trace "next: " r)
    r))


(deftype EntityMapCollIterator [ds-iter]
  java.util.Iterator
  (hasNext [this]
    (.hasNext ds-iter))
  (next    [this]
    (EntityMap. (.next ds-iter)))
  ;; (remove  [this])
)

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

(declare keychainer emap? keychain=)

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
  (and (keyword k)
       (not (nil? (clj/namespace k)))))

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
  (log/trace "EM identifier" (.entity em))
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
        (if (keyword? k)
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
  "Make a datastore Key from a Clojure keyword or vector of keywords.  For
  numeric IDs with keywords use e.g. :Foo/d123 (decimal) or :Foo/x0F (hex)"
  (fn [arg & args]
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

;; (defmethod keychainer [clojure.lang.PersistentVector nil]
;;   ([^clojure.lang.Keyword k]
;;    (
;;    ))

(defmethod keychainer [clojure.lang.Keyword nil]
  ([^clojure.lang.Keyword k]
   {:pre [(= (type k) clojure.lang.Keyword)]}
   ;; (log/trace "keychainer [Keyword nil]" k) (flush)
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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;  emap stuff

(defn emap?
  [em]
  (= (type em) migae.datastore.EntityMap))

(defn entity?
  [e]
  (= (type e) Entity))

(defn empty?
  [em]
  (= (count em) 0))

;; no put - EntityMap only
(defn emap
  [keychain em]
  (if (clj/empty? keychain)
    (throw (IllegalArgumentException. "key vector must not be empty"))
    (let [k (apply keychainer keychain)
          e (Entity. k)]
      (doseq [[k v] em]
        (.setProperty e (subs (str k) 1) (get-val-ds v)))
      (EntityMap. e))))

;; without override - discard body if entity already exists
(defn emap!
  ([keychain em]
   ;; (log/trace "emap! 2 args " keychain em)
   (if (clj/empty? keychain)
     (throw (IllegalArgumentException. "key vector must not be empty"))
     (let [k (apply keychainer keychain)
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
     (let [k (apply keychainer keychain)
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
  (let [k (apply keychainer keychain)
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
  (let [k (apply keychainer keychain)]
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
        (emap-old k e content) ; even a new one hits this if id autogenned by keychainer
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
    (let [k (apply keychainer keychain)
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
    (let [k (apply keychainer keychain)
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
  (let [dskey (keychainer (first keylinks) (second keylinks))
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
      (let [k (apply keychainer pfx)
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
                k (apply keychainer keychain)
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
                                               ;; (keychainer (first (second op)) (rest (second op))))
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

(defn emap??                            ;FIXME: use a multimethod?
  [keylinks filter-map]  ; keychain and predicate-map
  ;; {:pre []} ;; check types
  (log/trace "emap?? keylinks" keylinks (type keylinks))
  (log/trace "emap?? filter-map" filter-map (type filter-map))
)
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
  ;;                (catch EntityNotFoundException e (throw e))
  ;;                (catch DatastoreFailureException e (throw e))
  ;;                (catch java.lang.IllegalArgumentException e (throw e)))]
  ;;     (EntityMap. e))))

;; Filter propertyFilter =
;;   new FilterPredicate("height",
;;                       FilterOperator.GREATER_THAN_OR_EQUAL,
;;                       minHeight);
;; Query q = new Query("Person").setFilter(propertyFilter);


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- get-keychain
  [^Key k]
  (let [parent (.getParent k)
        kind (.getKind k)
        nm (.getName k)
        id (.getId k)
        kw (if (nil? nm)
             (keyword kind (str id))
              (keyword kind nm))]
    (if (nil? parent)
      [kw]
      (clj/into [kw]
             (get-keychain parent)))))

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
        kch (get-keychain k)
        props (clj/into {} (.getProperties e))
        em (clj/into {} {:kind_ (.getKind k) :ident_ (identifier k)})]
        ;; em (clj/into {} {:key kch})]
    (clj/into em (for [[k v] props] [(keyword k) v]))))

;; from 1.7-alpha6
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
