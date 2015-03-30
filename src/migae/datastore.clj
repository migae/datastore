(ns migae.datastore
  (:refer-clojure :exclude [assoc! empty? get into key name])
  (:import [java.lang IllegalArgumentException]
           [java.util ArrayList HashMap HashSet Vector]
           [clojure.lang MapEntry]
           [com.google.appengine.api.datastore
            Blob
            DatastoreFailureException
            DatastoreService
            DatastoreServiceFactory
            DatastoreServiceConfig
            DatastoreServiceConfig$Builder
            Email
            EmbeddedEntity
            Entity
            EntityNotFoundException
            FetchOptions$Builder
            ImplicitTransactionManagementPolicy
            KeyFactory
            KeyFactory$Builder
            Key
            Link
            PhoneNumber
            ReadPolicy
            ReadPolicy$Consistency
            Query
            Query$FilterOperator
            Query$FilterPredicate
            Query$SortDirection
            ShortBlob
            Text
            Transaction]
           [com.google.appengine.api.blobstore BlobKey])
  (:require [clojure.core :as clj]
            [clojure.walk :as walk]
            [clojure.stacktrace]
            [clojure.tools.reader.edn :as edn]
            ;; [migae.datastore.service :as dss]
            [migae.datastore.entity :as dse]
            [migae.datastore.key :as dskey]
            [migae.datastore.query :as dsqry]
            [migae.infix :as infix]
            [clojure.tools.logging :as log :only [trace debug info]]))
  ;; (:use org.mobileink.migae.core.utils))

(defonce ^{:dynamic true} *datastore-service* (atom nil))

;; (defn get-datastore-service []
;;   (when (nil? @*datastore-service*)
;;         (reset! *datastore-service* (DatastoreServiceFactory/getDatastoreService)))
;;   @*datastore-service*)

(defn datastore []
  (when (nil? @*datastore-service*)
    (do ;; (prn "datastore ****************")
        (reset! *datastore-service* (DatastoreServiceFactory/getDatastoreService))))
  @*datastore-service*)


;; (defonce ^{:dynamic true} *datastore-service* (atom nil))
;; (defn get-datastore-service []
;;   (when (nil? @*datastore-service*)
;;     ;; (do (prn "getting ds service ****************")
;;     (reset! *datastore-service* (DatastoreServiceFactory/getDatastoreService)))
;;   @*datastore-service*)

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
    (log/trace "emap-iter next")
    (let [r (get-next-emap-prop this)
          k (.getKey r)
          v (.getValue r)
          res {(keyword k) v}]
      res))
;      {(keyword k) v}))

  ;; (remove  [this])
)


(declare get-val-ds get-val-clj)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(deftype EntityMap [entity]
  ;; migae.datastore.EntityMap
  ;; (EntityMap [this content])

  ;; com.google.appengine.api.datastore.Entity
  ;; (getKey [this] (.getKey this))

  java.lang.Iterable
  (iterator [this]
    (log/trace "Iterable iterator")
    (let [props (.getProperties entity) ;; java.util.Map<java.lang.String,java.lang.Object>
          entry-set (.entrySet props)
          e-iter (.iterator entry-set)
          em-iter (EntityMapIterator. e-iter) ]
    ;; (log/trace "Iterable res:" em-iter)
    em-iter))

  ;; FIXME: put :^EntityMap in every EntityMap
  clojure.lang.IMeta
  (meta [_] {:key (.getKey entity)})
  ;; clojure.lang.IObj
  ;; (withMeta [this md] (EntityMap. (with-meta m md)))

  clojure.lang.IFn
  (invoke [_ k]
    {:pre [(keyword? k)]}
    ;; (log/trace "IFn invoke")
    (let [prop (.getProperty entity (clj/name k))]
      (get-val-clj prop)))
  ;; (applyTo [_ arglist])
  ;; (invokePrim [_ ??] )

  ;; clojure.lang.IMapEntry
  ;; (key [_])
  ;; (val [_])

  clojure.lang.ITransientCollection
  (conj [this args]
    (log/trace "ITransientMap conj")
    (let [item (first args)
          k (clj/name (clj/key item))
          v (clj/val item)
          val (if (number? v) v
                  (if (string? v) v
                      (edn/read-string v)))]
      (log/trace "ITransientMap conj: " args item k v)
      (.setProperty entity k v)
      this))

  ;; clojure.lang.Counted
  ;; (count [_]
  ;;   (.size (.getProperties entity)))


  clojure.lang.IPersistentCollection
  (count [_]
    (.size (.getProperties entity)))
  (cons [this o] ;; o should be a MapEntry?
    (log/trace "cons: " o (type o))
    (cond
      (nil? o)
      this
      (= (type o) clojure.lang.MapEntry)
      (do
        ;; (log/trace "cons clj map entry to emap")
        (.setProperty entity (clj/name (first o)) (get-val-ds (second o)))
        (.put (datastore) entity)
        this)
      (= (type o) clojure.lang.PersistentArrayMap)
      (do
        ;; (log/trace "cons clj map to emap")
        (doseq [[k v] o]
          (let [nm (clj/name k)
                val (get-val-ds v)]
            (log/trace "cons key: " nm (type nm))
            (log/trace "cons val: " val (type val))
            (.setProperty entity nm val)))
        (.put (datastore) entity)
        this)
      (= (type o) java.util.Collections$UnmodifiableMap$UnmodifiableEntrySet$UnmodifiableEntry)
      (do
        ;; (log/trace "cons entity prop to emap")
        (.setProperty entity (.getKey o) (.getValue o))
        (.put (datastore) entity)
        this)
      (= (type o) EntityMap)
      (let [props (.getProperties (.entity o))]
        ;; (log/trace "cons emap to emap")
        (doseq [[k v] props]
          (.setProperty entity (clj/name k) (get-val-ds v)))
        (.put (datastore) entity)
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

  ;; clojure.lang.IReduce
  ;; (reduce [this f]
  ;;   (log/trace "reduce"))
  ;; (reduce [this f seed]
  ;;   (log/trace "reduce w/seed"))

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
    (let [prop (clj/name k)]
      (log/trace "assoc: " k v "(" prop v ")")
      (.setProperty entity prop v)
      this))
  (assocEx [_ k v]
    (EntityMap. (.assocEx entity k v)))
  (without [this k]                     ; = dissoc!, return new datum with k removed
    (let [prop (clj/name k)]
      (log/trace "without: removing prop " k "->" prop)
      (.removeProperty entity prop)
      this))

  clojure.lang.Associative
  (containsKey [_ k]
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

  ;; clojure.lang.Seqable
  ;; (seq [this] (EntityMap/seq this))

  clojure.lang.Seqable
  (seq [this]
    ;; (log/trace "seq" (.hashCode this) (.getKey entity))
    (let [props (.getProperties entity)
          kprops (clj/into {}
                           (for [[k v] props]
                             (do
                             (log/trace "v: " v)
                             (let [prop (keyword k)
                                   val (get-val-clj v)]
                               (log/trace "prop " prop " val " val)
                               {prop val}))))
          res (clj/seq kprops)]
      (log/trace "seq result:" entity " -> " res)
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
      (log/trace "valAt " k prop val)
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
(defmethod id Key
  [^Key k]
  (.getId k))
(defmethod id Entity
  [^Entity e]
  (.getId (.getKey e)))
(defmethod id EntityMap
  [^EntityMap e]
  (.getId (.getKey (.entity e))))

;; ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; ;; public class IteratorSeq extends ASeq
;; ;; public abstract class ASeq extends Obj implements ISeq, Sequential, List, Serializable, IHashEq
;; (deftype EntityMapSeq [em-iter
;;                        ^:volatile-mutable _val
;;                        ^:volatile-mutable _rest]

;;   clojure.lang.ISeq                     ;extends IPersistentCollection
;;   (first [this]                            ; car?  returns obj
;;     (log/trace "FIRST hash " (.hashCode this))
;;     (log/trace "FIRST val " _val)
;;     (log/trace "FIRST em-iter " em-iter)
;;     (log/trace "FIRST em-iter hasNext " (.hasNext em-iter))
;;     (EntityMap. _val))
;;   (next [this]                             ; = cdr?  returns ISeq, not Obj
;;     (log/trace "next: this " (.hashCode this))
;;     (log/trace "em-iter 1 type " (type em-iter))
;;     (log/trace "em-iter 1 " em-iter)
;;     ;; (log/trace "em-iter 1 hasNext " (.hasNext em-iter))
;;     (set! _val (.next em-iter))
;;     (set! _rest (if (.hasNext em-iter)
;;                   (let [ems (EntityMapSeq. em-iter nil nil)]
;;                     (log/trace "ems hash: " (.hashCode ems))
;;                     ems)
;;                   nil))
;;       _rest)
;;   (more [this]
;;     (log/trace "more " (.hashCode this))
;;     (let [s (next this)]
;;       (if (nil? s)
;;         nil ;; PersistentList/EMPTY
;;         s)))

;;     ;; (log/trace "EMSeq em-iter: " em-iter)
;;     ;; (if (.hasNext em-iter)
;;     ;;   (let [nxt (.next em-iter)]
;;     ;;     (log/trace "EMSeq.next " nxt)
;;     ;;     ;; (EntityMap. nxt))))
;;     ;;     )))

;;   ;; (more [_])
;;   ;; (cons [_])

;;   ;; clojure.lang.IPersistentCollection  ;; extends Seqable
;;   ;; (count [this])
;;   ;; (cons  [this o])
;;   ;; (empty [this])
;;   ;; (equiv [this o])

;;   clojure.lang.Seqable
;;   (seq [this]
;;     this)

;;   ;; clojure.lang.Sequential
;;   ;; ;; empty!

;;   ;; clojure.lang.IHashEq
;;   ;; (hasheq [_]
;;   ;;   (log/trace "IHashEq.hasheq"))

;;   ;; java.util.List
;;   ;; java.io.Serializable
;;   ) ;; deftype EntityMapSeq

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
      (.setProperty embed (clj/name k) (get-val-ds v)))
    embed))

(defn- get-val-clj-coll
  "Type conversion: java to clojure"
  [coll]
  (log/trace "get-val-clj-coll" coll (type coll))
  (cond
    (= (type coll) java.util.ArrayList) (clj/into '() (for [item coll]
                                                       (get-val-clj item)))
    (= (type coll) java.util.HashSet)  (clj/into #{} (for [item coll]
                                                       (get-val-clj item)))
    (= (type coll) java.util.Vector)  (clj/into [] (for [item coll]
                                                       (get-val-clj item)))
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
  (log/trace "get-val-ds-coll" coll (type coll))
  (cond
    (list? coll) (let [a (ArrayList.)]
                     (doseq [item coll]
                       (do
                         (log/trace "vector item:" item (type item))
                         (.add a (get-val-ds item))))
                     (log/trace "ds converted:" coll " -> " a)
                     a)

    (map? coll) (make-embedded-entity coll)

    (set? coll) (let [s (java.util.HashSet.)]
                  (doseq [item coll]
                    (let [val (get-val-ds item)]
                      ;; (log/trace "set item:" item (type item))
                      (.add s (get-val-ds item))))
                  (log/trace "ds converted:" coll " -> " s)
                  s)

    (vector? coll) (let [a (Vector.)]
                     (doseq [item coll]
                       (do
                         ;; (log/trace "vector item:" item (type item))
                         (.add a (get-val-ds item))))
                     (log/trace "ds converted:" coll " -> " a)
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
                  (string? v) (str "\"" v "\"")
                  (= (class v) java.lang.Double) (.toString v)
                  (= (class v) java.lang.Boolean) v
                  (= (class v) java.util.Date) v
                  (instance? java.util.Collection v) (get-val-clj-coll v)
                  (= (type v) Link) (.toString v)
                  (= (type v) Email) (.getEmail v)
                  (= (type v) EmbeddedEntity) (let [e (Entity. (.getKey v))]
                                                (.setPropertiesFrom e v) (EntityMap. e)
                                                (EntityMap. e))
                  (= (type v) Key) (let [kind (.getKind v)]
                                     (if (= kind "Keyword")
                                       (keyword (.getName v))
                                       ;; (symbol (.getName v))))
                                       (str \' (.getName v))))
                  :else (do
                          (log/trace "get-val-clj else " v (type v))
                          (edn/read-string v)))]
    ;; (log/trace "get-val-clj result:" v val)
    val))

;; this is for values to be stored (i.e. from clojure to ds)
(defn- get-val-ds
  [v]
  (log/trace "get-val-ds" v (type v))
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

;; (defn new-entitymap
;;   [raw-contents]
;;   {:pre [(let [m (apply hash-map raw-contents)]
;;            (:_kind m))]}
;;   ;; ;; :_kind required
;;   (let [m (apply hash-map raw-contents)
;;         {k :_kind n :_name i :_id} m]
;;     (if (not k)
;;       (throw (Throwable. "missing :_kind"))
;;       (if (and (not= (type k) java.lang.String)
;;                (not= (type k) clojure.lang.Keyword))
;;         (throw (Throwable. ":_kind must be String or Keyword"))
;;         (if (and n i) (throw (Throwable. "only one of:_name and :_id allowed"))
;;       ;; ;; :_name and :id optional; only one allowed
;;       ;; (if (:_name m) ...)
;;       ;; (if (:_id m) ...)
;;       ;; create entity now?
;;       (EntityMap. m))))))

(defn persist
  [theMap]
  {:pre [ ]} ;; TODO: validate entitymap
  (let [{kind :_kind name :_name id :_id parent :_parent} (meta theMap)
        parentEntity (if parent
                       (let [k (:_kind parent)
                             n (:_name parent)
                             i (:_id parent)]
                         (cond name (Entity. (clojure.core/name k) n)
                               id (Entity. (clojure.core/name k) i)
                               :else (Entity. (clojure.core/name k))))
                       nil)
        parentKey (if parent (dse/key parentEntity)
                      nil)
        theEntity (if parentKey
                    (cond name (Entity. (clojure.core/name kind) name parentKey)
                          id (Entity. (clojure.core/name kind) id parentKey)
                          :else (Entity. (clojure.core/name kind) parentKey))
                    (cond name (Entity. (clojure.core/name kind) name)
                          id (Entity. (clojure.core/name kind) id)
                          :else (Entity. (clojure.core/name kind))))]
        (do
          (doseq [[k v] theMap]
            (.setProperty theEntity (clojure.core/name k) v))
          (let [key (.put (datastore) theEntity)
                kw (if (and (not id) (not name)) :_id)
                v  (if (and (not id) (not name)) (dskey/id key))
                m (clj/assoc (meta theMap)
                    kw v
                    :_key key
                    :_entity theEntity)]
            (with-meta theMap m)))))

;; TODO:  support tabular input, each row one entity
(defn persist-list
  [kind theList]
  ;; (log/trace "persist kind" kind)
  ;; (log/trace "persist list:" theList)
  (doseq [item theList]
    (let [theEntity (Entity. (clojure.core/name kind))]
      (doseq [[k v] item]
        ;; (log/trace "item" k (type v))
        (.setProperty theEntity (clojure.core/name k)
                      (cond
                       (= (type v) clojure.lang.Keyword) (clojure.core/name v)
                       :else v)))
      (.put (datastore) theEntity)))
  true)

      ;;       kw (if (and (not id) (not name)) :_id)
      ;;       v  (if (and (not id) (not name)) (dskey/id key))
      ;;       m (assoc (meta theMap)
      ;;           kw v
      ;;           :_key key
      ;;           :_ent theEntity)]
      ;; (with-meta theMap m)))))

;; ################
;; (defn fetch
;;   ([^String kind] )
;;   ([^Key key] (dss/get key))
;;   ([^String kind ^String name] (let [key ...] (ds/get key)))
;;   ([^String kind ^Long id] (let [key ...] (ds/get key)))

(defmulti fetch
  (fn [{key :_key kind :_kind name :_name id :_id parent :_parent :as args}]
    (cond
     (= (type args) com.google.appengine.api.datastore.Key) :key
     parent :parent
     key  :keymap
     (and kind name) :kindname
     (and kind id)   :kindid
     kind :keysonly
     :else :bug)))

(defmethod fetch :bug
 [{key :_key kind :_kind name :_name id :_id :as args}]
  (log/trace "fetch method :bug, " args))

(defmethod fetch :key
  [key]
  {:pre [(= (type key) com.google.appengine.api.datastore.Key)]}
  (let [ent (.get (datastore) key)
        kind (dskey/kind key)
        name (dskey/name key)
        id (dskey/id   key)
        props  (clj/into {} (.getProperties ent))] ;; java.util.Collections$UnmodifiableMap???
    (with-meta
      (clj/into {} (for [[k v] props] [(keyword k) v]))
      {:_kind kind :_name name :_key key :_entity ent})))

(defmethod fetch :keymap
  [{key :_key}]
  {:pre [(= (type key) com.google.appengine.api.datastore.Key)]}
  (let [ent (.get (datastore) key)
        kind (dskey/kind key)
        name (dskey/name key)
        id (dskey/id   key)
        props  (clj/into {} (.getProperties ent))] ;; java.util.Collections$UnmodifiableMap???
    (with-meta
      (clj/into {} (for [[k v] props] [(keyword k) v]))
      {:_kind kind :_name name :_key key :_entity ent})))

(defmethod fetch :parent
  [{key :_key kind :_kind name :_name id :_id parent :_parent :as args}]
  (let [parentKey (dskey/make {:_kind (:_kind parent) :_name (:_name parent)})
        childKey (dskey/make {:_kind kind :_name name :_parent parent})
        ;; childKey (dskey/make parentKey kind name)
        ent (.get (datastore) childKey)
        kind (dskey/kind childKey)
        name (dskey/name childKey)
        id (dskey/id   childKey)
        props  (clj/into {} (.getProperties ent))]
    (with-meta
      (clj/into {} (for [[k v] props] [(keyword k) v]))
      {:_kind kind :_name name :_key childKey :_entity ent})))

(defmethod fetch :kindname
  ;; validate kind, name
  ;; {:pre [ ]}
  [{kind :_kind name :_name}]
  (let [foo (log/debug (format "fetching kind %s name %s" kind name))
        key (try (dskey/make {:_kind kind :_name name})
                 (catch Exception e nil))]
    (if key
      (let [ent (.get (datastore) key)
            props  (clj/into {} (.getProperties ent))]
        ;; TODO: if not found, return nil
        (with-meta
          (clj/into {} (for [[k v] props] [(keyword k) v]))
          {:_kind kind :_name name :_key key :_entity ent}))
      nil)))

(defmethod fetch :kindid
  ;; {:pre [ ]}
  [{kind :_kind id :_id}]
  (let [key (dskey/make {:_kind kind :_id id})
        ent (.get (datastore) key)
        props (clj/into {} (.getProperties ent))] ;; java.util.Collections$UnmodifiableMap???
    (with-meta
      (clj/into {} (for [[k v] props] [(keyword k) v]))
      {:_kind kind :_id id :_key key :_entity ent})))

;; see also dsqry/fetch
(defmethod fetch :keysonly
  ;; {:pre [ ]}
  [{kind :_kind :as args}]
  (let [q (dsqry/keys-only kind)
        foo (log/debug "isKeysOnly? " (.isKeysOnly q))
        pq (dsqry/prepare q)
        bar (log/debug "resulting prepq: " pq)
        c (log/debug "count " (dsqry/count pq))]
    (dsqry/run pq)))

(defmulti ptest
  (fn
    ([arg1 arg2]
       (cond
        ;; :_kind :Person
        (= arg1 :_kind) (do (log/trace "dispatching on kw " arg2) :kind)
        ;; :Person '(:sex = :M)
        (list? arg2) (do (log/trace "dispatching on kind filter "
                                    (infix/infix-to-prefix arg2)) :kindfilter)
        :else :bug)
        )
    ([kw kind filter]
       (let [f (infix/infix-to-prefix filter)]
         (do (println "ptest kw " kind)
             (println "ptest kw" (type kind))
             (log/trace "kw filter: " f)
             (cond
              ;; :_kind :Person '(:age >= 18)
              (list? filter) (do (log/trace "dispatching on kw filter "
                                    (infix/infix-to-prefix filter)) :kwfilter)
              (map? kw) (do (log/trace "map " kw) :map)
              (vector? kw) (do (let [a (apply hash-map kw)]
                                  (log/trace kw) :v))
              :else :bug
              ))))
    ;; ([kw kind & args]
    ;;    (let [a (apply hash-map kw kind args)
    ;;          f (:filter a)]
    ;;      (do (println "ptest a " a)
    ;;          (println "ptest a" (type a))
    ;;          (log/trace "filter: " (infix/infix-to-prefix f))
    ;;          (cond
    ;;           (map? a) (do (log/trace "map " a) :map)
    ;;           (vector? a) (do (let [a (apply hash-map a)]
    ;;                               (log/trace a) :v))
    ;;           :else :bug
    ;;           ))))
    ([{kind :_kind :as args}]
       (do (println "ptest b" args)
           (println "ptest b" (type args))
           (cond
            ;; check for null kind
            (keyword? args) (do (log/trace "dispatching on kind " args) :kind)
            (map? args) (do (log/trace "map " args) :map)
            (vector? args) (do (let [a (apply hash-map args)]
                                 (log/trace a) :v))
            :else :bug
            )))))

(defmethod ptest :bug
  [arg & args]
  (log/trace "ptest bug: " args)
  (log/trace "ptest bug: " (type args))
  )

(defmethod ptest :map
  [arg & args]
  )

(defmethod ptest :kw
  [arg1 arg2]
  )

(defmethod ptest :kind
  [arg & args]
  )

(defmethod ptest :kindfilter
  [kind filter]
  )

(defmethod ptest :kwfilter
  [kw kind filter]
  )

(defmethod ptest :v
  [arg & args]
  )
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; An alternative: use defProtocol

(defn dump-entity [theEntity]
  (do
    (prn "****************")
    (prn "Dumping entity: " theEntity)
    (prn "entity: " ((meta theEntity) :entity))
    (prn "keymap: "(meta theEntity))
    (prn "entitymap: " (theEntity))
    (prn "****************")
    ))


(declare Entities)
(declare wrap-entity)

(defn entity-from-entitymap
  [theEntityMap]
  {:pre [;; (do (prn "e map meta: " (meta theEntityMap))
         ;;     (prn "e map id: "   (:id (meta theEntityMap))) true),
         ;; :key not allowed in EntityMap initializers
         (not (nil? (:kind (meta theEntityMap)))),
         ;; one of :id or :name or neither
         (or (nil? (:id (meta theEntityMap)))
             (nil? (:name (meta theEntityMap)))),
         (if (not (nil? (:id (meta theEntityMap))))
           (number? (:id (meta theEntityMap)))
           true),
         (if (not (nil? (:name (meta theEntityMap))))
           (or (string?  (:name (meta theEntityMap)))
               (keyword? (:name (meta theEntityMap))))
           true)
         ;; TODO: validate :parent
         ]}
  (let [{:keys [kind id name parent]} (meta theEntityMap)
        arg2 (if id id (if name name nil))
        arg3 (if (nil? parent) nil
                 (cond
                  (= (type parent)
                     :migae.datastore/Key)
                  ;;no yet
                  nil
                  (= (type parent)
                     :migae.datastore/Entity)
                  (:key (meta parent))
                  :else  ;; type parent = EntityMap
                  (:key (meta (Entities parent)))))
        ;; OR: (ds/keys ds parent)))))
        theEntity (if (nil? parent)
                    (Entity. (clojure.core/name kind)
                             (if id id (if name name)))
                    (Entity. (clojure.core/name kind)
                             (if id id (if name name))
                             arg3))]    ; arg3 = parent Key
          (doseq [[k v] theEntityMap]
            ;; TODO: handle val types
            (.setProperty theEntity
                          (clojure.core/name k)
                          (if (number? v) v
                              (clojure.core/name v))))
          ;; TODO: wrap-entity s/b resonsible for putting if needed
          (.put (datastore) theEntity)
          (wrap-entity theEntity)))


;; QUESTION: do we want to implement a
;; :migae.datastore/Key clojo to go with our
;; :migae.datastore/Entity clojo?

(defn- wrap-entity
  ;; wrap-entity wraps an Entity in a function.  It memoizes metadata
  ;; (key, kind, id, name, etc.)  as a 'keymap' for use as clojure
  ;; metadata.  We could implement access to e.g. :kind as logic in
  ;; the function, but since this data is immutable, there is no
  ;; reason not to memoize it.  (TODO: see about using deftype for
  ;; Entities; problem is metadata)
  ;; BUT: implementing this in the closure amounts to the same thing?
  [theEntity]
  (do ;;(prn "making entity " theEntity)
      (let [theKey (.getKey theEntity)
            kind (keyword (.getKind theEntity))]
        ;; then construct function
        ^{:entity theEntity
          :parent (.getParent theEntity)
          :type ::Entity ;; :migae.datastore/Entity
          :key (.getKey theEntity)
          :kind kind ; (keyword (.getKind theEntity))
          :namespace (.getNamespace theEntity)
          :name (.getName theKey)
          :id (.getId theKey)
          :keystring (.toString theKey)
          :keystringrep (KeyFactory/keyToString theKey)}
        (fn [& kw]
          ;; the main job of the function is to lookup properties
          ;; TODO: accomodate iteration, seq-ing, etc
          ;; e.g.  (clj/into myEnt {:foo "bar"})
          ;; also conj, clj/into, etc.
          ;; e.g.  (conj myEnt {:foo "bar"})
          ;; etc.
          ;; only way I see to do this as of now is local replacement
          ;; funcs in our namespace
          ;; (cond (= kw :kind kind) ...
          (if (nil? kw)
            (let [props (.getProperties theEntity)]
              ;; efficiency?  this constructs map of all props
              ;; every time
              (clj/into {} (map (fn [item]
                              {(keyword (.getKey item))
                               (.getValue item)}) props)))
            (.getProperty theEntity (name kw)))))))

(declare keychainer)

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

(defn keychain? [k]
  ;; k is vector of DS Keys and clojure keywords
  (every? #(or (keyword? %) (= (type %) Key)) k))

(defmulti keychainer
  "Make a datastore Key from a Clojure symbol or a pair of args.  For
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

(defmethod keychainer [clojure.lang.Keyword nil]
  ([^clojure.lang.Keyword k]
   {:pre [(= (type k) clojure.lang.Keyword)]}
   ;;(log/trace "KEYWORD")
   (let [kind (clojure.core/namespace k)
         ident (edn/read-string (clojure.core/name k))]
     ;; (log/trace (format "keychainer 1: kind=%s, ident=%s" kind ident))
     (cond
       (nil? kind)
       (let [e (Entity. (str ident))]
         (.put (datastore) e))
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

(defn get-identifier
  [^Key k]
  (let [nm (.getName k)
        id (.getId k)]
    (if (nil? nm) id nm)))

(defn add-children [parent chain]
  (doseq [sym chain]
    (let [k (keychainer sym)]
      ;; (log/trace "Keychainer: " k)
      (.addChild parent
                 (.getKind k)
                 (get-identifier k)))))

(defmethod keychainer [com.google.appengine.api.datastore.Key clojure.lang.ArraySeq]
  ;; vector of keywords, string pairs, or both
  ([head & chain]
   (let [root (KeyFactory$Builder. head)]
     (.getKey (doto root (add-children chain))))))

(defmethod keychainer [clojure.lang.Keyword clojure.lang.ArraySeq]
  ;; vector of keywords, string pairs, or both
  ([head & chain]
   ;; (log/trace "kw arrayseq" head chain)
   ;; (let [root (KeyFactory$Builder. (clj/namespace head)
   ;;                                 ;; FIXME: check for IDs too, e.g. :Foo/d99, :Foo/x0F
   ;;                                 (clj/name head))]
   (let [k (keychainer head)
         root (KeyFactory$Builder. k)]
     (.getKey (doto root (add-children chain))))))


   ;; (if (empty? (first (seq chain)))
   ;;   head
   ;;   (key (first chain) (rest chain)))))

(defmethod keychainer [java.lang.String clojure.lang.ArraySeq]
  ;; vector of keywords, string pairs, or both
  ([head & chain]
   (log/trace "str str")))

(defmethod keychainer [clojure.lang.ArraySeq nil]
  ;; vector of keywords, string pairs, or both
  ([head & chain]
   (log/trace "seq nil" head chain)))

(defmethod keychainer [clojure.lang.PersistentList$EmptyList clojure.lang.ArraySeq]
  ([head & chain]
   (log/trace "emptylist arrayseq: " head chain)))


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
        (.setProperty e (clj/name k) (get-val-ds v)))
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
             (.setProperty e (clj/name k) (get-val-ds v)))
           (.put (datastore) e)
           ;; (log/trace "created and put entity " e)
           (EntityMap. e))
         (do
           ;; (log/trace "found entity " e)
           ;; if em content not null throw exception
           (EntityMap. e))))))
  ;; emap!
  ([keychain]
   (log/trace "emap! 1 " keychain)
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
       (log/trace "emap! got e: " e)
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
      (let [prop (clj/name k)
            val (get-val-ds v)]
        ;; (log/trace "emap-new setting prop: " k prop v val)
        (.setProperty e prop val)))
    (.put (datastore) e)
    (EntityMap. e)))

(defn- emap-old
  [^Key k ^Entity e content]
  {:pre [(map? content)]}
  ;; (log/trace "emap old")
  (if (clj/empty? content)
    (EntityMap. e)
    (doseq [[k v] content]
      (let [prop (clj/name k)]
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
            (log/trace "saving val" val (type val))
            (flush)
            (.setProperty e prop val)))
        (.put (datastore) e)
        ;;(log/trace "saved entity " e)
        )))
  (EntityMap. e))

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
  (let [k (apply keychainer keychain)
        e (try (.get (datastore) k)
               (catch EntityNotFoundException e nil)
               (catch DatastoreFailureException e (throw e))
               (catch java.lang.IllegalArgumentException e (throw e)))]
    (if (nil? e)
      (emap-new k content)
      (emap-old k e content)
      )))


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
      (if (fn? (first content))
        (emap-update-fn keychain (first content))
        (if (map? (first content))
          (emap-update-map keychain (first content))
          (throw (IllegalArgumentException. "content must be map or function")))))))


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
        (.setProperty e (clj/name k) v))
      (.put (datastore) e)
      ;; (log/trace "created and put entity " e)
      (EntityMap. e))))

          ;; e (try (.get (datastore) k)
          ;;        (catch EntityNotFoundException e
          ;;          ;; (log/trace (.getMessage e))
          ;;          e)
          ;;        (catch DatastoreFailureException e
          ;;          ;; (log/trace (.getMessage e))
          ;;          nil)
          ;;        (catch java.lang.IllegalArgumentException e
          ;;          ;; (log/trace (.getMessage e))
          ;;          nil))
                 ;; ]
      ;; (if (emap? e)
      ;;   (do
      ;;     (log/trace "found entity " e)
      ;;     (doseq [[k v] content]
      ;;       (.setProperty e (subs (str k) 1) (str v)))
      ;;     (log/trace "modified entity " e)
      ;;     (.put (datastore) e)
      ;;     (log/trace "get name " (.getProperty e "name"))
      ;;     e)


(defn key=
  [em1 em2]
  (.equals (.entity em1) (.entity em2)))

;; (defn get
;;   [^EntityMap em prop]
;;   ;; (log/trace "getting prop " prop " for ent " em)
;;   (let [p (.getProperty (.entity em) (subs (str prop) 1))]
;;     ;;(log/trace "got " p)
;;     p))

;; (defmacro :
;;   [prop em]
;;   ;; (log/trace "getting prop " prop " for ent " em)
;;   (let [p (.getProperty em (subs (str prop) 1))]
;;     ;;(log/trace "got " p)
;;     p))

;;;; TODO embedded entities


;; (def
;;  ^{:arglists '([map key val] [map key val & kvs])
;;    :doc "assoc[iate]. When applied to a map, returns a new map of the
;;     same (hashed/sorted) type, that contains the mapping of key(s) to
;;     val(s). When applied to a vector, returns a new vector that
;;     contains val at index. Note - index must be <= (count vector)."
;;    :added "1.0"
;;    :static true}
;;  assoc
;;  (fn ^:static assoc
;;    ([map key val] (. clojure.lang.RT (assoc map key val)))
;;    ([map key val & kvs]
;;     (let [ret (assoc map key val)]
;;       (if kvs
;;         (if (next kvs)
;;           (recur ret (first kvs) (second kvs) (nnext kvs))
;;           (throw (IllegalArgumentException.
;;                   "assoc expects even number of arguments after map/vector, found odd number")))
;;         ret)))))

;; (defn assoc
;;   "assoc for DS Entities"
;;   ;; ([^clojure.lang.IPersistentVector ekey propname val]
;;   ;;  (let [e (emap! ekey)]
;;   ;;    (.setProperty ekey (subs (str propname) 1) val)
;;   ;;    e))
;;   ([^com.google.appengine.api.datastore.Entity coll propname val]
;;    (.setProperty coll (subs (str propname) 1) val)
;;    coll)
;;   ([^com.google.appengine.api.datastore.Entity coll propname val & kvs]
;;    (.setProperty coll (subs (str propname) 1) val) ; setProperty returns void
;;    (if kvs
;;      (recur coll (first kvs) (second kvs) (nnext kvs))
;;      coll)))

(defn assoc!
  "unsafe assoc with save but no txn for DS Entities"
  ;; ([^clojure.lang.IPersistentVector coll propname val]
  ;;  )
  ([^com.google.appengine.api.datastore.Entity coll propname val]
   (.setProperty coll (subs (str propname) 1) val)
   (.put (datastore) coll)
   coll)
  ([^com.google.appengine.api.datastore.Entity coll propname val & kvs]
   (.setProperty coll (subs (str propname) 1) val) ; setProperty returns void
   (if kvs
     (recur coll (first kvs) (second kvs) (nnext kvs))
     (do
       (.put (datastore) coll)
       coll))))

(defn assoc!!
  "safe assoc with save and txn for DS Entities"
  ;; ([^clojure.lang.IPersistentVector coll propname val]
  ;;  )
  ([^com.google.appengine.api.datastore.Entity coll propname val]
   (let [txn (.beginTransaction (datastore))]
     ;; e (Entity. k)]
     (try
       (.setProperty coll (subs (str propname) 1) val)
       (.put (datastore) coll)
       (.commit txn)
       (finally
         (if (.isActive txn)
           (.rollback txn))))
     coll))
  ([^com.google.appengine.api.datastore.Entity coll propname val & kvs]
   (.setProperty coll (subs (str propname) 1) val) ; setProperty returns void
   (if kvs
     (recur coll (first kvs) (second kvs) (nnext kvs))
     coll)))

;; (def
;;  ^{:arglists '([coll x] [coll x & xs])
;;    :doc "conj[oin]. Returns a new collection with the xs
;;     'added'. (conj nil item) returns (item).  The 'addition' may
;;     happen at different 'places' depending on the concrete type."
;;    :added "1.0"
;;    :static true}
;;  conj (fn ^:static conj
;;         ([coll x] (. clojure.lang.RT (conj coll x)))
;;         ([coll x & xs]
;;          (if xs
;;            (recur (conj coll x) (first xs) (next xs))
;;            (conj coll x)))))

;; (defn into
;;   "Returns a new coll consisting of to-coll with all of the items of
;;   from-coll conjoined."
;;   {:added "1.0"
;;    :static true}
;;   [to from]
;;   (if (instance? clojure.lang.IEditableCollection to)
;;     (with-meta (persistent! (reduce conj! (transient to) from)) (meta to))
;;     (reduce conj to from)))

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

;;  single '?'  means predicate
;;  double '??' means expression as query (= sync expression against datastore)
(defn emap??                            ;FIXME: use a multimethod?
  [kws]                                 ; vector of keywords (DSKeys)
  ;; {:pre []} ;; check types
  ;; (log/trace "emap?? args: " kws)
  (let [k (if (keyword? kws)
            (apply keychainer [kws])
            (apply keychainer kws))
        ;; foo (log/trace "emap?? kw args: " k)
        e (try (.get (datastore) k)
                  (catch EntityNotFoundException e (throw e))
                  (catch DatastoreFailureException e (throw e))
                  (catch java.lang.IllegalArgumentException e (throw e)))]
      (EntityMap. e)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defmulti emaps??
  (fn [arg]
    (cond
      (key? arg)
      'Key
      (keyword? arg)
      (if (nil? (clj/namespace arg))
        'Kind                           ; e.g.  :Foo
        'Key)                          ; e.g.  :Foo/Bar
      (vector? arg)
      (cond
        (= 1 (count arg))
        (let [kw (first arg)]
          (if (keyword? kw)
            (if (nil? (clj/namespace kw))
              'KindVec
              'KeyVec)
            (throw (IllegalArgumentException. "arg must be a keyword"))))
        (= 2 (count arg))
        (let [kw1 (first arg)
              kw2 (last arg)]
          (if (and (keyword? kw1) (keyword? kw2))
            (if (nil? (namespace kw2))
              'Ancestor                   ; e.g. [:Foo/Bar :Baz]
              'Keychain)
            ;; 'Multikey)                  ; e.g. #{:Foo/Bar :Baz/Buz}
            (if (and (key? kw1) (keyword? kw2))
              (if (nil? (namespace kw2))
                'Ancestor                   ; e.g. [:Foo/Bar :Baz]
                'Keychain)
              (throw (IllegalArgumentException. "both args must be keywords")))))
        :else
        'Multikey)
      :else
      (throw (IllegalArgumentException. "arg must be key, keyword, or a vector of keywords")))))


;; (defmethod keychainer [clojure.lang.ArraySeq nil]
;;   ;; vector of keywords, string pairs, or both
;;   ([head & chain]
;;    (log/trace "seq nil" head chain)))

;; Key query (i.e. plain get)
(defmethod emaps?? 'Key
  [^clojure.lang.Keyword k]             ; e.g. :Foo/Bar
  (let [dskey (key k)
        e (try (.get (datastore) dskey)
         (catch EntityNotFoundException e (throw e))
         (catch Exception e (throw e)))]
    (EntityMap. e)))

(defmethod emaps?? 'Keychain
  [k]             ; e.g. :Foo/Bar
  (let [dskey (keychainer (first k) (second k))
        e (try (.get (datastore) dskey)
         (catch EntityNotFoundException e (throw e))
         (catch Exception e (throw e)))]
    (EntityMap. e)))

(defmethod emaps?? 'KeyVec
  [[^clojure.lang.Keyword k]]           ; e.g [:Foo/Bar]
  (let [dskey (key k)
        foo (log/trace "key: " dskey)
        e (try (.get (datastore) dskey)
         (catch EntityNotFoundException e (throw e))
         (catch Exception e (throw e)))]
    (EntityMap. e)))

;; core.clj:
;; (defn iterator-seq
;;   "Returns a seq on a java.util.Iterator. Note that most collections
;;   providing iterators implement Iterable and thus support seq directly."
;;   {:added "1.0"
;;    :static true}
;;   [iter]
;;   (clojure.lang.IteratorSeq/create iter))


;; Kind query
(defmethod emaps?? 'Kind               ; e.g. :Foo
  [^clojure.lang.Keyword kind]
  (log/trace "emaps?? Kind")
  (let [q (Query. (clj/name kind))
        pq (.prepare (datastore) q)
        it (.asIterator pq)
        emseq (emap-seq it)]
    (log/trace "EMSeq: " emseq)
    ;; (log/trace "EMSeq es: " emseq)
        ;; res (seq (.asIterable pq))]
        ;; res (iterator-seq (.asIterator pq))]
    ;; (iterator-seq it)))
    emseq))

(defmethod emaps?? 'KindVec               ; e.g. [:Foo]
  [[^clojure.lang.Keyword kind]]
  (let [q (Query. (clj/name kind))
        pq (.prepare (datastore) q)
        it (.asIterator pq)
        em-seq (emap-seq it)]
    em-seq))

(defmethod emaps?? 'Ancestor
  [kw-pair]
  (let [q (Query. (clj/name (last kw-pair)))
        qq (.setAncestor q (key (first kw-pair)))
        pq (.prepare (datastore) qq)
        it (.asIterator pq)
        em-seq (emap-seq it)]
    (log/trace "em-seq " em-seq)
    em-seq))

(defmethod emaps?? 'Multikey
  [key-set]
  )

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
  ;; (let [k (.getKey e)
  ;;       kch (get-keychain k)
  ;;       props (clj/into {} (.getProperties e))
  ;;       em (clj/into {} {:kind_ (.getKind k) :ident_ (get-identifier k)})]
  ;;       ;; em (clj/into {} {:key kch})]
  ;;   (clj/into em (for [[k v] props] [(keyword k) v]))))

(defmethod to-edn [Entity nil]
  ;;[com.google.appengine.api.datastore.Entity nil]
  [^Entity e]
  (let [k (.getKey e)
        kch (get-keychain k)
        props (clj/into {} (.getProperties e))
        em (clj/into {} {:kind_ (.getKind k) :ident_ (get-identifier k)})]
        ;; em (clj/into {} {:key kch})]
    (clj/into em (for [[k v] props] [(keyword k) v]))))
