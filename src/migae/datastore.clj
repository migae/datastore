(ns migae.datastore
  (:refer-clojure :exclude [assoc! empty? get into key name])
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
            Query Query$FilterOperator Query$FilterPredicate Query$SortDirection
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

(defn datastore []
  (when (nil? @*datastore-service*)
    (do ;; (prn "datastore ****************")
        (reset! *datastore-service* (DatastoreServiceFactory/getDatastoreService))))
  @*datastore-service*)

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
      ;; (log/trace "emap-iter hasNext")
      (.hasNext ds-iter)))
  (next    [this]                       ;
    ;; (log/trace "emap-iter next")
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
(deftype EntityMap [entity]

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
    (log/trace "IFn invoke")
    (let [prop (.getProperty entity (clj/name k))]
      (get-val-clj prop)))
  ;; (applyTo [_ arglist])
  ;; (invokePrim [_ ??] )

  ;; clojure.lang.IMapEntry
  ;; (key [_])
  ;; (val [_])

  clojure.lang.ITransientCollection
  (conj [this args]
    ;; (log/trace "ITransientMap conj")
    (let [item (first args)
          k (clj/name (clj/key item))
          v (clj/val item)
          val (if (number? v) v
                  (if (string? v) v
                      (edn/read-string v)))]
      ;; (log/trace "ITransientMap conj: " args item k v)
      (.setProperty entity k v)
      this))

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
        (.setProperty entity (clj/name (first o)) (get-val-ds (second o)))
        ;; (.put (datastore) entity)
        this)
      (= (type o) clojure.lang.PersistentArrayMap)
      (do
        ;; (log/trace "cons clj map to emap")
        (doseq [[k v] o]
          (let [nm (clj/name k)
                val (get-val-ds v)]
            ;; (log/trace "cons key: " nm (type nm))
            ;; (log/trace "cons val: " val (type val))
            (.setProperty entity nm val)))
        ;; (.put (datastore) entity)
        this)
      (= (type o) EntityMap)
      (let [props (.getProperties (.entity o))]
        ;; (log/trace "cons emap to emap")
        (doseq [[k v] props]
          (.setProperty entity (clj/name k) (get-val-ds v)))
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
      (log/trace "IPersistentCollection assoc: " k v "(" prop v ")")
      (.setProperty entity prop v)
      this))
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

  ;; clojure.lang.Seqable
  ;; (seq [this] (EntityMap/seq this))

  clojure.lang.Seqable
  (seq [this]
    ;; seq is called by: into, merge, "print", e.g. (log/trace em)
    (log/trace "seq" (.hashCode this) (.getKey entity))
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
      (.setProperty embed (clj/name k) (get-val-ds v)))
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

;; this is for values to be stored (i.e. from clojure to ds)
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

(declare emap?)

(declare keychain=)

(defn key=
  [em1 em2]
  (if (emap? em1)
    (if (emap? em2)
      (.equals (.entity em1) (.entity em2))
      (keychain= em1 em2))
    (if (map? em1)
      (keychain= em1 em2)
      (log/trace "EXCEPTION: key= applies only to maps and emaps"))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
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

(defmulti keychain class)
(defmethod keychain nil
  [x]
  nil)

(defmethod keychain Key
  [^Key k]
  (log/trace "keychain Key: " k)
  (if (nil? k)
    nil
    (let [kind (.getKind k)
          nm (.getName k)
          id (.getId k)
          this (keyword kind (if nm nm id))
          res (if (.getParent k)
                  (conj (list this) (keychain (.getParent k)))
                  (list this))]
      (log/trace "kind" kind "nm " nm " id " id " parent " (.getParent k))
      (log/trace "res: " res)
      (log/trace "res2: " (vec (flatten res)))
      (vec (flatten res)))))

(defmethod keychain Entity
  [^Entity e]
  (log/trace "keychain Entity: " e)
  (keychain (.getKey e)))

(defmethod keychain EntityMap
  [^EntityMap em]
  (log/trace "keychain EntityMap: " em)
  (keychain (.getKey (.entity em))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
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

(defmethod keychainer [Key nil]
  [k] k)

(defmethod keychainer [clojure.lang.Keyword nil]
  ([^clojure.lang.Keyword k]
   {:pre [(= (type k) clojure.lang.Keyword)]}
   ;; (log/trace "keychainer Keyword nil" k)
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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defmulti identifier class)

(defmethod identifier Key
  [^Key k]
  (log/trace "Key identifier" k)
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
(defn add-children
  [^KeyFactory$Builder builder chain]
  ;; (log/trace "add-chilren" builder chain)
  (doseq [sym chain]
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
                   (identifier k))))))

(defmethod keychainer [com.google.appengine.api.datastore.Key clojure.lang.ArraySeq]
  ;; vector of keywords, string pairs, or both
  ([head & chain]
   ;; (log/trace "keychainer Key ArraySeq" head chain)
   (let [root (KeyFactory$Builder. head)
         k (if (> (count chain) 1)
             (.getKey (doto root (add-children chain)))
             (.getKey (doto root (add-children chain))))]
             ;; (add-children root chain))]
     ;; (log/trace "keychainer Key ArraySeq result: " k)
     k)))
  ;; (let [k (first chain)]
       ;;   (if

(defmethod keychainer [clojure.lang.Keyword clojure.lang.ArraySeq]
  ;; vector of keywords, string pairs, or both
  ([head & chain]
   ;; (log/trace "kw Keyword ArraySeq" head chain)
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
      (let [prop (clj/name k)
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
              (let [e (Entity. (clj/name k))] ;; key of form :Foo, i.e. a Kind specifier
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

(defn assoc!
  "unsafe assoc with save but no txn for DS Entities"
  [m k v & kvs]
  ;; (log/trace "assoc! " m k v  "&" kvs)
   (let [txn (.beginTransaction (datastore))
         coll (if (emap? m)
                (.entity m)
                (if (= (class m) Entity)
                  m
                  (do (log/trace "HELP: assoc!") (flush))))]
     (try
       (.setProperty coll (clj/name k) v)
       (if (nil? (first kvs))
         (try
           (.put (datastore) coll)
           (.commit txn)
           (finally
             (if (.isActive txn)
               (.rollback txn))))
         (do ;; (log/trace "recur on assoc!")
             (assoc! coll (first kvs) (second kvs) (nnext kvs))))
       (finally
         (if (.isActive txn)
               (.rollback txn))))
       coll))

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
       (.setProperty coll (clj/name k) v)
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

;;  single '?'  means predicate
;;  double '??' means expression as query (= sync expression against datastore)
(defn emap??                            ;FIXME: use a multimethod?
  [kws]                                 ; vector of keywords (DSKeys)
  ;; {:pre []} ;; check types
  ;; (log/trace "emap?? args: " kws)
  (let [k (if (coll? kws)
            (apply keychainer kws)
            (apply keychainer [kws]))
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
        ;; foo (log/trace "key: " dskey)
        e (try (.get (datastore) dskey)
         (catch EntityNotFoundException e (throw e))
         (catch Exception e (throw e)))]
    (EntityMap. e)))

;; Kind query
(defmethod emaps?? 'Kind               ; e.g. :Foo
  [^clojure.lang.Keyword kind]
  ;; (log/trace "emaps?? Kind")
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
    ;; (log/trace "em-seq " em-seq)
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

(defmethod to-edn [Entity nil]
  ;;[com.google.appengine.api.datastore.Entity nil]
  [^Entity e]
  (let [k (.getKey e)
        kch (get-keychain k)
        props (clj/into {} (.getProperties e))
        em (clj/into {} {:kind_ (.getKind k) :ident_ (identifier k)})]
        ;; em (clj/into {} {:key kch})]
    (clj/into em (for [[k v] props] [(keyword k) v]))))

(defn into
  [to-coll from-coll]
  {:pre [(emap? from-coll)]}
  (log/trace "ds into" to-coll from-coll (type from-coll))
  (let [result (clj/into to-coll from-coll)]
    (log/trace "result " result (type result))
    (if (emap? to-coll)
      to-coll
      (with-meta to-coll
        {:migae/key
         (keychain (key from-coll))}))))
