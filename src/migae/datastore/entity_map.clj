(in-ns 'migae.datastore)
  ;; (:refer-clojure :exclude [name hash key])
  ;; (:import [com.google.appengine.api.datastore])
  ;;           ;; Entity
  ;;           ;; Key])
  ;; (:require [clojure.tools.logging :as log :only [trace debug info]]))

;;(load "datastore/dsmap")

(declare get-val-clj get-val-ds)
(declare props-to-map get-next-emap-prop)
(declare to-keychain keychain-to-key identifier)
(declare make-embedded-entity)

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
    {:migae/key (to-keychain entity)
     :type EntityMap})

  clojure.lang.IObj
  (withMeta [this md]
    (log/trace "IObj withMeta" md))
 ;; (EntityMap. (with-meta m md)))

  clojure.lang.IFn
  (invoke [_ k]
    {:pre [(keyword? k)]}
    ;; (log/trace "IFn invoke")
    (let [prop (.getProperty entity (clj/name k))]
      (get-val-clj prop)))
  ;; (applyTo [_ arglist])
  ;; (invokePrim [_ ??] )

  java.util.Map$Entry
  (getKey [this]
    (log/trace "java.util.Map$Entry getKey ")
    (let [k (.getKey entity)]
      (to-keychain k)))
  ;; FIXME: just do (to-keychain entity)??
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
    ;; (log/trace "count")
    (.size (.getProperties entity)))
  (cons [this o] ;; o should be a MapEntry?
    ;; (log/trace "cons: " o (type o))
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
            to-keychain (to-keychain to-map)
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
                          (to-keychain entity)
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
          kch (to-keychain entity)
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
            key-chain (to-keychain this)
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
    ;; (log/trace "containsKey " k)
    (let [prop (clj/name k)
          r    (.hasProperty entity prop)]
      r))
  (entryAt [this k]
    (let [val (.getProperty entity (clj/name k))
          entry (MapEntry. k val)]
      ;; (log/trace "entryAt " k val entry)
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
      ;; (log/trace "valAt " k ": " prop val)
      val))
  (valAt [_ k not-found]
    (log/trace "valAt w/notfound: " k)
    (.getProperty entity (str k) not-found)))
;; end deftype EntityMap

(deftype EntityMapCollIterator [ds-iter]
  java.util.Iterator
  (hasNext [this]
    (.hasNext ds-iter))
  (next    [this]
    (EntityMap. (.next ds-iter)))
  ;; (remove  [this])
  )



;; ;; this is for values to be printed (i.e. from ds to clojure)
;; (defn- get-val-clj
;;   [v]
;;   ;; (log/trace "get-val-clj" v (type v) (class v))
;;   (let [val (cond (integer? v) v
;;                   (string? v) (str v)
;;                   (= (class v) java.lang.Double) (.toString v)
;;                   (= (class v) java.lang.Boolean) v
;;                   (= (class v) java.util.Date) v
;;                   (instance? java.util.Collection v) (get-val-clj-coll v)
;;                   (= (type v) Link) (.toString v)
;;                   (= (type v) Email) (.getEmail v)
;;                   (= (type v) EmbeddedEntity) ;; (let [e (Entity. (.getKey v))]
;;                                               ;;  (.setPropertiesFrom e v)
;;                                                 (EntityMap. v) ;; )
;;                   (= (type v) Key) (let [kind (.getKind v)]
;;                                      (if (= kind "Keyword")
;;                                        (keyword (.getName v))
;;                                        ;; (symbol (.getName v))))
;;                                        (str \' (.getName v))))
;;                   :else (do
;;                           (log/trace "HELP: get-val-clj else " v (type v))
;;                           (edn/read-string v)))]
;;     ;; (log/trace "get-val-clj result:" v val)
;;     val))

;; ;; this is for values to be stored (i.e. from clojure to ds java types)
;; (defn- get-val-ds
;;   [v]
;;   ;; (log/trace "get-val-ds" v (type v))
;;   (let [val (cond (integer? v) v
;;                   (string? v) (str v)
;;                   (coll? v) (get-val-ds-coll v)
;;                   (= (type v) clojure.lang.Keyword) (keyword-to-ds v)
;;                   (= (type v) clojure.lang.Symbol) (symbol-to-ds v)
;;                   (= (type v) EmbeddedEntity) v
;;                   (= (type v) Link) v
;;                   (= (type v) Email) v
;;                   (= (type v) Key) v
;;                   (= (type v) java.lang.Double) v
;;                   (= (type v) java.lang.Long) v
;;                   (= (type v) java.lang.Boolean) v
;;                   (= (type v) java.util.Date) v
;;                   (= (type v) java.util.ArrayList) v ;; (clj/into [] v)
;;                   :else (do
;;                           (log/trace "ELSE: get val type" v (type v))
;;                           v))]
;;     ;; (log/trace "get-val-ds result:" v " -> " val "\n")
;;     val))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- keyword-to-ds
  [kw]
   (KeyFactory/createKey "Keyword"
                         ;; remove leading ':'
                         (subs (clj/str kw) 1)))

(defn- symbol-to-ds
  [sym]
   (KeyFactory/createKey "Symbol" (clj/str sym)))

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

(defn- props-to-map
  [props]
  (clj/into {} (for [[k v] props]
                 (let [prop (keyword k)
                       val (get-val-clj v)]
                   {prop val}))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defmulti to-keychain
  "to-keychain converts a DS Key to a vector of Clojure keywords"
  class)

(defmethod to-keychain migae.datastore.EntityMap
  [^EntityMap em]
  (log/trace "to-keychain EntityMap: " em)
  (to-keychain (.getKey (.entity em))))

(defmethod to-keychain nil
  [x]
  nil)

(defmethod to-keychain Key
  [^Key k]
  ;; (log/trace "to-keychain Key: " k)
  (if (nil? k)
    nil
    (let [kind (.getKind k)
          nm (.getName k)
          id (str (.getId k))
          this (keyword kind (if nm nm id))
          res (if (.getParent k)
                (conj (list this) (to-keychain (.getParent k)))
                (list this))]
      ;; (log/trace "kind" kind "nm " nm " id " id " parent " (.getParent k))
      ;; (log/trace "res: " res)
      ;; (log/trace "res2: " (vec (flatten res)))
      (vec (flatten res)))))

(defmethod to-keychain Entity
  [^Entity e]
  ;; (log/trace "to-keychain Entity: " e)
  (to-keychain (.getKey e)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn add-child-keylink
  [^KeyFactory$Builder builder chain]
  ;; (log/trace "add-child-keylink builder:" builder)
  ;; (log/trace "add-child-keylink chain:" chain)
  (doseq [sym chain]
    (if (nil? sym)
      nil
      (let [k (keychain-to-key sym)]
        ;; (log/trace "Keychain-To-Key: " sym " -> " k)
        (if (keyword? k)
          (let [parent (.getKey builder)
                e (Entity. (clj/name k) parent) ; k of form :Foo
                ;; v (.put (datastore) e)          ; should we store this?
                k (.getKey e)]
            ;; (log/trace "created entity " e)
            (.addChild builder (.getKind k) (.getId k)))
          (.addChild builder
                     (.getKind k)
                     (identifier k)))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defmulti keychain-to-key
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

(defmethod keychain-to-key [Key nil]
  [k] k)

;; (defmethod keychain-to-key [clojure.lang.PersistentVector nil]
;;   ([^clojure.lang.Keyword k]
;;    (
;;    ))

(defmethod keychain-to-key [clojure.lang.Keyword nil]
  ([^clojure.lang.Keyword k]
     {:pre [(= (type k) clojure.lang.Keyword)]}
     ;; (log/trace "keychain-to-key [Keyword nil]" k) (flush)
     (let [kind (clojure.core/namespace k)
           ident (edn/read-string (clojure.core/name k))]
       ;; (log/trace (format "keychain-to-key 1: kind=%s, ident=%s" kind ident))
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

(defmethod keychain-to-key [clojure.lang.Keyword clojure.lang.PersistentVector$ChunkedSeq]
  ;; vector of keywords, string pairs, or both
  ([head & chain]
     (log/trace "keychain-to-key Keyword ChunkedSeq" head chain)
     (flush)
     ;; (let [root (KeyFactory$Builder. (clj/namespace head)
     ;;                                 ;; FIXME: check for IDs too, e.g. :Foo/d99, :Foo/x0F
     ;;                                 (clj/name head))]
     (let [k (keychain-to-key head)
           root (KeyFactory$Builder. k)]
       (.getKey (doto root (add-child-keylink chain))))))

(defmethod keychain-to-key [clojure.lang.Keyword clojure.lang.ArraySeq]
  ;; vector of keywords, string pairs, or both
  ([head & chain]
     ;; (log/trace "kch Keyword ArraySeq" head chain)
     ;; (let [root (KeyFactory$Builder. (clj/namespace head)
     ;;                                 ;; FIXME: check for IDs too, e.g. :Foo/d99, :Foo/x0F
     ;;                                 (clj/name head))]
     (let [k (keychain-to-key head)
           root (KeyFactory$Builder. k)]
       (.getKey (doto root (add-child-keylink chain))))))


;; (if (empty? (first (seq chain)))
;;   head
;;   (key (first chain) (rest chain)))))

(defmethod keychain-to-key [clojure.lang.MapEntry nil]
  [^clojure.lang.MapEntry k]
  (log/trace "keychain-to-key MapEntry nil" k) (flush)
  (let [kind (clojure.core/namespace k)
        ident (edn/read-string (clojure.core/name k))]
    ;; (log/trace (format "keychain-to-key 1: kind=%s, ident=%s" kind ident))
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

(defmethod keychain-to-key [com.google.appengine.api.datastore.Key clojure.lang.ArraySeq]
  ;; vector of keywords, string pairs, or both
  ([head & chain]
     ;; (log/trace "keychain-to-key Key ArraySeq" head chain)
     (let [root (KeyFactory$Builder. head)
           k (if (> (count chain) 1)
               (.getKey (doto root (add-child-keylink chain)))
               (.getKey (doto root (add-child-keylink chain))))]
       ;; (add-child-keylink root chain))]
       ;; (log/trace "keychain-to-key Key ArraySeq result: " k)
       k)))
;; (let [k (first chain)]
;;   (if

(defmethod keychain-to-key [java.lang.String clojure.lang.ArraySeq]
  ;; vector of keywords, string pairs, or both
  ([head & chain]
     (log/trace "str str")))

(defmethod keychain-to-key [clojure.lang.ArraySeq nil]
  ;; vector of keywords, string pairs, or both
  ([head & chain]
     (log/trace "seq nil" head chain)))

(defmethod keychain-to-key [clojure.lang.PersistentVector$ChunkedSeq nil]
  ;; vector of keywords, string pairs, or both
  ([head & chain]
     (log/trace "keychain-to-key ChunkedSeq nil:" head chain)
     (keychain-to-key (first head) (rest head))))

(defmethod keychain-to-key [clojure.lang.PersistentList$EmptyList clojure.lang.ArraySeq]
  ([head & chain]
     (log/trace "emptylist arrayseq: " head chain)
     ))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defmulti identifier class)

(defmethod identifier Key
  [^Key k]
  ;; (log/trace "Key identifier" k)
  (let [nm (.getName k)
        id (.getId k)]
    (if (nil? nm) id nm)))

(defmethod identifier migae.datastore.EntityMap
  [^EntityMap em]
  (log/trace "EM identifier" (.entity em))
  (let [k (.getKey (.entity em))
        nm (.getName k)
        id (.getId k)]
    (if (nil? nm) id nm)))

(defn- make-embedded-entity
  [m]
  {:pre [(map? m)]}
  (let [embed (EmbeddedEntity.)]
    (doseq [[k v] m]
      ;; FIXME:  (if (map? v) then recur
      (.setProperty embed (subs (str k) 1) (get-val-ds v)))
    embed))

;;(load "datastore/ekey")
