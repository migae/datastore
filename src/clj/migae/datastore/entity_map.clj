(clojure.core/println "Start loading migae.datastore.types.entity_map")

#_(ns migae.datastore.types.entity-map
  (:refer-clojure :exclude [name hash])
  (:import [java.lang IllegalArgumentException RuntimeException]
           [java.util
            Collection
            Collections
            ArrayList
            HashMap HashSet
            Map Map$Entry
            Vector]
           [clojure.lang IFn ILookup IMapEntry IObj
            IPersistentCollection IPersistentMap IReduce IReference ISeq ITransientCollection]
           [com.google.appengine.api.datastore
            Blob
            DatastoreFailureException
            DatastoreService
            DatastoreServiceFactory
            DatastoreServiceConfig
            DatastoreServiceConfig$Builder
            Email
            Entity EmbeddedEntity EntityNotFoundException
            Key KeyFactory KeyFactory$Builder
            Link
            Query Query$SortDirection]
           )
  ;; (:use [clj-logging-config.log4j])
  (:require [clojure.test :refer :all]
            [clojure.tools.reader.edn :as edn]
            ;; [migae.datastore :as ds]
            ;; [migae.datastore.keys :as k]
            [clojure.tools.logging :as log :only [trace debug info]]))

(clojure.core/println "loading migae.datastore.types.entity_map")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;  PersistentEntityMap
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(in-ns 'migae.datastore)

(clojure.core/refer 'clojure.core)
(require '(clojure.tools [logging :as log :only [debug info]])
         ;; '(migae.datastore.adapter [gae :as gae])
         ;; '(migae.datastore [keys :as k])
         ;; get access to funcs above in local ns
         ;; '(migae.datastore.types [entity-map :refer :all])
         '(clojure.tools.reader [edn :as edn])
         )

(import '(clojure.lang IFn IObj IPersistentMap ISeq)
        '(com.google.appengine.api.datastore Entity
                                             Email
                                             EmbeddedEntity
                                             Key KeyFactory
                                             Link)
        '(java.lang IllegalArgumentException RuntimeException)
        '(java.util ;Collection
          ;Collections
            ArrayList
            ;; HashMap HashSet
            ;; Map Map$Entry
             Vector
            ))

(declare clj-to-ds ds-to-clj ds-to-clj-coll)
(declare ->PersistentEntityMap)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
#_(defn- get-parent-keychain
  [^Key k]
  {:pre [(not (nil? k))]}
  (log/trace "get-parent-keychain for key: " k)
  (let [kind (.getKind k)
        nm (.getName k)
        id (str (.getId k))
        dogtag (keyword kind (if nm nm id))
        res (if (.getParent k)
              (conj (list dogtag) (keychain (.getParent k)))
              (list dogtag))]
    ;; (log/trace "kind" kind "nm " nm " id " id " parent " (.getParent k))
    ;; (log/trace "res: " res)
    ;; (log/trace "res2: " (vec (flatten res)))
    (vec (flatten res))))

(defn- get-keychain
  [^Key k]
  {:pre [(not (nil? k))]}
  ;; (log/trace "keychain co-ctor 2: key" k)
  (let [kind (.getKind k)
        nm (.getName k)
        id (str (.getId k))
        dogtag (keyword kind (if nm nm id))
        res (if (.getParent k)
              (conj (list dogtag) (get-keychain (.getParent k)))
              (list dogtag))]
    ;; (log/trace "kind" kind "nm " nm " id " id " parent " (.getParent k))
    ;; (log/trace "res: " res)
    ;; (log/trace "res2: " (vec (flatten res)))
    (vec (flatten res))))


;;; implementation helpers
(defn- make-embedded-entity
  [m]
  {:pre [(map? m)]}
  (let [embed (EmbeddedEntity.)]
    (doseq [[k v] m]
      ;; FIXME:  (if (map? v) then recur
      (.setProperty embed (subs (str k) 1) (clj-to-ds v)))
    embed))

(defn- embedded-entity->map
  [ee]
  (let [props (.getProperties ee)
        emap (into {} (for [[k v] props]
                        (do
                          (let [prop (keyword k)
                                val (if (= (type v) EmbeddedEntity)
                                      (embedded-entity->map v)
                                      (ds-to-clj v))]
                            ;; (println "prop " prop " val " val)
                            {prop val}))))]
    emap))

(defn ds-to-clj
  [v]
  ;; (log/trace "ds-to-clj:" v (type v) (class v))
  (let [val (cond (integer? v) v
                  (string? v) (str v)
                  (= (class v) java.lang.Double) v
                  (= (class v) java.lang.Boolean) v
                  (= (class v) java.util.Date) v

                  (= (class v) java.util.Collections$UnmodifiableMap)
                  (let [props v]
                    (into {} (for [[k v] props]
                               (let [prop (keyword k)
                                     val (ds-to-clj v)]
                                 {prop val}))))

                  (instance? java.util.Collection v) (ds-to-clj-coll v)
                  (= (type v) Link) (.toString v)

                  (= (type v) Email) (.getEmail v)
                  (= (type v) EmbeddedEntity)
                  (do (log/trace "CONVERTING EMBEDDED ENTITY")
                      (->PersistentEntityMap v nil))

                  (= (type v) Key) (let [kind (.getKind v)]
                                     (if (= kind "Keyword")
                                       (keyword (.getName v))
                                       ;; (symbol (.getName v))))
                                       (str \' (.getName v))))
                  :else (do
                          ;; (log/trace "HELP: ds-to-clj else " v (type v))
                          (throw (RuntimeException. (str "HELP: ds-to-clj else " v (type v))))
                          (edn/read-string v)))]
    ;; (println "ds-to-clj result:" v val)
    val))

(defn ds-to-clj-coll
  "Type conversion: java datastore to clojure"
  [coll]
  ;; (log/trace "ds-to-clj-coll" coll (type coll))
  (cond
   (= (type coll) java.util.ArrayList) (into '() (for [item coll]
                                                   (ds-to-clj item)))
   (= (type coll) java.util.HashSet)  (into #{} (for [item coll]
                                                  (ds-to-clj item)))
   (= (type coll) java.util.Vector)  (into [] (for [item coll]
                                                (ds-to-clj item)))
   :else (log/trace "EXCEPTION: unhandled coll " coll)
   ))

;;(declare ->PersistentEntityMap)

(defn clj-to-ds-coll
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
  ;; (log/trace "clj-to-ds-coll" coll (type coll))
  (cond
   (list? coll) (let [a (ArrayList.)]
                  (doseq [item coll]
                    (do
                      ;; (log/trace "vector item:" item (type item))
                      (.add a (clj-to-ds item))))
                  ;; (log/trace "ds converted:" coll " -> " a)
                  a)

   (map? coll) (make-embedded-entity coll)

   (set? coll) (let [s (java.util.HashSet.)]
                 (doseq [item coll]
                   (let [val (clj-to-ds item)]
                     ;; (log/trace "set item:" item (type item))
                     (.add s (clj-to-ds item))))
                 ;; (log/trace "ds converted:" coll " -> " s)
                 s)

   (vector? coll) (let [a (Vector.)]
                    (doseq [item coll]
                      (do
                        ;; (log/trace "vector item:" item (type item))
                        (.add a (clj-to-ds item))))
                    ;; (log/trace "ds converted:" coll " -> " a)
                    a)

   :else (do
           (log/trace "HELP" coll)
           coll))
  )

(defn- keyword-to-ds
  [kw]
  (KeyFactory/createKey "Keyword"
                        ;; remove leading ':'
                        (subs (str kw) 1)))

(defn- symbol-to-ds
  [sym]
  (KeyFactory/createKey "Symbol" (str sym)))

(defn clj-to-ds
  [v]
  ;; (log/trace "clj-to-ds" v (type v))
  (let [val (cond (integer? v) v
                  (string? v) (str v)
                  (coll? v) (clj-to-ds-coll v)
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
    ;; (log/trace "clj-to-ds result:" v " -> " val "\n")
    val))

(clojure.core/println "migae.datastore.types.entity_map ns:" *ns*)

(deftype PersistentEntityMap [content ^IPersistentMap em-meta]

  ;; Protocol
  ;; migae.datastore.IPersistentEntityMap
 ;; :extends [java.lang.Iterable
 ;;           java.util.Map
 ;;           java.io.Serializable
 ;;           ;; NB: these are all Java interfaces
 ;;           clojure.lang.IFn             ; AFn: call(), run(), invoke()
 ;;           clojure.lang.IPersistentMap
 ;;           clojure.lang.IHashEq
 ;;           clojure.lang.IKVReduce       ; support clojure.core/reduce-kv
 ;;           clojure.lang.MapEquivalence
 ;;           clojure.lang.IObj ;; extends IMeta
 ;;           clojure.lang.ITransientCollection


  ;;Protocol:
  ;; java.lang.Iterable
  ;; (iterator [this]
  ;;   ;; (log/trace "Iterable iterator" (.content this))
  ;;   (let [props (.getProperties content) ;; java.util.Map<java.lang.String,java.lang.Object>
  ;;         entry-set (.entrySet props)
  ;;         e-iter (.iterator entry-set)
  ;;         ;; em-iter (->PersistentEntityMapSeq e-iter)
  ;;         ]
  ;;     ;; (log/trace "Iterable res:" em-iter)
  ;;     ;; em-iter))
  ;;     e-iter))

  ;;Protocol:
  java.util.Map
  ;; void	clear()
  ;; boolean containsKey(Object key) => implemented by Associative, below
  ;; boolean containsValue(Object value)
  ;; Set<Map.Entry<K,V>> entrySet()
  ;; boolean equals(Object o)
  ;; V get(Object key)
  (get [_ k]
    (log/trace "PersistentEntityMap.get: " k)
    ;; (let [prop (name k)
    ;;       r    (.hasProperty content prop)]
    (get (.getProperties content) (name k)))
  ;; int hashCode()
  (hashCode [_]
    (log/trace "PersistentEntityMap.hashCode")
    (.hashCode content))
  ;; boolean isEmpty()
  (isEmpty [_]
    (log/trace "PersistentEntityMap.hashCode")
    (.isEmpty (.getProperties content)))
  ;; Set<K> keySet()
  (keySet [_]
    (log/trace "PersistentEntityMap.keySet")
    (.keySet (.getProperties content)))
  ;; V put(K key, V value)
  ;; void putAll(Map<? extends K,? extends V> m)
  ;; V remove(Object key)
  ;; int size()
  (size [_]
    (log/trace "PersistentEntityMap.SIZE ")
    (.size (.getProperties content)))
  ;; Collection<V> values()
  (values [_]
    (log/trace "PersistentEntityMap.size")
    (.values (.getProperties content)))
  ;;j8 default V compute(K key, BiFunction<? super K,? super V,? extends V> remappingFunction)
  ;;j8 default V computeIfAbsent(K key, Function<? super K,? extends V> mappingFunction)
  ;;j8 default V computeIfPresent(K key, BiFunction<? super K,? super V,? extends V> remappingFunction)
  ;;j8 default void forEach(BiConsumer<? super K,? super V> action)
  ;;j8 default V getOrDefault(Object key, V defaultValue)
  ;;j8 default V merge(K key, V value, BiFunction<? super V,? super V,? extends V> remappingFunction)
  ;;j8 default V putIfAbsent(K key, V value)
  ;;j8 default boolean remove(Object key, Object value)
  ;;j8 default V replace(K key, V value)
  ;;j8 default boolean replace(K key, V oldValue, V newValue)
  ;;j8 default void replaceAll(BiFunction<? super K,? super V,? extends V> function)

  ;;Protocol:
  java.io.Serializable
  ;;   no methods or fields; this class only serves to "identify the semantics of being serializable"

  ;;Protocol:
  clojure.lang.IFn
  ;; invoke, applyTo
  (invoke [this k]  ; -> Object
    {:pre [(keyword? k)]}
    ;; (log/trace "PersistentEntityMap.invoke(" k ")")
    (if (= k :migae/keychain)
      (get-keychain (.getKey content))
      (let [kw (subs (str k) 1)
            prop (.getProperty (.content this) kw)]
        ;; (log/trace "Property: " k " : " prop)
        (if (not (nil? prop))
          (ds-to-clj prop)
          nil))))
  ;; Object applyTo(ISeq arglist) ;
  (applyTo [_ ^ISeq arglist]  ; -> Object
    (log/trace "IFn.applyTo"))

  ;;Protocol:
  clojure.lang.IObj ;; extends IMeta
  ;; withMeta; meta
  (^IPersistentMap meta [this]
    (do
      ;; (log/trace "PersistentEntityMap.meta on: " (type (.content this)))
      (if (instance? EmbeddedEntity (.content this))
        ;; FIXME: handle case where embedded entity has key
        {}
        (let [md (into (some identity [em-meta {}])
                       {:migae/keychain (get-keychain (.getKey (.content this)))})]
          ;; (log/trace "meta: " md)
          md))))
  ;;;; extends IMeta
  (^IObj withMeta [this ^IPersistentMap md]
    (do
      ;; (log/trace "IObj withMeta" md)
      (let [em (PersistentEntityMap. (.clone content) md)]
        ;; (log/trace "entity with meta" em)
        em)))

  ;;Protocol:
  clojure.lang.IPersistentMap
  ;; extends Iterable, Associative, Counted
  ;; assoc, assocEx, without; containsKey, entryAt; valAt; cons, count, empty, equiv; seq
  ;; extends java.lang.Iterable
  (iterator [this]
    ;; (log/trace "Iterable iterator" (.content this))
    (let [props (.getProperties content) ;; java.util.Map<java.lang.String,java.lang.Object>
          entry-set (.entrySet props)
          e-iter (.iterator entry-set)
          ;; em-iter (->PersistentEntityMapSeq e-iter)
          ]
      ;; (log/trace "Iterable res:" em-iter)
      ;; em-iter))
      e-iter))

  (assoc [this k v] ; -> IPersistentMap
    (do
      (log/trace "PersistentEntityMap.assoc")
      (let [prop (subs (str k) 1)]
        (log/trace "IPersistentMap assoc: " k v "(" prop v ")")
        ;; (.setProperty content prop v)
        ;; this))
        (let [to-props (.getProperties content)
              to-coll (into {} (for [[k v] to-props]
                                 (let [prop (keyword k)
                                       val (ds-to-clj v)]
                                   {prop val})))
              key-chain (get-keychain (.getKey content))
              res (assoc to-coll k v)]
          (log/trace "IPersistentMap assoc res: " res)
          (with-meta res {:migae/keychain key-chain})))))
  (assocEx [_ k v]
    (do
      (log/trace "assocEx")
      (PersistentEntityMap. (.assocEx content k v) nil)))
  (without [this k]                     ; = dissoc!, return new datum with k removed
    (do
      (log/trace "PersistentEntityMap.without")
      (let [prop (name k)]
        (log/trace "without: removing prop " k "->" prop)
        (.removeProperty content prop)
        this)))

  ;; IPersistentMap extends Counted
  (count [_]  ; -> int
    "number of properties in container, plus one for the keychain"
    (do
      (log/trace "PersistentEntityMap.count")
      (let [props (.getProperties content)
            c (.size props)]
        c)))
  ;; IPersistentMap extends Associative extends IPersistentCollection, ILookup
  ;; containsKey, entryAt, assoc
  ;; (containsKey [_ k]
  ;;   (log/trace "PersistentEntityMap.containsKey: " k)
  ;;   (.containsKey (.getProperties content) k))
  (containsKey [_ k] ; -> boolean
    (do
      (log/trace "PersistentEntityMap.containsKey")
      (let [prop (name k)
            r    (.hasProperty content prop)]
        ;; (log/trace "PersistentEntityMap.containsKey: " k r)
        r)))
  (entryAt [this k] ; -> IMapEntry
    (do (log/trace "Associative.entryAt " k)
        (let [val (.getProperty content (name k))
              entry (clojure.lang.MapEntry. k val)]
          ;; (log/trace "entryAt " k val entry)
          entry)))
  ;; (assoc) -> Associative => overridden at IPersistentMap

  ;;;; extends ILookup
  ;; valAt(Object key), valAt(Object key, Object notFound)
  (valAt [_ k]  ; -> Object
    (log/trace "PersistentEntityMap.ILookup.valAt: " k)
    (if (= k :migae/keychain)
      (get-keychain (.getKey content))
      (let [prop (ds-to-clj (subs (str k) 1))]
        (if-let [v  (.getProperty content prop)]
          (do ;;(log/trace "prop:" prop ", val:" v)
            (ds-to-clj v))
          nil))))
  (valAt [_ k not-found]  ; -> Object
    (log/trace "PersistentEntityMap.ILookup.valAt w/notfound: " k)
    ;; FIXME: is k a keyword or a string?
    (.getProperty content (str k) not-found))

  ;;;;  extends IPersistentCollection extends Seqable
  ;; cons(Object o), count(), empty(), equiv(Object o);
  (cons [this o] ; -> IPersistentCollection
    ;; this is called on:  (into em {:a }); not called on (into em1 em2)
    ;; (log/trace "PersistentEntityMap.cons: " o (type o))
    ;; (log/trace "IPersistentCollection this: " this (type this))
    (cond
     (= (type o) PersistentEntityMap)
     (do
       ;; (log/trace "cons PersistentEntityMap to this")
       ;; (log/trace "this:" this)
       ;; (log/trace "that:" o)
       (let [props (.getProperties (.content o))
             newe (.clone content)]
         (.setPropertiesFrom newe content) ;; is this needed, or is clone enough?
         (doseq [[k v] props]
           (.setProperty newe k v))
         (PersistentEntityMap. newe em-meta)))
     ;; (= (type o) java.util.Collections$UnmodifiableMap$UnmodifiableEntrySet$UnmodifiableEntry)
     (nil? o)
     (do
       (log/trace "nil object")
       this)
     (= (type o) clojure.lang.MapEntry)
     (do
       (log/trace "cons clojure.lang.MapEntry" o " to emap" this)
       (let [new-entity (.clone content)]
         (.setProperty new-entity (subs (str (first o)) 1) (clj-to-ds (second o)))
         (PersistentEntityMap. new-entity nil)))

     (= (type o) clojure.lang.PersistentArrayMap)
     (do
       (log/trace "cons PersistentArrayMap to emap")
       (let [newe (.clone content)
             newm (into {} em-meta)]
         (doseq [[k v] o]
           (.setProperty newe (subs (str k) 1) (clj-to-ds v)))
         ;; (.put (datastore) content)
         (PersistentEntityMap. newe newm)))

     (= (type o) java.util.Map$Entry)
     (do
       (log/trace "cons java.util.Map$Entry to emap")
       (.setProperty content (.getKey o) (.getValue o))
       ;; (.put (datastore) content)
       this)

     (= (type o) java.util.Collections$UnmodifiableMap$UnmodifiableEntrySet$UnmodifiableEntry)
     (do
       (.setProperty content (.getKey o) (.getValue o))
       ;; (.put (datastore) content)
       this)

     :else (log/trace "cons HELP?" o (type o))))
  ;;int count();
  ;; (count  overridden by Counted
  (empty [this]  ; -> IPersistentCollection
    (log/trace "PersistentEntityMap.empty")
    (let [k (.getKey (.content this))
          e (Entity. k)]
      (PersistentEntityMap. e nil)))
  (equiv [this o]  ; -> boolean
    ;; FIXME: double-check the logic
    (cond
     (instance? PersistentEntityMap o) ;; migae.datastore.I
     (do
       ;; (log/trace "comparing PersistentEntityMap")
       (if (.equals content (.content o))
         (do ;; the entities are key=
           ;; (log/trace "(.equals content (.content o)) is true: key=")
           ;; (log/trace "content:" content)
           ;; (log/trace "(.content o):" (.content o))
           (let [this-map (.getProperties content)
                 that-map (.getProperties (.content o))]
             ;; (log/trace "this-map:" this-map)
             ;; (log/trace "that-map:" that-map)
             ;; (log/trace "(= this-map that-map)" (= this-map that-map))
             (= this-map that-map)))
         (do ;; (log/trace "entities have different keys")
           false)))
     (instance? clojure.lang.PersistentArrayMap o)
     (do ;; (log/trace "comparing " (type o))
       (let [props (.getProperties content)
             pmap (into {}
                        (for [[k v] props]
                          (do
                            (let [prop (keyword k)
                                  val (ds-to-clj v)]
                              ;; (log/trace "prop " prop " val " val)
                              {prop val}))))]
         (= pmap o)))
     :else (throw (RuntimeException. "PersistentEntityMap.equiv " (type o)))))

  ;;;;;;;;;;;;;;;; extends Seqable
  (^ISeq seq [this]
    ;; seq is called by: into, merge, printing stuff (prn, println, (log/trace em), etc)
    ;; also called by reduce
    ;; (log/trace "PersistentEntityMap.seq:" (type this)) ;; (.hashCode this) (.getKey content))
    ;; (log/trace (str "seq on (.content this)" (.content this)))
    (let [props (.getProperties (.content this))
          ;; foo (println "props" props)
          emap (into {}
                     (for [[k v] props]
                       (do
                         (let [prop (keyword k)
                                val (if (= (type v) EmbeddedEntity)
                                      (embedded-entity->map v)
                                      (ds-to-clj v))]
                               ;; val (ds-to-clj v)]
                           ;; (println "prop " prop " val " val)
                           {prop val}))))]
          ;; _ (println "EMAP: " emap)
          ;; _ (println "KEY: " (.getKey (.content this)))
      (if (instance? EmbeddedEntity (.content this))
        (seq emap)
        (let [k (get-keychain (.getKey (.content this)))
          ;; _ (println "K: " k (type k))
              res (with-meta emap {:migae/keychain k})
          ;; _ (println "RES: " res)
          ;; res (into r {:migae/keychain k})
              ]
      ;; (log/trace "seq result:" #_(meta res) (type res) res)
      ;; (log/trace "result as seq:" (seq res))
          (seq res)))))
  ;;      this))

  ;;Protocol:
  clojure.lang.IKVReduce
  ;; support clojure.core/reduce-kv
  ;; Object kvreduce(IFn f, Object init);
  (kvreduce [this ^IFn f ^Object seed]
    (println "KVREDUCE")
    )

  ;;;;;;;;;;;;; clojure.lang.IReduce  ; extends IReduceInit
  ;; FIXME: do we need to implement IReduce if we implement seq?
  ;; support clojure.core/reduce
  #_(reduce [this ^IFn f]  ; -> Object
    (throw (RuntimeException. "PersistentEntityMap.reduce" f)))
  #_(reduce [this ^IFn f ^Object seed]  ; -> Object
    ;; called by "print" stuff
    (log/trace "PersistentEntityMap.reduce func:" f)
    (log/trace "PersistentEntityMap.reduce seed:" (.getSimpleName (class seed)))
    (cond

     ;; FIXME
     ;; (store-map? seed)
     ;; (do
     ;;   (let [ds (.content seed)]
     ;;     ;; (log/trace "PersistentEntityMap.reduce ds: " ds)
     ;;     (.put ds content)
     ;;     seed))

     (= (class seed) com.google.appengine.api.datastore.DatastoreServiceImpl)
     (do
       (.put seed content)
       seed)

     (= (class seed) clojure.lang.PersistentArrayMap)
     (do
       (log/trace "seed:  PersistentArrayMap")
       )

     (= (class seed) clojure.lang.PersistentVector)
     (do
       (log/trace "seed:  clojure.lang.PersistentVector")
       )

     (= (class seed) PersistentEntityMap)
     (do
       ;; (log/trace "seed:  PersistentEntityMap")
       (let [k (.getKey content)
             e (Entity. k)]
         (.setPropertiesFrom e (.content seed))
         (.setPropertiesFrom e content)
         (PersistentEntityMap. e nil)))
     ;; f = cons, so we can just use the native into
     ;; (let [from-props (.getProperties content)
     ;;       from-coll (into {} (for [[k v] from-props]
     ;;                                (let [prop (keyword k)
     ;;                                      val (ds-to-clj v)]
     ;;                                  {prop val})))
     ;;       foo (.setPropertiesFrom (.content seed) (.content this))
     ;;       to-props (.getProperties (.content seed))
     ;;       to-coll (into {} (for [[k v] to-props]
     ;;                              (let [prop (keyword k)
     ;;                                    val (ds-to-clj v)]
     ;;                                {prop val})))
     ;;       res (with-meta (into to-coll from-coll)
     ;;             {:migae/keychain (:migae/key (meta seed))
     ;;              :type PersistentEntityMap})]
     ;;   ;; (log/trace "to-coll: " res (type res))
     ;;   seed)
     (= (class seed) clojure.lang.PersistentArrayMap$TransientArrayMap)
     (do
       (log/trace "seed:  PersistentArrayMap$TransientArrayMap")
       ;; we use a ghastly hack in order to retain metadata
       ;; FIXME: handle case where seed is a clj-emap (map with ^:PersistentEntityMap metadata)
       (let [from-props (.getProperties content)
             from-coll (into {} (for [[k v] from-props]
                                  (let [prop (keyword k)
                                        val (ds-to-clj v)]
                                    {prop val})))
             to-ent (Entity. (.getKey content))
             ;; to-coll (into {} (for [[k v] to-props]
             ;;                        (let [prop (keyword k)
             ;;                              val (ds-to-clj v)]
             ;;                          {prop val})))
             to-keychain (if (nil? (:migae/keychain seed))
                           (get-keychain (.getKey content))
                           (:migae/keychain seed))]
         (doseq [[k v] from-coll]
           (assoc! seed k v))
         (let [p (persistent! seed)]
           (doseq [[k v] p]
             (.setProperty to-ent (subs (str k) 1) (clj-to-ds v))))
         ;; (let [m1 (persistent! seed)
         ;;       m2 (with-meta m1 {:migae/keychain keychain
         ;;                         :type PersistentEntityMap})
         ;;       to-coll (transient m2)]
         ;;   (log/trace "m2: " (meta m2) m2 (class m2))
         ;;   (log/trace "to-coll: " (meta to-coll) to-coll (class to-coll))
         (PersistentEntityMap. to-ent nil)))
     :else (log/trace "PersistentEntityMap.reduce HELP" (class seed)))
    )

  ;;;;;;;;;; clojure.lang.IReference ; extends IMeta; required to support metadata reader syntax?
  ;; IPersistentMap alterMeta(IFn alter, ISeq args) ;
  ;; (^IPersistentMap alterMeta [this, ^IFn alter, ^ISeq args]
  ;;   (log/trace "IReference.alterMeta")
  ;;   {:foo 3})
  ;; ;; IPersistentMap resetMeta(IPersistentMap m);
  ;; (^IPersistentMap resetMeta [this ^IPersistentMap m]
  ;;   (log/trace "IReference.resetMeta")
  ;;   {:bar 3})

  ;; FIXME: make result of (into {} em) support Map$Entry so it behaves like an em
  ;; this doesn't work since clojure.lang.PersistentArrayMap cannot be cast to java.util.Map$Entry
  ;; NB:  IMapEntry extends java.util.Map$Entry
  ;; can we use defprotocol and extend to do this?

  ;;Protocol:
  clojure.lang.ITransientCollection
  ;; FIXME: do we need this?
  ;; conj, persistent
  (conj [this args]  ; -> ITransientCollection
    (log/trace "ITransientCollection conj")
    (let [item (first args)
          k (name (key item))
          v (val item)
          val (if (number? v) v
                  (if (string? v) v
                      (edn/read-string v)))]
      ;; (log/trace "ITransientCollection conj: " args item k v)
      (.setProperty content k v)
      this))
  (persistent [this]  ; -> IPersistentCollection
    (log/trace "ITransientCollection persistent")
    ;; (try (/ 1 0) (catch Exception e (print-stack-trace e)))
    (let [props (.getProperties content)
          kch (get-keychain (.getKey content))
          coll (into {} (for [[k v] props]
                          (let [prop (keyword k)
                                val (ds-to-clj v)]
                            {prop val})))
          res (with-meta coll {:migae/keychain kch
                               :type PersistentEntityMap})]
      (log/trace "persistent result: " res (class res))
      res))


  ;; clojure.lang.ITransientMap
  ;; (assoc [this k v]                     ; both assoc! and assoc (?)
  ;;   (let [prop (name k)]
  ;;     (log/trace "ITransientMap assoc: setting prop " k "->" prop v)
  ;;     (.setProperty content prop v)
  ;;     this))
  ;; (without [this k]                     ; = dissoc!, return new datum with k removed
  ;;   (let [prop (name k)]
  ;;     (log/trace "without: removing prop " k "->" prop)
  ;;     (.removeProperty content prop)
  ;;     this))
  ;; (persistent [this]                    ; persistent!
  ;;     (log/trace "ITransientMap persistent")
  ;;   )

  ;;;;;;;;; clojure.lang.Indexed extends Counted
  ;; FIXME: do we need this if we support Seqable?
  ;; (count [this]                         ; Counted
  ;;   (log/trace "count"))
  #_(nth [this i]                         ; called by get(int index)
    (log/trace "nth"))
  ;;    (next em-iter)) ;; HACK
  #_(nth [this i not-found]
    )

  ;;Protocol:
  clojure.lang.IHashEq
  (^int hasheq [this]
    (clojure.lang.Murmur3/hashUnordered this))

  ;; clojure.lang.IMapEntry
  ;; key, val
  ;; (key [this]  ; -> Object
  ;;   (log/trace "PersistentEntityMap.IMapEntry key")
  ;;   (get-keychain (.getKey content)))
  ;; (val [this]  ; -> Object
  ;;   (log/trace "PersistentEntityMap.IMapEntry val")
  ;;   (.printStackTrace (Exception.))
  ;;   (let [props (.getProperties content)
  ;;         coll (into {} (for [[k v] props]
  ;;                             (let [prop (keyword k)
  ;;                                   val (ds-to-clj v)]
  ;;                               {prop val})))
  ;;         keychain (get-keychain (.getKey content))
  ;;         ;; res (into coll {:migae/keychain keychain})]
  ;;         ]
  ;;     coll))

  ) ;; end deftype PersistentEntityMap

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(in-ns 'migae.datastore.types.entity-map)

(clojure.core/println "Done loading migae.datastore.types.entity_map")
