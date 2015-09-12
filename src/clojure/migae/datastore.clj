(ns migae.datastore
  (:refer-clojure :exclude [get into print println print-str pr-str reduce])
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
           ;; [clojure.lang MapEntry]
           [com.google.appengine.api.blobstore BlobKey]
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
           [clojure.lang IFn ILookup IMapEntry IObj
            IPersistentCollection IPersistentMap IReduce IReference ISeq ITransientCollection]
           )
  (:require [clojure.core :refer :all]
            [clojure.walk :as walk]
            [clojure.stacktrace :refer [print-stack-trace]]
            [clojure.tools.reader.edn :as edn]
            [migae.datastore.service :as ds]
            ;; [migae.datastore.keychain :as ekey]
            ;; [migae.datastore.ctor-push :as push]
;;            [migae.datastore.adapter :refer :all]
            ;; [migae.datastore.entity-map :as emap]
            ;; [migae.datastore.key :as dskey]
            ;; [migae.datastore.query :as dsqry]
            [migae.infix :as infix]
            [clojure.tools.logging :as log :only [trace debug info]]))

;; (declare make-embedded-entity)

(defn- get-next-emap-prop [this]
  ;; (log/trace "get-next-emap-prop" (.query this))
  (let [r (.next (.query this))]
    ;; (log/trace "next: " r)
    r))

;; (load "datastore/keychain")

(declare ->PersistentEntityMapSeq)
(declare ->PersistentEntityMap)

(declare get-val-clj get-val-ds)
(declare make-embedded-entity)
(declare props-to-map get-next-emap-prop)

(declare keychain? keylink?)
(declare keychain keychain-to-key proper-keychain? improper-keychain?)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;  PersistentEntityMap
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(deftype PersistentEntityMap [content ^IPersistentMap em-meta]

  migae.datastore.IPersistentEntityMap
  ;; java.lang.Iterable
  (iterator [this]
    (log/trace "Iterable iterator" (.content this))
    (let [props (.getProperties content) ;; java.util.Map<java.lang.String,java.lang.Object>
          entry-set (.entrySet props)
          e-iter (.iterator entry-set)
          em-iter (->PersistentEntityMapSeq e-iter) ]
    ;; (log/trace "Iterable res:" em-iter)
    em-iter))

  ;; ;; java.util.Map$Entry
  ;; (getKey [this]
  ;;   (log/trace "PersistentEntityMap.java.util.Map$Entry getKey ")
  ;;   (let [k (.getKey content)]
  ;;     (log/trace "key:" k)
  ;;     (keychain k)))
  ;; ;; FIXME: just do (keychain content)??
  ;;     ;;     kind (.getKind k)
  ;;     ;;     nm (.getName k)]
  ;;     ;; [(keyword kind (if nm nm (.getId k)))]))
  ;; (getValue [_]
  ;;   (log/trace "PersistentEntityMap.java.util.Map$Entry getValue ")
  ;;   (let [r (props-to-map (.getProperties content))]
  ;;     (log/trace "r: " r)
  ;;     r))
  ;; ;; (equals [_ o]
  ;; ;;   (log/trace "PersistentEntityMap.java.util.Map$Entry equals ")
  ;; ;;   )
  ;; ;; (hashCode [_]
  ;; ;;   (log/trace "PersistentEntityMap.java.util.Map$Entry hashCode ")
  ;; ;;   )
  ;; (setValue [_ v]
  ;;   (log/trace "PersistentEntityMap.java.util.Map$Entry setValue ")
  ;;   )

  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;; clojure.lang.IFn
  ;; invoke, applyTo
  (invoke [this k]  ; -> Object
    {:pre [(keyword? k)]}
    (log/trace "IFn.invoke(" k ")")
    (if (= k :migae/keychain)
      (keychain content)
      (let [kw (subs (str k) 1)
            prop (.getProperty content kw)]
        (if (not (nil? prop))
          (get-val-clj prop)
          nil))))
  ;; Object applyTo(ISeq arglist) ;
  (applyTo [_ ^ISeq arglist]  ; -> Object
    (log/trace "IFn.applyTo"))

  ;; clojure.lang.IMapEntry
  ;; key, val
  ;; (key [this]  ; -> Object
  ;;   (log/trace "PersistentEntityMap.IMapEntry key")
  ;;   (keychain content))
  ;; (val [this]  ; -> Object
  ;;   (log/trace "PersistentEntityMap.IMapEntry val")
  ;;   (.printStackTrace (Exception.))
  ;;   (let [props (.getProperties content)
  ;;         coll (into {} (for [[k v] props]
  ;;                             (let [prop (keyword k)
  ;;                                   val (get-val-clj v)]
  ;;                               {prop val})))
  ;;         keychain (keychain content)
  ;;         ;; res (into coll {:migae/keychain keychain})]
  ;;         ]
  ;;     coll))

  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;; clojure.lang.IObj ;; extends IMeta
  ;; withMeta; meta
  (^IPersistentMap meta [this]
    ;; (log/trace "IObj.meta" (keychain content))
    (into (some identity [em-meta {}]) {:migae/keychain (keychain content)}))
  ;;;; extends IMeta
  (^IObj withMeta [this ^IPersistentMap md]
    (log/trace "IObj withMeta" md)
    (let [em (PersistentEntityMap. (.clone content) md)]
      ;; (log/trace "entity with meta" em)
      em))

  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;; clojure.lang.IPersistentMap ; extends Iterable, Associative, Counted
  ;; assoc, assocEx, without; containsKey, entryAt; valAt; cons, count, empty, equiv; seq
  (assoc [this k v] ; -> IPersistentMap
    (let [prop (subs (str k) 1)]
      (log/trace "IPersistentMap assoc: " k v "(" prop v ")")
      ;; (.setProperty content prop v)
      ;; this))
      (let [to-props (.getProperties content)
            to-coll (into {} (for [[k v] to-props]
                                     (let [prop (keyword k)
                                           val (get-val-clj v)]
                                       {prop val})))
            key-chain (keychain this)
            res (assoc to-coll k v)]
      (log/trace "IPersistentMap assoc res: " res)
      (with-meta res {:migae/keychain key-chain}))))
  (assocEx [_ k v]
    (log/trace "assocEx")
    (PersistentEntityMap. (.assocEx content k v) nil))
  (without [this k]                     ; = dissoc!, return new datum with k removed
    (let [prop (name k)]
      (log/trace "without: removing prop " k "->" prop)
      (.removeProperty content prop)
      this))

  ;;;; extends Counted
  (count [_]  ; -> int
    "number of properties in container, plus one for the keychain"
    (log/trace "PersistentEntityMap.count")
    (let [props (.getProperties content)
          c (.size props)]
      c))
  ;;;; extends Associative extends IPersistentCollection, ILookup
  ;; containsKey, entryAt, assoc
  (containsKey [_ k] ; -> boolean
    (do (log/trace "Associative.containsKey " k)
    (let [prop (name k)
          r    (.hasProperty content prop)]
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
      (keychain content)
      (let [prop (get-val-clj (subs (str k) 1))]
        (if-let [v  (.getProperty content prop)]
          (do ;;(log/trace "prop:" prop ", val:" v)
              (get-val-clj v))
          nil))))
  (valAt [_ k not-found]  ; -> Object
    (log/trace "PersistentEntityMap.ILookup.valAt w/notfound: " k)
    ;; FIXME: is k a keyword or a string?
    (.getProperty content (str k) not-found))

  ;;;;  extends IPersistentCollection extends Seqable
  ;; cons(Object o), count(), empty(), equiv(Object o);
  (cons [this o] ; -> IPersistentCollection
    ;; this is called on:  (into em {:a }); not called on (into em1 em2)
    (log/trace "PersistentEntityMap.cons: " o (type o))
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
          (.setProperty new-entity (subs (str (first o)) 1) (get-val-ds (second o)))
          (PersistentEntityMap. new-entity nil)))
      (= (type o) clojure.lang.PersistentArrayMap)
      (do
        (log/trace "cons PersistentArrayMap to emap")
        (let [newe (.clone content)
              newm (into {} em-meta)]
          (doseq [[k v] o]
            (.setProperty newe (subs (str k) 1) (get-val-ds v)))
          ;; (.put (datastore) content)
          (PersistentEntityMap. newe newm)))
      (= (type o) java.util.Map$Entry)
      (do
        (log/trace "cons java.util.Map$Entry to emap")
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
    ;; (log/trace "PersistentEntityMap.equiv")
    (if (instance? migae.datastore.IPersistentEntityMap o)
      true
      ;; FIXME:  implement PropertyContainer to map logic as a function
      (let [props (.getProperties content)
            pmap (into {}
                           (for [[k v] props]
                             (do
                               (let [prop (keyword k)
                                     val (get-val-clj v)]
                                 ;; (log/trace "prop " prop " val " val)
                                 {prop val}))))]
        (= pmap o))))
    ;; (.equals this o))
    ;; (.equals content (.content o)))

  ;;;; extends Seqable
  ;; seq()
  (^ISeq seq [this]
    ;; seq is called by: into, merge, "print", e.g. (log/trace em)
    (log/trace "PersistentEntityMap.seq: " (type this)) ;; (.hashCode this) (.getKey content))
    ;; (log/trace "    on content" content)
    (let [props (.getProperties content)
          ;; foo (log/trace "props" props)
          emap (into {}
                           (for [[k v] props]
                             (do
                               (let [prop (keyword k)
                                     val (get-val-clj v)]
                                 ;; (log/trace "prop " prop " val " val)
                                 {prop val}))))
          k (keychain content)
          res (with-meta emap {:migae/keychain k})
          ;; res (into r {:migae/keychain k})
          ]
      ;; (log/trace "seq result:" #_(meta res) (type res) res)
      (seq res)))
  ;;      this))

  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;; clojure.lang.IReduce  ; extends IReduceInit
  (reduce [this ^IFn f]  ; -> Object
    (log/trace "HELP! reduce") (flush)
    this)
  (reduce [this ^IFn f ^Object to-map]  ; -> Object
    ;; called by "print" stuff
    (log/trace "IReduce.reduce:" (class to-map) (type to-map))
    (cond
      (= (class to-map) clojure.lang.PersistentArrayMap)
      (do
        (log/trace "to-map:  PersistentArrayMap")
        )
      (= (class to-map) clojure.lang.PersistentVector)
      (do
        (log/trace "to-map:  clojure.lang.PersistentVector")
        )
      (= (class to-map) PersistentEntityMap)
      (do
        ;; (log/trace "to-map:  PersistentEntityMap")
        (let [k (.getKey content)
              e (Entity. k)]
          (.setPropertiesFrom e (.content to-map))
          (.setPropertiesFrom e content)
          (PersistentEntityMap. e nil)))
      ;; f = cons, so we can just use the native into
      ;; (let [from-props (.getProperties content)
      ;;       from-coll (into {} (for [[k v] from-props]
      ;;                                (let [prop (keyword k)
      ;;                                      val (get-val-clj v)]
      ;;                                  {prop val})))
      ;;       foo (.setPropertiesFrom (.content to-map) (.content this))
      ;;       to-props (.getProperties (.content to-map))
      ;;       to-coll (into {} (for [[k v] to-props]
      ;;                              (let [prop (keyword k)
      ;;                                    val (get-val-clj v)]
      ;;                                {prop val})))
      ;;       res (with-meta (into to-coll from-coll)
      ;;             {:migae/keychain (:migae/key (meta to-map))
      ;;              :type PersistentEntityMap})]
      ;;   ;; (log/trace "to-coll: " res (type res))
      ;;   to-map)
      (= (class to-map) clojure.lang.PersistentArrayMap$TransientArrayMap)
      (do
        (log/trace "to-map:  PersistentArrayMap$TransientArrayMap")
        ;; we use a ghastly hack in order to retain metadata
        ;; FIXME: handle case where to-map is a clj-emap (map with ^:PersistentEntityMap metadata)
        (let [from-props (.getProperties content)
              from-coll (into {} (for [[k v] from-props]
                                       (let [prop (keyword k)
                                             val (get-val-clj v)]
                                         {prop val})))
              to-ent (Entity. (.getKey content))
              ;; to-coll (into {} (for [[k v] to-props]
              ;;                        (let [prop (keyword k)
              ;;                              val (get-val-clj v)]
              ;;                          {prop val})))
              to-keychain (if (nil? (:migae/keychain to-map))
                            (keychain content)
                            (:migae/keychain to-map))]
          (doseq [[k v] from-coll]
            (assoc! to-map k v))
          (let [p (persistent! to-map)]
            (doseq [[k v] p]
              (.setProperty to-ent (subs (str k) 1) (get-val-ds v))))
          ;; (let [m1 (persistent! to-map)
          ;;       m2 (with-meta m1 {:migae/keychain keychain
          ;;                         :type PersistentEntityMap})
          ;;       to-coll (transient m2)]
          ;;   (log/trace "m2: " (meta m2) m2 (class m2))
          ;;   (log/trace "to-coll: " (meta to-coll) to-coll (class to-coll))
          (PersistentEntityMap. to-ent nil)))
      :else (log/trace "HELP! reduce!" (class to-map)))
      )

  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;; clojure.lang.IReference ; extends IMeta; required to support metadata reader syntax?
    ;; IPersistentMap alterMeta(IFn alter, ISeq args) ;
  (^IPersistentMap alterMeta [this, ^IFn alter, ^ISeq args]
    (log/trace "IReference.alterMeta")
    {:foo 3})
    ;; IPersistentMap resetMeta(IPersistentMap m);
  (^IPersistentMap resetMeta [this ^IPersistentMap m]
    (log/trace "IReference.resetMeta")
    {:bar 3})

  ;; FIXME: make result of (into {} em) support Map$Entry so it behaves like an em
  ;; this doesn't work since clojure.lang.PersistentArrayMap cannot be cast to java.util.Map$Entry
  ;; NB:  IMapEntry extends java.util.Map$Entry
  ;; can we use defprotocol and extend to do this?

  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;; clojure.lang.ITransientCollection
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
          kch (keychain content)
          coll (into {} (for [[k v] props]
                              (let [prop (keyword k)
                                    val (get-val-clj v)]
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

  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;; clojure.lang.Indexed                  ; extends Counted
  ;; (count [this]                         ; Counted
  ;;   (log/trace "count"))
  (nth [this i]                         ; called by get(int index)
    (log/trace "nth"))
;;    (next em-iter)) ;; HACK
  (nth [this i not-found]
    )
  ) ;; end deftype PersistentEntityMap

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(deftype PersistentEntityMapCollIterator [query]
  java.util.Iterator
  (hasNext [this]
    (log/trace "PersistentEntityMapCollIterator hasNext")
    (.hasNext query))
  (next    [this]
    (log/trace "PersistentEntityMapCollIterator next")
    (PersistentEntityMap. (.next query) nil))
  ;; (remove  [this])
  )

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(deftype PersistentEntityMapSeq [query]

  migae.datastore.IPersistentEntityMapSeq
  ;; clojure.lang.ISeq ;; < IPersistentCollection (< Seqable)
  (^Object
    first [_]
    (log/trace "PersistentEntityMapSeq.ISeq.first of" (type query))
    (let [r  (first query)
          rm (->PersistentEntityMap r nil)]
      ;; rm (migae.datastore.PersistentEntityMap. r nil)]
      ;; (log/trace "rm:" rm)
      rm))
  (^ISeq
    next [_]
    (log/trace "ISeq next")
    (let [res (next query)]
      (if (nil? res)
        nil
        (->PersistentEntityMapSeq res))))
  (^ISeq
    more [_]  ;;  same as next?
    (log/trace "PersistentEntityMapSeq.ISeq.more")
    (let [res (next query)]
      (if (nil? res)
        nil
        (->PersistentEntityMapSeq res))))
  (^ISeq ;;^clojure.lang.IPersistentVector
    cons  ;; -> ^ISeq ;;
    [this ^Object obj]
    (log/trace "ISeq cons"))

  ;;;; Seqable interface
  (^ISeq
    seq [this]  ; specified in Seqable
     (log/trace "PersistentEntityMapSeq.ISeq.seq")
    this)

  ;;;; IPersistentCollection interface
  (^int
    count [_]
    ;; (log/trace "PersistentEntityMapSeq.count")
    (count query))
  ;; cons - overridden by ISeq
  (^IPersistentCollection
    empty [_]
    (log/trace "PersistentEntityMapSeq.empty"))
  (^boolean
    equiv [_ ^Object obj]
    (log/trace "PersistentEntityMapSeq.equiv"))

  ;; clojure.lang.IndexedSeq extends ISeq, Sequential, Counted{
  ;;public int index();

  ;; clojure.lang.Indexed                  ; extends Counted
  ;; (count [this]                         ; Counted
  ;;   (log/trace "PersistentEntityMapSeq.clojure.lang.Indexed.count")
  ;;   (count query))
  (nth [this i]                         ; called by get(int index)
    (log/trace "Indexed nth" i))
  ;; (next em-iter)) ;; HACK
  (nth [this i not-found]
    (log/trace "Indexed nth with not-found" i))
  )

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- keyword-to-ds
  [kw]
   (KeyFactory/createKey "Keyword"
                         ;; remove leading ':'
                         (subs (str kw) 1)))

(defn- symbol-to-ds
  [sym]
   (KeyFactory/createKey "Symbol" (str sym)))

(defn- get-val-clj-coll
  "Type conversion: java to clojure"
  [coll]
  ;; (log/trace "get-val-clj-coll" coll (type coll))
  (cond
    (= (type coll) java.util.ArrayList) (into '() (for [item coll]
                                                       (get-val-clj item)))
    (= (type coll) java.util.HashSet)  (into #{} (for [item coll]
                                                       (get-val-clj item)))
    (= (type coll) java.util.Vector)  (into [] (for [item coll]
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
                  (= (class v) java.lang.Double) v
                  (= (class v) java.lang.Boolean) v
                  (= (class v) java.util.Date) v
                  (instance? java.util.Collection v) (get-val-clj-coll v)
                  (= (type v) Link) (.toString v)
                  (= (type v) Email) (.getEmail v)
                  (= (type v) EmbeddedEntity) (->PersistentEntityMap v nil)
                  (= (type v) Key) (let [kind (.getKind v)]
                                     (if (= kind "Keyword")
                                       (keyword (.getName v))
                                       ;; (symbol (.getName v))))
                                       (str \' (.getName v))))
                  :else (do
                          ;; (log/trace "HELP: get-val-clj else " v (type v))
                          (throw (RuntimeException. (str "HELP: get-val-clj else " v (type v))))
                          (edn/read-string v)))]
    ;; (log/trace "get-val-clj result:" v val)
    val))

;; this is for values to be stored (i.e. from clojure to ds java types)
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

(defn- props-to-map
  [props]
  (into {} (for [[k v] props]
                 (let [prop (keyword k)
                       val (get-val-clj v)]
                   {prop val}))))

(defn- make-embedded-entity
  [m]
  {:pre [(map? m)]}
  (let [embed (EmbeddedEntity.)]
    (doseq [[k v] m]
      ;; FIXME:  (if (map? v) then recur
      (.setProperty embed (subs (str k) 1) (get-val-ds v)))
    embed))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;  predicates
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn entity-map?
  [em]
  (= (instance? migae.datastore.IPersistentEntityMap em)))

(defn emap? ;; OBSOLETE - use entity-map?
  [em]
  (entity-map? em))

(defn entity?
  [e]
  (= (type e) Entity))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;   PUSH ctors
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(declare into-ds!)

(defn entity-map!
  ([keychain]
   (into-ds! keychain))
  ([keychain em]
   {:pre [(map? em)
          (vector? keychain)
          (not (empty? keychain))
          (every? keyword? keychain)]}
   (into-ds! keychain em))
  ([force keychain em]
   {:pre [(or (map? em) (vector? em))
          (vector? keychain)
          (not (empty? keychain))
          (every? keyword? keychain)]}
   (into-ds! force keychain em)))

(defn put-kinded-emap
  [keychain & data]
  ;; precon: improper keychain is validated
  ;; (log/trace "keychain" keychain)
  ;; (log/trace "data" data)
  (let [em (first data)
        kind (name (last keychain))
        parent-keychain (vec (butlast keychain))
        ;; foo (log/trace "foo" parent-keychain)
        e (if (empty? parent-keychain)
            (Entity. kind)
            (do #_(log/trace "parent-keychain" parent-keychain)
                #_(log/trace "parent-key" (keychain-to-key parent-keychain))
                (Entity. kind  (keychain-to-key parent-keychain))
                ))]
    (when (not (empty? em))
      (doseq [[k v] (seq em)]
        (.setProperty e (subs (str k) 1) (get-val-ds v))))
    (.put (ds/datastore) e)
    (PersistentEntityMap. e nil)))

(defn- put-proper-emap
  [& {:keys [keychain propmap force] :or {force false,}}]
  ;; precon: keychain has already been validated
  ;; (log/trace "keychain" keychain)
  ;; (log/trace "propmap" propmap)
  ;; (log/trace "force" force)
  (let [k (keychain-to-key keychain)
        e (if (not force)
            (do #_(log/trace "not force")
            (let [ent (try (.get (ds/datastore) k)
                           (catch EntityNotFoundException ex ex))]
              (if (= (type ent) Entity) ;; found
                (throw (RuntimeException. "Key already used"))
                (Entity. k))))
            (do #_(log/trace "force push")
                (Entity. k)))]
    (when (not (empty? propmap))
      (doseq [[k v] propmap]
        (.setProperty e (subs (str k) 1) (get-val-ds v))))
    (.put (ds/datastore) e)
    (PersistentEntityMap. e nil)))

(declare into-ds!)
;; FIXME:  replace emap! with entity-map!
;; TODO: support nil data map, e.g. (entity-map! [:A/B])

(defn into-ds!
  ;; FIXME: convert to [keychain & em]???
  ([arg]
   (cond
     (map? arg)
     (do
       ;; edn, e.g.  (entity-map! ^{:migae [:a/b]} {:a 1})
       (let [k (:migae/keychain (meta arg))]
         ;; (log/debug "EDN KEY: " k " improper?" (improper-keychain? k))
         (cond
           (improper-keychain? k)
           (put-kinded-emap k arg)
           (proper-keychain? k)
           (put-proper-emap :keychain k :propmap arg :force true)
           :else (throw (IllegalArgumentException. (str "INVALID KEYCHAIN!: " k)))))
       )
     (keychain? arg)
     (do
       (cond
         (improper-keychain? arg)
         (put-kinded-emap arg {})
         (proper-keychain? arg)
         (put-proper-emap :keychain arg :propmap {} :force true)
         :else (throw (IllegalArgumentException. (str "Invalid keychain" arg)))))
     :else (throw (IllegalArgumentException.))))
  ([keychain em]
  "Put entity-map to datastore unless already there; use :force true to replace existing"
  (log/trace "into-ds! " keychain em (proper-keychain? keychain))
  ;; (if (empty? keychain)
  ;;   (throw (IllegalArgumentException. "keychain vector must not be empty"))
  (cond
    (improper-keychain? keychain)
    (put-kinded-emap keychain em)
    (proper-keychain? keychain)
    (put-proper-emap :keychain keychain :propmap em)
    :else (throw (IllegalArgumentException. (str "Invalid keychain : " keychain)))))
  ([mode keychain em]
  "Modally put entity-map to datastore.  Modes: :force, :multi"
  ;; (log/trace "force into-ds! " force keychain em)
  ;; (if (empty? keychain)
  ;;   (throw (IllegalArgumentException. "keychain vector must not be empty"))
  ;; (if (not= force :force)
  ;;   (throw (IllegalArgumentException. force)))
  (cond
    (= mode :force)
    (cond
      (improper-keychain? keychain)
      (put-kinded-emap keychain em)
      (proper-keychain? keychain)
      (put-proper-emap :keychain keychain :propmap em :force true)
      :else
      (throw (IllegalArgumentException. (str "Invalid keychain" keychain))))
    (= mode :multi)
    (do
      (log/trace "entity-map! :multi processing...")
      (if (improper-keychain? keychain)
        (if (vector? em)
          (do
            (for [emap em]
              (do
                ;; (log/trace "ctoring em" (print-str emap))
                (entity-map! keychain emap))))
          (throw (IllegalArgumentException. ":multi ctor requires vector of maps")))
        (throw (IllegalArgumentException. ":multi ctor requires improper keychain"))))
    :else
      (throw (IllegalArgumentException. (str "Invalid mode keyword:" force))))
    ))


     ;; (let [k (apply keychain-to-key keychain)
     ;;       e (try (.get (ds/datastore) k)
     ;;              (catch EntityNotFoundException e
     ;;                ;;(log/trace (.getMessage e))
     ;;                e)
     ;;              (catch DatastoreFailureException e
     ;;                ;;(log/trace (.getMessage e))
     ;;                nil)
     ;;              (catch java.lang.IllegalArgumentException e
     ;;                ;;(log/trace (.getMessage e))
     ;;                nil))
     ;;       ]
     ;;   ;; (log/trace "emap! got e: " e)
     ;;   (if (nil? e)
     ;;     (let [e (Entity. k)]
     ;;       (.put (ds/datastore) e)
     ;;       (PersistentEntityMap. e nil))
     ;;     (PersistentEntityMap. e nil))))))

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
    (.put (ds/datastore) e)
    (PersistentEntityMap. e nil)))

(defn- emap-old
  [^Key k ^Entity e content]
  {:pre [(map? content)]}
  ;; (log/trace "emap old " k content)
  (if (empty? content)
    (PersistentEntityMap. e nil)
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
              (PersistentEntityMap. e nil))
            ;; new property
            (let [val (get-val-ds v)]
              ;; (log/trace "setting val" val (type val))
              ;; (flush)
              (.setProperty e prop val)))))
      (.put (ds/datastore) e)
      ;; (log/trace "saved entity " e)
      (PersistentEntityMap. e nil))))

(defn- emap-update-empty
  [keychain]
  (let [k (apply keychain-to-key keychain)
        e (try (.get (ds/datastore) k)
               (catch EntityNotFoundException e nil)
               (catch DatastoreFailureException e (throw e))
               (catch java.lang.IllegalArgumentException e (throw e)))]
        (if (entity-map? e)
          (PersistentEntityMap. e nil)
          (let [e (Entity. k)]
            (.put (ds/datastore) e)
            (PersistentEntityMap. e nil)))))

;; TODO: support embedded maps, e.g. (ds/emap!! [:Foo/bar] {:a 1, :b {:c 3, :d 4}})
;; technique: store them as embedded entities
(defn- emap-update-map
  [keychain content]
  ;; (log/trace "emap-update-map " keychain content)
  (let [k (apply keychain-to-key keychain)]
    ;; (log/trace "emap-update-map key: " k)
    (let [e (if (keyword? k)
              (let [e (Entity. (subs (str k) 1))] ;; key of form :Foo, i.e. a Kind specifier
                (.put (ds/datastore) e)
                e)
              (try (.get (ds/datastore) k)
                   (catch EntityNotFoundException e nil)
                   (catch DatastoreFailureException e (throw e))
                   (catch java.lang.IllegalArgumentException e (throw e))))]
      (if (nil? e)
        (emap-new k content)
        (emap-old k e content) ; even a new one hits this if id autogenned by keychain-to-key
        ))))

(defn- emap-update-fn
  "Second arg is a function to be applied to the Entity whose key is first arg"
  [keychain f]
  (if (nil? (namespace (first keychain)))
    ;; if first link in keychain has no namespace, it cannot be an ancestor node
    (let [txn (.beginTransaction (ds/datastore)) ;; else new entity
          e (Entity. (name (first keychain)))
          em (PersistentEntityMap. e nil)]
      (try
        (f em)
        (.put (ds/datastore) e)
        (.commit txn)
        (finally
          (if (.isActive txn)
            (.rollback txn))))
      em)
    (let [k (apply keychain-to-key keychain)
          e (try (.get (ds/datastore) k)
                 (catch EntityNotFoundException e
                   ;;(log/trace (.getMessage e))
                   e)
                 (catch DatastoreFailureException e
                   ;;(log/trace (.getMessage e))
                   nil)
                 (catch java.lang.IllegalArgumentException e
                   ;;(log/trace (.getMessage e))
                   nil))]
      (if (entity-map? e) ;; existing entity
        (let [txn (.beginTransaction (ds/datastore))]
          (try
            (f e)
            (.commit txn)
            (finally
              (if (.isActive txn)
                (.rollback txn))))
          (PersistentEntityMap. e nil))
        (let [txn (.beginTransaction (ds/datastore)) ;; else new entity
              e (Entity. k)
              em (PersistentEntityMap. e nil)]
          (try
            (f em)
            (.put (ds/datastore) e)
            (.commit txn)
            (finally
              (if (.isActive txn)
                (.rollback txn))))
          em)))))

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
  (if (empty? keychain)
    (throw (IllegalArgumentException. "keychain vector must not be empty"))
    (if (empty? content)
      (emap-update-empty keychain)
      (if (map? (first content))
        (emap-update-map keychain (first content))
        (if (fn? (first content))
          (emap-update-fn keychain (first content))
          (throw (IllegalArgumentException. "content must be map or function")))))))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;  key stuff
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(declare keychain? keychain= dogtag)

(defn keylink?
  [k]
  ;; (log/trace "keylink?" k (and (keyword k)
  ;;                              (not (nil? (namespace k)))))
  (or (and (keyword k)
           (not (nil? (namespace k))))
      (= (type k) com.google.appengine.api.datastore.Key)))

(defn proper-keychain?
  [k]
  {:pre [(and (vector? k) (not (empty? k)))]}
  ;; (if (and  (vector? k) (not (empty? k)))
  (if (every? keylink? k)
      true
      false))
(defn improper-keychain?
  [k]
  {:pre [(and (vector? k) (not (empty? k)))]}
  ;; (log/trace "improper-keychain?: " k)
  (if (every? keylink? (butlast k))
    (let [dogtag (last k)]
      ;; (log/trace "DOGTAG K" dogtag)
      (and (keyword? dogtag)
           (nil? (namespace dogtag))))
    false))

(defn keychain?
  [k]
  {:pre [(and (vector? k) (not (empty? k)))]}
  (or (proper-keychain? k) (improper-keychain? k)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defmulti kind class)
(defmethod kind Key
  [^Key k]
  (keyword (.getKind k)))
(defmethod kind Entity
  [^Entity e]
  (keyword (.getKind e)))
;; (defmethod kind migae.datastore.IPersistentEntityMap
;;   [^migae.datastore.PersistentEntityMap e]
;;   (log/trace "IPersistentEntityMap.kind")
;;   (keyword (.getKind (.content e))))
(defmethod kind migae.datastore.PersistentEntityMap
  [^migae.datastore.PersistentEntityMap e]
  ;; (log/trace "PersistentEntityMap.kind")
  (keyword (.getKind (.content e))))

;; FIXME: implement
;; (defmethod kind migae.datastore.PersistentEntityHashMap
;;   [^migae.datastore.PersistentEntityHashMap e]
;;   (keyword (namespace (last (.k e)))))

(defmethod kind clojure.lang.Keyword
  [^clojure.lang.Keyword kw]
  (when-let [k (namespace kw)]
    (keyword k)))
    ;; (clojure.core/name kw)))
(defmethod kind clojure.lang.PersistentVector
  [^clojure.lang.PersistentVector k]
  ;; FIXME: validate keychain contains only keylinks
  (if (keychain? k)
    (keyword (namespace (last k)))))

(defmulti identifier class)
(defmethod identifier Key
  [^Key k]
  ;; (log/trace "Key identifier" k)
  (let [nm (.getName k)
        id (.getId k)]
    (if (nil? nm) id (str nm))))
(defmethod identifier migae.datastore.PersistentEntityMap
  [^migae.datastore.PersistentEntityMap em]
  ;; (log/trace "PersistentEntityMap.identifier")
  (let [k (.getKey (.content em))
        nm (.getName k)
        id (.getId k)]
    (if (nil? nm) id (str nm))))

;; FIXME:
;; (defmethod identifier migae.datastore.PersistentEntityHashMap
;;   [^migae.datastore.PersistentEntityHashMap em]
;;   ;; (log/trace "PersistentEntityHashMap.identifier")
;;   (let [fob (dogtag (.k em))
;;         nm (read-string (name fob))]
;;     nm))

(defmethod identifier clojure.lang.PersistentVector
  [^clojure.lang.PersistentVector keychain]
  ;; FIXME: validate vector contains only keylinks
  (let [k (last keychain)]
    (if-let [nm (.getName k)]
      nm
      (.getId k))))

(defmulti ename class)
(defmethod ename Entity
  [^Entity e]
  (.getName (.getKey e)))
(defmethod ename clojure.lang.PersistentVector
  [^ clojure.lang.PersistentVector keychain]
  (name (last keychain)))
(defmethod ename migae.datastore.PersistentEntityMap
  [^migae.datastore.PersistentEntityMap em]
  (.getName (.getKey (.content em))))

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
(defmethod id migae.datastore.PersistentEntityMap
  [^migae.datastore.PersistentEntityMap e]
  (.getId (.getKey (.content e))))

(defn ekey? [^com.google.appengine.api.datastore.Key k]
  (= (type k) com.google.appengine.api.datastore.Key))

(defmulti to-keychain class)
(defmethod to-keychain Key
  [k]
  (keychain k))
(defmethod to-keychain migae.datastore.PersistentEntityMap
  [em]
  (keychain (.getKey (.content em))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defmulti keychain class)
(defmethod keychain Key
  [k]
  (let [keychain (keychain k)]
    keychain))
(defmethod keychain migae.datastore.PersistentEntityMap
  [em]
  (let [keychain (keychain (.getKey (.content em)))]
    keychain))
(defmethod keychain clojure.lang.PersistentVector
  [keychain]
  keychain)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(declare keyword-to-key)
(defn add-child-keylink
  [^KeyFactory$Builder builder chain]
  ;; (log/trace "add-child-keylink builder:" builder)
  ;; (log/trace "add-child-keylink chain:" chain (type chain) (type (first chain)))
  (doseq [kw chain]
    (if (nil? kw)
      nil
      (if (keylink? kw)
        (let [k (keyword-to-key kw)]
          (.addChild builder
                     (.getKind k)
                     (identifier k)))
        (throw (IllegalArgumentException.
                (str "not a clojure.lang.Keyword: " kw)))))))
;; (throw (RuntimeException. (str "Bad child keylink (not a clojure.lang.Keyword): " kw)))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; FIXME: restrict keychain-to-key to proper keychains
;; (defmethod keychain-to-key clojure.lang.PersistentVector
(defn keychain-to-key
  ;; FIXME: validate keychain only keylinks
  ([keychain]
  ;; (log/trace "keychain-to-key: " keychain (type keychain) " : " (vector? keychain))
  ;; (stack/print-cause-trace (throw (RuntimeException. "foo")) 99)
   (if (proper-keychain? keychain)
     (let [k (keyword-to-key (first keychain))
           root (KeyFactory$Builder. k)]
       (.getKey (doto root (add-child-keylink (rest keychain)))))
     (throw (IllegalArgumentException.
             (str "Invalid keychain: " keychain))))))
;; (throw (RuntimeException. (str "Bad keychain (not a vector of keywords): " keychain))))))


;; FIXME: make this an internal helper method
;; (defmethod keychain-to-key clojure.lang.Keyword
(defn keyword-to-key
  [^clojure.lang.Keyword k]
   "map single keyword to key."
;;     {:pre [(= (type k) clojure.lang.Keyword)]}
     ;; (log/trace "keyword-to-key:" k (type k))
     (if (not (= (type k) clojure.lang.Keyword))
        (throw (IllegalArgumentException.
                (str "not a clojure.lang.Keyword: " k))))
       ;; (throw (RuntimeException. (str "Bad keylink (not a clojure.lang.Keyword): " k))))
     (let [kind (clojure.core/namespace k)
           ident (edn/read-string (clojure.core/name k))]
       ;; (log/trace (format "keychain-to-key 1: kind=%s, ident=%s" kind ident))
       (cond
        (nil? kind)
        ;;(throw (RuntimeException. (str "Improper keylink (missing namespace): " k)))
        (throw (IllegalArgumentException.
                (str "missing namespace: " k)))
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
           (KeyFactory/createKey kind s)))
        :else
        (throw (IllegalArgumentException.
                (str k))))))
        ;; (throw (RuntimeException. (str "Bad keylink: " k))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defmulti dogtag class)
(defmethod dogtag Key
  [k]
  (let [keychain (keychain k)]
    (last keychain)))
(defmethod dogtag migae.datastore.PersistentEntityMap
  [em]
  (let [keychain (keychain (.getKey (.content em)))]
    (last keychain)))
(defmethod dogtag clojure.lang.PersistentVector
  [keychain]
  ;; FIXME: validate vector contains only keylinks
  (if (every? keylink? keychain)
    (last keychain)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defmulti entity-key class)
(defmethod entity-key Key
  [^Key k]
  k)
(defmethod entity-key migae.datastore.PersistentEntityMap
  [^migae.datastore.PersistentEntityMap e]
  (.getKey (.content e)))
(defmethod entity-key com.google.appengine.api.datastore.Entity
  [^Entity e]
  (.getKey e))
(defmethod entity-key clojure.lang.Keyword
  [^clojure.lang.Keyword k]
  (keychain-to-key [k]))
(defmethod entity-key clojure.lang.PersistentVector
  [kchain]
  ;; FIXME: validate vector contains only keylinks
  (keychain-to-key kchain))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn key=
  [em1 em2]
  (if (emap? em1)
    (if (emap? em2)
      (.equals (.content em1) (.content em2))
      (keychain= em1 em2))
    (if (map? em1)
      (keychain= em1 em2)
      (log/trace "EXCEPTION: key= applies only to maps and emaps"))))

(defn keykind?
  [k]
  ;; (log/trace "keykind?" k (and (keyword k)
  ;;                              (not (nil? (namespace k)))))
  (and (keyword? k) (nil? (namespace k))))

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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; FIXME: rename "keychain"
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

(defmethod keychain PersistentEntityMap
  [^PersistentEntityMap e]
  ;; (log/trace "keychain IPersistentEntityMap: " e)
  (keychain (.getKey (.content e))))

(defmethod keychain com.google.appengine.api.datastore.EmbeddedEntity
  [^com.google.appengine.api.datastore.EmbeddedEntity ee]
  ;; (log/trace "keychain Entity: " e)
  (keychain (.getKey ee)))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;  utils
(defn print
  [em]
  (binding [*print-meta* true]
    (prn em)))

(defn print-str
  [em]
  (binding [*print-meta* true]
    (pr-str em)))

(defn println
  [^migae.datastore.IPersistentEntityMap em]
  (binding [*print-meta* true]
    (prn em)))

(defn dump
  [msg datum data]
  (binding [*print-meta* true]
    (log/trace msg (pr datum) (pr data))))


;;(load "datastore/PersistentEntityMapSeq")
;(load "datastore/PersistentEntityMap")
(load "datastore/PersistentEntityHashMap")
;;(load "datastore/utils")
(load "datastore/ctor_local")
;;(load "datastore/predicates")

(load "datastore/query")
(load "datastore/ctor_pull")
(load "datastore/ekey")
;;(load "datastore/dsmap")
(load "datastore/api")

