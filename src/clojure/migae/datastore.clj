(ns migae.datastore
  (:refer-clojure :exclude [get into name print reduce])
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
           ;; migae.datastore.ImproperKeylinkException
           ;; [migae.datastore PersistentEntityMap PersistentEntityMapCollIterator])
           )
  (:require [clojure.core :as clj]
            [clojure.walk :as walk]
            [clojure.stacktrace :refer [print-stack-trace]]
            [clojure.tools.reader.edn :as edn]
            ;; [migae.datastore :refer :all]
            [migae.datastore.service :as ds]
            [migae.datastore.keychain :as ekey]
            ;; [migae.datastore.dsmap :as dsm]
            ;; [migae.datastore.emap :as emap]
            ;; [migae.datastore.entity :as dse]
            ;; [migae.datastore.key :as dskey]
            ;; [migae.datastore.query :as dsqry]
            [migae.infix :as infix]
            [clojure.tools.logging :as log :only [trace debug info]]))
;; (ns migae.datastore
;;   (:refer-clojure :exclude [empty?])
;;   (:import [java.util
;;             ArrayList
;;             Vector]
;;            [com.google.appengine.api.datastore
;;             DatastoreServiceFactory
;;             DatastoreFailureException
;;             Email
;;             Entity EmbeddedEntity EntityNotFoundException
;;             Key KeyFactory KeyFactory$Builder
;;             Link])
;;   (:require [migae.datastore.keychain :refer :all]
;;             ;; [migae.datastore.adapter  :refer :all]
;;             [clojure.core :as clj]
;;             [clojure.tools.reader.edn :as edn]
;;             [clojure.tools.logging :as log :only [trace debug info]]))

;; (:refer-clojure :exclude [name hash key])
  ;; (:import [com.google.appengine.api.datastore])
  ;;           ;; Entity
  ;;           ;; Key])
  ;; (:require [clojure.tools.logging :as log :only [trace debug info]]))

(declare get-val-clj get-val-ds)
(declare props-to-map get-next-emap-prop)

(declare make-embedded-entity)

(defn- get-next-emap-prop [this]
  ;; (log/trace "get-next-emap-prop" (.query this))
  (let [r (.next (.query this))]
    ;; (log/trace "next: " r)
    r))

;;(load "datastore/keychain")

(load "datastore/PersistentEntityMapIterator")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;  PersistentEntityMap
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(deftype PersistentEntityMap [content meta]

  java.lang.Iterable
  (iterator [this]
    (log/trace "Iterable iterator" (.content this))
    (let [props (.getProperties content) ;; java.util.Map<java.lang.String,java.lang.Object>
          entry-set (.entrySet props)
          e-iter (.iterator entry-set)
          em-iter (PersistentEntityMapIterator. e-iter) ]
    ;; (log/trace "Iterable res:" em-iter)
    em-iter))

  ;; FIXME: put :^PersistentEntityMap in every PersistentEntityMap
  clojure.lang.IObj ;; extends IMeta
  (meta [this]      ; IMeta's only method
    (log/trace "IObj meta" (ekey/to-keychain content))
    meta)
  (withMeta [this md]
    (log/trace "IObj withMeta" md)
    (let [em (PersistentEntityMap. (.clone content) md)]
      ;; (log/trace "entity with meta" em)
      em))

  clojure.lang.IReference ; extends IMeta; required to support metadata reader syntax?
    ;; IPersistentMap alterMeta(IFn alter, ISeq args) ;
  (alterMeta [this alter args]
    (log/trace "IReference.alterMeta")
    {:foo 3})
    ;; IPersistentMap resetMeta(IPersistentMap m);
  (resetMeta [this m]
    (log/trace "IReference.resetMeta")
    {:bar 3})

  clojure.lang.IFn
  (invoke [this k]
    {:pre [(keyword? k)]}
    (log/trace "IFn invoke, arg: " k)
    (let [prop (.getProperty content (clj/name k))]
      (if (not (nil? prop))
        (get-val-clj prop)
        nil)))
  ;; Object applyTo(ISeq arglist) ;
  (applyTo [_ arglist]
    (log/trace "IFn.applyTo"))
  ;; (invokePrim [_ ??]
  ;;   (log/trace "IFn.invokePrim"))

  java.util.Map$Entry
  (getKey [this]
    (log/trace "java.util.Map$Entry getKey ")
    (let [k (.getKey content)]
      (ekey/to-keychain k)))
  ;; FIXME: just do (ekey/to-keychain content)??
      ;;     kind (.getKind k)
      ;;     nm (.getName k)]
      ;; [(keyword kind (if nm nm (.getId k)))]))
  (getValue [_]
    (log/trace "java.util.Map$Entry getVal")
    (props-to-map (.getProperties content)))
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
  (clj/key [this]
    (log/trace "IMapEntry key")
    )
  (val [_]
    (log/trace "IMapEntry val")
    )

  ;; clojure.lang.Counted
  ;; (count [_]
  ;;   (.size (.getProperties content)))


  clojure.lang.IReduce
  (reduce [this f]
    (log/trace "HELP! reduce") (flush)
    this)
  (reduce [this f to-map]
    ;; called by "print" stuff
    ;; (log/trace "IReduce.reduce:" (class to-map) (type to-map))
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
      ;; f = cons, so we can just use the native clj/into
      ;; (let [from-props (.getProperties content)
      ;;       from-coll (clj/into {} (for [[k v] from-props]
      ;;                                (let [prop (keyword k)
      ;;                                      val (get-val-clj v)]
      ;;                                  {prop val})))
      ;;       foo (.setPropertiesFrom (.content to-map) (.content this))
      ;;       to-props (.getProperties (.content to-map))
      ;;       to-coll (clj/into {} (for [[k v] to-props]
      ;;                              (let [prop (keyword k)
      ;;                                    val (get-val-clj v)]
      ;;                                {prop val})))
      ;;       res (with-meta (clj/into to-coll from-coll)
      ;;             {:migae/key (:migae/key (meta to-map))
      ;;              :type PersistentEntityMap})]
      ;;   ;; (log/trace "to-coll: " res (type res))
      ;;   to-map)
      (= (class to-map) clojure.lang.PersistentArrayMap$TransientArrayMap)
      (do
        (log/trace "to-map:  PersistentArrayMap$TransientArrayMap")
        ;; we use a ghastly hack in order to retain metadata
        ;; FIXME: handle case where to-map is a clj-emap (map with ^:PersistentEntityMap metadata)
        (let [from-props (.getProperties content)
              from-coll (clj/into {} (for [[k v] from-props]
                                       (let [prop (keyword k)
                                             val (get-val-clj v)]
                                         {prop val})))
              to-ent (Entity. (.getKey content))
              ;; to-coll (clj/into {} (for [[k v] to-props]
              ;;                        (let [prop (keyword k)
              ;;                              val (get-val-clj v)]
              ;;                          {prop val})))
              to-keychain (if (nil? (:migae/keychain (meta to-map)))
                            (ekey/to-keychain content)
                            (:migae/keychain (meta to-map)))]
          (doseq [[k v] from-coll]
            (assoc! to-map k v))
          (let [p (persistent! to-map)]
            (doseq [[k v] p]
              (.setProperty to-ent (subs (str k) 1) (get-val-ds v))))
          ;; (let [m1 (persistent! to-map)
          ;;       m2 (with-meta m1 {:migae/keychain ekey/to-keychain
          ;;                         :type PersistentEntityMap})
          ;;       to-coll (transient m2)]
          ;;   (log/trace "m2: " (meta m2) m2 (class m2))
          ;;   (log/trace "to-coll: " (meta to-coll) to-coll (class to-coll))
          (PersistentEntityMap. to-ent nil)))
      :else (log/trace "HELP! reduce!" (class to-map)))
      )

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
  ;; why implement ISeq?  An emap is a map!  But its field names are
  ;; strings, so they have a natural order.
;; FIXME:  bad idea.  undo this.
  ;; clojure.lang.ISeq ;; extends IPersistentCollection, extends Seqable
  ;;;; Seqable interface
  ;; ISeq Seqable.seq()
  ;; (seq [this]
  ;;   ;; seq is called by: into, merge, "print", e.g. (log/trace em)
  ;;   ;; (log/trace "seq" (.hashCode this) (.getKey content))
  ;;   (let [props (.getProperties content)
  ;;         kprops (clj/into {}
  ;;                          (for [[k v] props]
  ;;                            (do
  ;;                            ;; (log/trace "v: " v)
  ;;                            (let [prop (keyword k)
  ;;                                  val (get-val-clj v)]
  ;;                              ;; (log/trace "prop " prop " val " val)
  ;;                              {prop val}))))
  ;;         k (ekey/to-keychain content)
  ;;         res (clj/into kprops {:migae/keychain k})]
  ;;     (log/trace "seq result:" (type res) res)
  ;;     ;;(seq res)))
  ;;     this))
  ;;   ;;;; IPersistentCollection interface
  (cons [this o] ;; o should be a MapEntry?
    ;; this is called on:  (into em {:a }); not called on (into em1 em2)
    (log/trace "IPersistentCollection.cons: " o (type o))
    (cond
      (nil? o)
      (do
        (log/trace "nil object")
        this)
      (= (type o) clojure.lang.MapEntry)
      (do
        ;; (log/trace "cons clojure.lang.MapEntry" o " to emap" this)
        (let [new-entity (.clone content)]
          (.setProperty new-entity (subs (str (first o)) 1) (get-val-ds (second o)))
          (PersistentEntityMap. new-entity nil)))
      (= (type o) clojure.lang.PersistentArrayMap)
      (do
        (log/trace "cons PersistentArrayMap to emap")
        o)
      ;; (do
      ;;   ;; (log/trace "cons clj map to emap")
      ;;   (doseq [[k v] o]
      ;;     (let [nm (subs (str k) 1)
      ;;           val (get-val-ds v)]
      ;;       ;; (log/trace "cons key: " nm (type nm))
      ;;       ;; (log/trace "cons val: " val (type val))
      ;;       (.setProperty entity nm val)))
      ;;   ;; (.put (datastore) content)
      ;;   this)
      (= (type o) PersistentEntityMap)
      (do
        (log/trace "cons PersistentEntityMap to emap")
        (let [props (.getProperties (.content o))]
          ;; (log/trace "cons emap to emap")
          (doseq [[k v] props]
            (.setProperty content (subs (str k) 1) (get-val-ds v)))
          ;; (.put (datastore) content)
          this))
      ;; (= (type o) java.util.Collections$UnmodifiableMap$UnmodifiableEntrySet$UnmodifiableEntry)
      (= (type o) java.util.Map$Entry)
      (do
        (log/trace "cons java.util.Map$Entry to emap")
        (.setProperty content (.getKey o) (.getValue o))
        ;; (.put (datastore) content)
        this)
      :else (log/trace "cons HELP?" o (type o))))
      ;; (do
      ;;   (doseq [item o]
      ;;     (do (log/trace "item " item)
      ;;         (doseq [[k v] item]
      ;;           (do (log/trace "k/v: " k v)
      ;;               (.setProperty content (clj/name k) v)))))
      ;;   (.put (datastore) content)
      ;;   this)))
  ;; (empty [this] - overridden by ISeq

    ;; (and (isa? (class o) PersistentEntityMap)
    ;;      (.equiv (augment-contents content) (.(augment-contents content) o))))
  ;;int count();
  (count [_]
    (log/trace "ISeq count")
    (.size (.getProperties content)))
  ;;IPersistentCollection IPersistentCollection.cons(Object o) - overridden by ISeq.cons
  ;;IPersistentCollection IPersistentCollection.empty()
  (empty [this]
    (let [k (.getKey (.content this))
          e (Entity. k)]
      (PersistentEntityMap. e nil)))
  (equiv [this o]
    (.equals this o))
    ;; (.equals content (.content o)))
  ;;;; ISeq interface
  ;;Object first();
  ;; (first [this]
  ;;   (log/trace "ISeq first")
  ;;   ;;    (->PersistentEntityMap (first query)))
  ;;   this)
  ;; ;;ISeq next();
  ;; (next [this]
  ;;   (log/trace "ISeq next")
  ;;   ;; (let [res (next content)]
  ;;   ;;   (if (nil? res)
  ;;   ;;     nil
  ;;   ;;     (PersistentEntityMap. res))))
  ;;   nil)
  ;; ;;ISeq more();
  ;; (more [_] (log/trace "ISeq more"))
  ;; ;;ISeq cons(Object o);

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
      (.setProperty content k v)
      this))
  (persistent [this]
    (log/trace "ITransientCollection persistent")
    ;; (try (/ 1 0) (catch Exception e (print-stack-trace e)))
    (let [props (.getProperties content)
          kch (ekey/to-keychain content)
          coll (clj/into {} (for [[k v] props]
                              (let [prop (keyword k)
                                    val (get-val-clj v)]
                                {prop val})))
          res (with-meta coll {:migae/keychain kch
                               :type PersistentEntityMap})]
      (log/trace "persistent result: " (meta res) res (class res))
      res))


  ;; clojure.lang.ITransientMap
  ;; (assoc [this k v]                     ; both assoc! and assoc (?)
  ;;   (let [prop (clj/name k)]
  ;;     (log/trace "ITransientMap assoc: setting prop " k "->" prop v)
  ;;     (.setProperty content prop v)
  ;;     this))
  ;; (without [this k]                     ; = dissoc!, return new datum with k removed
  ;;   (let [prop (clj/name k)]
  ;;     (log/trace "without: removing prop " k "->" prop)
  ;;     (.removeProperty content prop)
  ;;     this))
  ;; (persistent [this]                    ; persistent!
  ;;     (log/trace "ITransientMap persistent")
  ;;   )

  clojure.lang.IPersistentMap ; extends Iterable, Associative, Counted
  (assoc [this k v]
    (let [prop (subs (str k) 1)]
      (log/trace "IPersistentMap assoc: " k v "(" prop v ")")
      ;; (.setProperty content prop v)
      ;; this))
      (let [to-props (.getProperties content)
            to-coll (clj/into {} (for [[k v] to-props]
                                     (let [prop (keyword k)
                                           val (get-val-clj v)]
                                       {prop val})))
            key-chain (ekey/to-keychain this)
            res (clj/assoc to-coll k v)]
      (log/trace "IPersistentMap assoc res: " res)
      (with-meta res {:migae/keychain key-chain}))))
  (assocEx [_ k v]
    (log/trace "assocEx")
    (PersistentEntityMap. (.assocEx content k v) nil))
  (without [this k]                     ; = dissoc!, return new datum with k removed
    (let [prop (clj/name k)]
      (log/trace "without: removing prop " k "->" prop)
      (.removeProperty content prop)
      this))

  clojure.lang.Associative
  (containsKey [_ k]
    (do (log/trace "Associative.containsKey " k)
    (let [prop (clj/name k)
          r    (.hasProperty content prop)]
      r)))
  (entryAt [this k]
    (do (log/trace "Associative.entryAt " k)
    (let [val (.getProperty content (clj/name k))
          entry (clojure.lang.MapEntry. k val)]
      ;; (log/trace "entryAt " k val entry)
      entry)))

  ;; clojure.lang.IObj
  ;; (withMeta [_ m])
  ;; (meta [_])

  clojure.lang.Seqable
  ;; ISeq seq(); overriden by ISeq
  (seq [this]
    ;; seq is called by: into, merge, "print", e.g. (log/trace em)
    ;; (log/trace "seq" (.hashCode this) (.getKey content))
    (let [props (.getProperties content)
          kprops (clj/into {}
                           (for [[k v] props]
                             (do
                             ;; (log/trace "v: " v)
                             (let [prop (keyword k)
                                   val (get-val-clj v)]
                               ;; (log/trace "prop " prop " val " val)
                               {prop val}))))
          k (ekey/to-keychain content)
          res (clj/into kprops {:migae/keychain k})]
      (log/trace "seq result:" (type res) res)
      (seq res)))
;;      this))

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
    (log/trace "ILookup.valAt" k)
    (let [prop (clj/name k)]
      (if-let [v  (.getProperty content prop)]
        (get-val-clj v)
        nil)))
  (valAt [_ k not-found]
    (log/trace "valAt w/notfound: " k)
    (.getProperty content (str k) not-found)))
;; end deftype PersistentEntityMap

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

;; load order matters!
;;(load "datastore/service")
(load "datastore/PersistentEntityMapIterator")
(load "datastore/adapter")
(load "datastore/ctor_common")
(load "datastore/ctor_push")
(load "datastore/query")
(load "datastore/ctor_pull")
(load "datastore/ekey")
(load "datastore/dsmap")
(load "datastore/api")
