(ns migae.datastore
  (:refer-clojure :exclude [get into name reduce])
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
  ;; (log/trace "get-next-emap-prop" (.ds-iter this))
  (let [r (.next (.ds-iter this))]
    ;; (log/trace "next: " r)
    r))

;;(load "datastore/keychain")

(deftype PersistentEntityMapIterator [ds-iter]
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
;;  PersistentEntityMap
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(deftype PersistentEntityMap [entity]

  java.lang.Iterable
  (iterator [this]
    (log/trace "Iterable iterator" (.entity this))
    (let [props (.getProperties entity) ;; java.util.Map<java.lang.String,java.lang.Object>
          entry-set (.entrySet props)
          e-iter (.iterator entry-set)
          em-iter (PersistentEntityMapIterator. e-iter) ]
    ;; (log/trace "Iterable res:" em-iter)
    em-iter))

  ;; FIXME: put :^PersistentEntityMap in every PersistentEntityMap
  clojure.lang.IMeta
  (meta [_]
    ;; (log/trace "IMeta meta")
    {:migae/key (ekey/to-keychain entity)
     :type PersistentEntityMap})

  clojure.lang.IObj
  (withMeta [this md]
    (log/trace "IObj withMeta" md))
 ;; (PersistentEntityMap. (with-meta m md)))

  clojure.lang.IFn
  (invoke [this k]
    {:pre [(keyword? k)]}
    ;; (log/trace "IFn invoke, arg: " k)
    (let [prop (.getProperty entity (clj/name k))]
      (if (not (nil? prop))
        (get-val-clj prop)
        nil)))
  ;; (applyTo [_ arglist])
  ;; (invokePrim [_ ??] )

  java.util.Map$Entry
  (getKey [this]
    ;; (log/trace "java.util.Map$Entry getKey ")
    (let [k (.getKey entity)]
      (ekey/to-keychain k)))
  ;; FIXME: just do (ekey/to-keychain entity)??
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
  (clj/key [this]
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
      (= (type o) PersistentEntityMap)
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
  (empty [this]
    (let [k (.getKey (.entity this))
          e (Entity. k)]
      (PersistentEntityMap. e)))
  (equiv [this o]
    (.equals this o))
    ;; (.equals entity (.entity o)))

    ;; (and (isa? (class o) PersistentEntityMap)
    ;;      (.equiv (augment-contents entity) (.(augment-contents entity) o))))

  clojure.lang.IReduce
  (reduce [this f]
    (log/trace "HELP! reduce") (flush)
    this)
  (reduce [this f to-map]
    ;; (log/trace "reduce f to-map: " to-map " from-coll: " (.entity this))
    (cond
      (= (class to-map) PersistentEntityMap)
      (let [k (.getKey (.entity this))
            e (Entity. k)]
        (.setPropertiesFrom e (.entity to-map))
        (.setPropertiesFrom e (.entity this))
        (PersistentEntityMap. e))
      ;; f = cons, so we can just use the native clj/into
      ;; (let [from-props (.getProperties entity)
      ;;       from-coll (clj/into {} (for [[k v] from-props]
      ;;                                (let [prop (keyword k)
      ;;                                      val (get-val-clj v)]
      ;;                                  {prop val})))
      ;;       foo (.setPropertiesFrom (.entity to-map) (.entity this))
      ;;       to-props (.getProperties (.entity to-map))
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
      ;; we use a ghastly hack in order to retain metadata
      ;; FIXME: handle case where to-map is a clj-emap (map with ^:PersistentEntityMap metadata)
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
                          (ekey/to-keychain entity)
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
        (PersistentEntityMap. to-ent))

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
          kch (ekey/to-keychain entity)
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
            key-chain (ekey/to-keychain this)
            res (clj/assoc to-coll k v)]
      (log/trace "IPersistentMap assoc res: " res)
      (with-meta res {:migae/keychain key-chain}))))
  (assocEx [_ k v]
    (log/trace "assocEx")
    (PersistentEntityMap. (.assocEx entity k v)))
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
          entry (clojure.lang.MapEntry. k val)]
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
    (let [prop (clj/name k)]
      (if-let [v  (.getProperty entity prop)]
        (get-val-clj v)
        nil)))
  (valAt [_ k not-found]
    (log/trace "valAt w/notfound: " k)
    (.getProperty entity (str k) not-found)))
;; end deftype PersistentEntityMap

(deftype PersistentEntityMapCollIterator [ds-iter]
  java.util.Iterator
  (hasNext [this]
    (.hasNext ds-iter))
  (next    [this]
    (PersistentEntityMap. (.next ds-iter)))
  ;; (remove  [this])
  )

;;(load "datastore/service")
(load "datastore/adapter")
(load "datastore/ctor_common")
(load "datastore/ctor_push")
(load "datastore/ctor_pull")
(load "datastore/ekey")
(load "datastore/dsmap")
(load "datastore/api")
