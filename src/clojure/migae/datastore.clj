(ns migae.datastore
  (:refer-clojure :exclude [print println print-str])
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
            ;; [migae.infix :as infix]
            [clojure.tools.logging :as log :only [trace debug info]]))


(clojure.core/println "loading datastore")

(declare ->PersistentEntityMapSeq)
(declare ->PersistentEntityMap)
(declare ->PersistentEntityHashMap)

(declare ds-to-clj get-val-ds)
(declare make-embedded-entity)
(declare ename props-to-map get-next-emap-prop)

(declare keychain? keylink? keykind? keychain keychain-to-key proper-keychain? improper-keychain?)

(load "datastore/PersistentEntityMapSeq")
(load "datastore/PersistentEntityMap")
(load "datastore/PersistentEntityHashMap")

(defn- get-next-emap-prop [this]
  ;; (log/trace "get-next-emap-prop" (.query this))
  (let [r (.next (.query this))]
    ;; (log/trace "next: " r)
    r))

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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(deftype PersistentEntityMapCollIterator [query]
  java.util.Iterator
  (hasNext [this]
    (log/trace "PersistentEntityMapCollIterator hasNext")
    (.hasNext query))
  (next    [this]
    (log/trace "PersistentEntityMapCollIterator next")
    (->PersistentEntityMap (.next query) nil))
  ;; (remove  [this])
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

(defn- ds-to-clj-coll
  "Type conversion: java to clojure"
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
(defn- ds-to-clj
  [v]
  (log/trace "DS-to-CLJ:" v (type v) (class v))
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
                  (= (type v) EmbeddedEntity) (->PersistentEntityMap v nil)
                  (= (type v) Key) (let [kind (.getKind v)]
                                     (if (= kind "Keyword")
                                       (keyword (.getName v))
                                       ;; (symbol (.getName v))))
                                       (str \' (.getName v))))
                  :else (do
                          ;; (log/trace "HELP: ds-to-clj else " v (type v))
                          (throw (RuntimeException. (str "HELP: ds-to-clj else " v (type v))))
                          (edn/read-string v)))]
    ;; (log/trace "ds-to-clj result:" v val)
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

(defn props-to-map
  [props]
  (into {} (for [[k v] props]
                 (let [prop (keyword k)
                       val (ds-to-clj v)]
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
(declare keychain? keychain=? key? key=? map=? entity-map=? dogtag)

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
;;;;  key stuff
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn keylink?
  [k]
  ;; (log/trace "keylink?" k (and (keyword k)
  ;;                              (not (nil? (namespace k)))))
  (or (and (keyword? k)
           (not (nil? (namespace k))))
      (= (type k) com.google.appengine.api.datastore.Key)))

(defn proper-keychain?
  [k]
  {:pre [(and (vector? k) (not (empty? k)))]}
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
(defmethod kind migae.datastore.IPersistentEntityMap
  [^migae.datastore.PersistentEntityMap e]
  (log/trace "IPersistentEntityMap.kind")
  (keyword (.getKind (.content e))))
;; (defmethod kind migae.datastore.PersistentEntityHashMap
;;   [^migae.datastore.PersistentEntityMap e]
;;   ;; (log/trace "PersistentEntityHashMap.kind")
;;   (kind (.k e)))
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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
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
(defmulti keychain
 class)

(defmethod keychain nil
  [x]
  nil)

(defmethod keychain Entity
  [^Entity e]
  ;; (log/trace "keychain co-ctor 1: entity" e)
  (keychain (.getKey e)))

(defmethod keychain EmbeddedEntity
  [^EmbeddedEntity e]
  ;; (log/trace "keychain co-ctor 1: entity" e)
  (keychain (.getKey e)))

(defmethod keychain Key
  [^Key k]
  {:pre [(not (nil? k))]}
  ;; (log/trace "keychain co-ctor 2: key" k)
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

(defmethod keychain migae.datastore.PersistentEntityMap
  [^PersistentEntityMap e]
  (log/trace "to-keychain IPersistentEntityMap: " e)
  (to-keychain (.getKey (.content e))))

;; (defmethod keychain migae.datastore.PersistentEntityHashMap
;;   [^PersistentEntityHashMap e]
;;   (log/trace "to-keychain IPersistentEntityMap: " e)
;;   (.k e))

(defmethod keychain clojure.lang.PersistentVector
  [keychain]
  (if (keychain? keychain)
    keychain
    (throw (IllegalArgumentException.
            (str "Invalid keychain: " keychain)))))

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
           (if (= (count s) 1)
             (KeyFactory/createKey kind s)
             (let [id (edn/read-string (str "0" s))]
               (if (= (type id) java.lang.Long)
                 (KeyFactory/createKey kind id)
                 (KeyFactory/createKey kind s))))
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
(defn key=?
  [em1 em2]
  ;; FIXME:  pre: validate types
  (if (entity-map? em1)
    (if (entity-map? em2)
      (.equals (.content em1) (.content em2))
      (keychain=? em1 em2))
    (if (map? em1)
      (keychain=? em1 em2)
      (log/trace "EXCEPTION: key= applies only to maps and emaps"))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn entity-map=?
  [em1 em2]
  ;; FIXME:  pre: validate types
  (and (key=? em1 em2) (map=? em1 em2)))
      ;; (log/trace "EXCEPTION: key= applies only to maps and emaps"))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defmulti map=?
  "Datastore entity equality predicate.  True if comparands have same
  keys and values.  See also key=? and map=?"
  (fn [em1 em2]
    [(type em1) (type em2)]))

(defmethod map=? [PersistentEntityMap PersistentEntityMap]
  [em1 em2]
  (if (instance? migae.datastore.IPersistentEntityMap em1)
    (if (instance? migae.datastore.IPersistentEntityMap em2)
      (do
        (log/trace "comparing 2 PersistentEntityMaps")
        (let [this-map (.getProperties (.content em1))
              that-map (.getProperties (.content em2))]
          (log/trace "this-map:" this-map (type this-map))
          (log/trace "that-map:" that-map (type that-map))
          (log/trace "(= this-map that-map)" (clojure.core/= this-map that-map))
          (clojure.core/= this-map that-map)))
      false) ; FIXME
    false)  ; FIXME
  )

(defn hybrid=
  [em1 em2]
  (let [this-map (ds-to-clj (.getProperties (.content em1)))
        ;; FIXME: is ds-to-clj appropriate for this?  is props-to-map a better name/concept?
        that-map em2]
    (log/trace "this-map:" this-map (type this-map))
    (log/trace "that-map:" em2 (type em2))
    (log/trace "(= this-map em2)" (clojure.core/= this-map em2))
    (clojure.core/= this-map em2))
  )

(defmethod map=? [PersistentEntityMap clojure.lang.PersistentArrayMap]
  [^PersistentEntityMap pem ^clojure.lang.PersistentArrayMap pam]
  (log/trace "comparing: PersistentEntityMap PersistentArrayMap")
  (hybrid= pem pam))

(defmethod map=? [clojure.lang.PersistentArrayMap PersistentEntityMap]
  [^clojure.lang.PersistentArrayMap pam ^PersistentEntityMap pem]
  (log/trace "comparing: PersistentEntityMap PersistentArrayMap")
  (hybrid= pem pam))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; (defmethod map=? [PersistentEntityMap clojure.lang.PersistentHashMap]
;;   [^PersistentEntityMap pem ^clojure.lang.PersistentHashMap phm]
;;   (log/trace "comparing: PersistentEntityMap PersistentHashMap")
;;   (hybrid= pem phm))

;; (defmethod map=? [clojure.lang.PersistentHashMap PersistentEntityMap]
;;   [^clojure.lang.PersistentHashMap phm ^PersistentEntityMap pem]
;;   (log/trace "comparing: PersistentEntityMap PersistentHashMap")
;;   (hybrid= pem phm))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn keykind?
  [k]
  (log/trace "keykind?" k (and (keyword k)
                               (not (nil? (namespace k)))))
  (and (keyword? k) (nil? (namespace k))))

(defn keychain=?
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

(load "datastore/ctor_local")
(load "datastore/query")
(load "datastore/ctor_push")
(load "datastore/ctor_pull")
(load "datastore/api")
