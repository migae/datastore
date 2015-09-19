(ns migae.datastore.keys
  (:import [com.google.appengine.api.datastore
            DatastoreFailureException
            DatastoreService
            DatastoreServiceFactory
            DatastoreServiceConfig
            DatastoreServiceConfig$Builder
            Entity EmbeddedEntity EntityNotFoundException
            Email
            Key KeyFactory KeyFactory$Builder
            Link]
           [java.util
            Collection
            Collections
            ArrayList
            HashSet
            Vector]
           ;; migae.datastore.PersistentEntityMap
           )
  (:require [clojure.tools.logging :as log :only [debug info]]
            [clojure.tools.reader.edn :as edn]
            [migae.datastore.api :as ds]))

(clojure.core/println "loading migae.datastore.keys")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;  key stuff
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(declare get-val-ds get-val-ds-coll proper-keychain? keyword-to-key add-child-keylink)

(defn keychain=?
  [k1 k2]
  (let [kch1 (if (ds/entity-map? k1)
               ;; recur with .getParent
               (if (map? k1)
                 (:migae/key (meta k1))))
        kch2 (if (ds/entity-map? k2)
               ;; recur with .getParent
               (if (map? k2)
                 (:migae/key (meta k2))))]
    ))

(defn key=?
  [em1 em2]
  ;; FIXME:  pre: validate types
  (if (ds/entity-map? em1)
    (if (ds/entity-map? em2)
      (.equals (.content em1) (.content em2))
      (keychain=? em1 em2))
    (if (map? em1)
      (keychain=? em1 em2)
      (log/debug "EXCEPTION: key= applies only to maps and emaps"))))

(defn- make-embedded-entity
  [m]
  {:pre [(map? m)]}
  (let [embed (EmbeddedEntity.)]
    (doseq [[k v] m]
      ;; FIXME:  (if (map? v) then recur
      (.setProperty embed (subs (str k) 1) (get-val-ds v)))
    embed))

(defn- symbol-to-ds
  [sym]
   (KeyFactory/createKey "Symbol" (str sym)))

(defn- keyword-to-ds
  [kw]
   (KeyFactory/createKey "Keyword"
                         ;; remove leading ':'
                         (subs (str kw) 1)))

;; (defmulti identifier class)
;; (defmethod identifier Key
;;   [^Key k]
;;   ;; (log/debug "Key identifier" k)
;;   (let [nm (.getName k)
;;         id (.getId k)]
;;     (if (nil? nm) id (str nm))))
;; (defmethod identifier migae.datastore.PersistentEntityMap
;;   [^migae.datastore.PersistentEntityMap em]
;;   ;; (log/debug "PersistentEntityMap.identifier")
;;   (let [k (.getKey (.content em))
;;         nm (.getName k)
;;         id (.getId k)]
;;     (if (nil? nm) id (str nm))))
;; (defmethod identifier migae.datastore.PersistentEntityHashMap
;;   [^migae.datastore.PersistentEntityHashMap em]
;;   ;; (log/debug "PersistentEntityHashMap.identifier")
;;   (let [fob (dogtag (.k em))
;;         nm (read-string (name fob))]
;;     nm))

;; (defmethod identifier clojure.lang.PersistentVector
;;   [^clojure.lang.PersistentVector keychain]
;;   ;; FIXME: validate vector contains only keylinks
;;   (let [k (last keychain)]
;;     (if-let [nm (.getName k)]
;;       nm
;;       (.getId k))))

(defn keylink?
  [k]
  (and (keyword? k)
       (not (nil? (namespace k)))))

(defn keyword-to-key
  [^clojure.lang.Keyword k]
   "map single keyword to key."
   ;; (log/debug "keyword-to-key:" k (type k))
   (if (not (keyword? k))
     (throw (IllegalArgumentException.
             (str "not a clojure.lang.Keyword: " k))))
   (let [kind (clojure.core/namespace k)
         ident (edn/read-string (clojure.core/name k))]
     ;; (log/debug (format "keychain-to-key 1: kind=%s, ident=%s" kind ident))
     (cond
       (nil? kind)
       (throw (IllegalArgumentException. (str "missing namespace: " k)))

       (integer? ident)
       (KeyFactory/createKey kind ident)

       ;; for alpha name parts, edn reader makes symbols, not strings
       (symbol? ident)
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

(defn identifier
  "identifier co-ctor"
  [v]
  (log/debug "identifier co-ctor" v)
  ;; FIXME validate
  (if (= (type v) Key)
    (let [nbr (.getId v)]
      (if (> nbr 0)
        (do (log/debug "id nbr: " nbr)
            nbr)
        (if-let [nm (.getName v)]
          (do (log/debug "id nm: " nm)
              nm))))
    (let [dogtag (last v)]
      (if-let [ns (namespace dogtag)]
        (keyword (name dogtag))
        dogtag))))

(defn add-child-keylink
  [^KeyFactory$Builder builder chain]
  ;; (log/debug "add-child-keylink builder:" builder)
  ;; (log/debug "add-child-keylink chain:" chain (type chain) (type (first chain)))
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

(defn proper-keychain?
  [k]
  (and (not (empty? k))
       (every? keylink? k)))

(defn improper-keychain?
  [k]
  (and (or (nil? (butlast k))
           (every? keylink? (butlast k)))
       (and (keyword? (last k))
            (nil? (namespace (last k))))))

(defn keychain?
  [k]
  (proper-keychain? k))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defmulti keychain
 class)

(defmethod keychain nil
  [x]
  nil)

(defmethod keychain Entity
  [^Entity e]
  ;; (log/debug "keychain co-ctor 1: entity" e)
  (keychain (.getKey e)))

(defmethod keychain EmbeddedEntity
  [^EmbeddedEntity e]
  ;; (log/debug "keychain co-ctor 1: entity" e)
  (keychain (.getKey e)))

(defmethod keychain Key
  [^Key k]
  {:pre [(not (nil? k))]}
  ;; (log/debug "keychain co-ctor 2: key" k)
  (let [kind (.getKind k)
        nm (.getName k)
        id (str (.getId k))
        dogtag (keyword kind (if nm nm id))
        res (if (.getParent k)
              (conj (list dogtag) (keychain (.getParent k)))
              (list dogtag))]
    ;; (log/debug "kind" kind "nm " nm " id " id " parent " (.getParent k))
    ;; (log/debug "res: " res)
    ;; (log/debug "res2: " (vec (flatten res)))
    (vec (flatten res))))

(defmethod keychain clojure.lang.PersistentVector
  [^clojure.lang.PersistentVector v]
  (log/debug "keychain IPersistentVector: " v)
  (if (keychain? v) v (throw (IllegalArgumentException. (str "Invalid keychain arg " v)))))

;; (defmethod keychain migae.datastore.PersistentEntityMap
;;   [^PersistentEntityMap e]
;;   (log/debug "keychain IPersistentEntityMap: " e)
;;   (keychain (.getKey (.content e))))

;; (defmethod keychain migae.datastore.PersistentEntityHashMap
;;   [^PersistentEntityHashMap e]
;;   (log/debug "to-keychain IPersistentEntityMap: " e)
;;   (.k e))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
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
  ;; (log/debug "get-val-ds-coll" coll (type coll))
  (cond
    (list? coll) (let [a (ArrayList.)]
                     (doseq [item coll]
                       (do
                         ;; (log/debug "vector item:" item (type item))
                         (.add a (get-val-ds item))))
                     ;; (log/debug "ds converted:" coll " -> " a)
                     a)

    (map? coll) (make-embedded-entity coll)

    (set? coll) (let [s (HashSet.)]
                  (doseq [item coll]
                    (let [val (get-val-ds item)]
                      ;; (log/debug "set item:" item (type item))
                      (.add s (get-val-ds item))))
                  ;; (log/debug "ds converted:" coll " -> " s)
                  s)

    (vector? coll) (let [a (Vector.)]
                     (doseq [item coll]
                       (do
                         ;; (log/debug "vector item:" item (type item))
                         (.add a (get-val-ds item))))
                     ;; (log/debug "ds converted:" coll " -> " a)
                     a)

    :else (do
            (log/debug "HELP" coll)
            coll))
    )

(defn get-val-ds
  [v]
  ;; (log/debug "get-val-ds" v (type v))
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
                          (log/debug "ELSE: get val type" v (type v))
                          v))]
    ;; (log/debug "get-val-ds result:" v " -> " val "\n")
    val))

(defn entity-key
  ([keychain]
   {:pre [(proper-keychain? keychain)]}
  ;; (log/debug "keychain-to-key: " keychain (type keychain) " : " (vector? keychain))
   (if (proper-keychain? keychain)
     (let [k (keyword-to-key (first keychain))
           root (KeyFactory$Builder. k)]
       (.getKey (doto root (add-child-keylink (rest keychain)))))
     (throw (IllegalArgumentException.
             (str "Invalid keychain: " keychain))))))
