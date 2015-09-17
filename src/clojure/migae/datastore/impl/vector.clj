(ns migae.datastore.impl.vector
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
           migae.datastore.PersistentEntityMap)
  (:require [clojure.tools.logging :as log :only [debug info]]
            [clojure.tools.reader.edn :as edn]))

(clojure.core/println "loading migae.datastore.impl.map")

(defn entity-map? [m]
  (log/debug "IPersistentVector.entity-map?" (meta m) m (type m))
  (not (nil? (:migae/keychain (meta m)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;  key stuff
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(declare get-val-ds get-val-ds-coll)

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


(defmulti identifier class)
(defmethod identifier Key
  [^Key k]
  ;; (log/debug "Key identifier" k)
  (let [nm (.getName k)
        id (.getId k)]
    (if (nil? nm) id (str nm))))
(defmethod identifier migae.datastore.PersistentEntityMap
  [^migae.datastore.PersistentEntityMap em]
  ;; (log/debug "PersistentEntityMap.identifier")
  (let [k (.getKey (.content em))
        nm (.getName k)
        id (.getId k)]
    (if (nil? nm) id (str nm))))
;; (defmethod identifier migae.datastore.PersistentEntityHashMap
;;   [^migae.datastore.PersistentEntityHashMap em]
;;   ;; (log/debug "PersistentEntityHashMap.identifier")
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

(defn keyword-to-key
  [^clojure.lang.Keyword k]
   "map single keyword to key."
;;     {:pre [(= (type k) clojure.lang.Keyword)]}
     ;; (log/debug "keyword-to-key:" k (type k))
     (if (not (= (type k) clojure.lang.Keyword))
        (throw (IllegalArgumentException.
                (str "not a clojure.lang.Keyword: " k))))
       ;; (throw (RuntimeException. (str "Bad keylink (not a clojure.lang.Keyword): " k))))
     (let [kind (clojure.core/namespace k)
           ident (edn/read-string (clojure.core/name k))]
       ;; (log/debug (format "keychain-to-key 1: kind=%s, ident=%s" kind ident))
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

(defn keylink?
  [k]
  ;; (log/debug "keylink?" k (and (keyword k)
  ;;                              (not (nil? (namespace k)))))
  (or (and (keyword? k)
           (not (nil? (namespace k))))
      (= (type k) com.google.appengine.api.datastore.Key)))

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

(defn keylink?
  [k]
  ;; (log/debug "keylink?" k (and (keyword k)
  ;;                              (not (nil? (namespace k)))))
  (or (and (keyword? k)
           (not (nil? (namespace k))))
      (= (type k) com.google.appengine.api.datastore.Key)))

(defn proper-keychain?
  [k]
  {:pre [(and (vector? k) (not (empty? k)))]}
  ;; (log/debug "proper-keychain?: " k)
  (if (every? keylink? k)
      true
      false))
(defn improper-keychain?
  [k]
  {:pre [(and (vector? k) (not (empty? k)))]}
  ;; (log/debug "improper-keychain?: " k)
  (if (every? keylink? (butlast k))
    (let [dogtag (last k)]
      ;; (log/debug "DOGTAG K" dogtag)
      (and (keyword? dogtag)
           (nil? (namespace dogtag))))
    false))

(defn keychain-to-key
  ;; FIXME: validate keychain only keylinks
  ([keychain]
  ;; (log/debug "keychain-to-key: " keychain (type keychain) " : " (vector? keychain))
   (if (proper-keychain? keychain)
     (let [k (keyword-to-key (first keychain))
           root (KeyFactory$Builder. k)]
       (.getKey (doto root (add-child-keylink (rest keychain)))))
     (throw (IllegalArgumentException.
             (str "Invalid keychain: " keychain))))))

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

(defn entity-map
  "IPersistentVector.entity-map: local constructor"
  ([k]
   (log/debug "IPersistentVector.entity-map 1" (meta k) k (type k))
   (with-meta {} {:migae/keychain k}))
  ([k m]
   (log/debug "IPersistentVector.entity-map 2" k m)
   (let [em (with-meta m {:migae/keychain k})]
     (log/debug "em: " (meta em) em)
     em))
  ([k m mode]
   {:pre [(= mode :em)]}
   (log/debug "IPersistentVector.entity-map :em" k m)
   (if (empty? k)
     (throw (IllegalArgumentException. "keychain vector must not be empty"))
     (let [ds (DatastoreServiceFactory/getDatastoreService)
           k (keychain-to-key k)
           foo (log/trace "k: " k)
           e (Entity. k)]
       (doseq [[k v] m]
         (.setProperty e (subs (str k) 1) (get-val-ds v)))
       (PersistentEntityMap. e nil))))
  )

;; (defn entity-map
;;   "PersistentEntityMap local constructor"
;;   ([k]
;;    "Construct empty entity-map"
;;    (log/debug "ctor-local.entity-map k" k (type k))
;;    (if (empty? k)
;;      (throw (IllegalArgumentException. "keychain vector must not be empty"))
;;      (let [k (keychain-to-key k)
;;            e (Entity. k)]
;;        (->PersistentEntityMap e nil))))
;;   ([keychain em]
;;    {:pre [(map? em)
;;           (vector? keychain)
;;           (not (empty? keychain))
;;           (every? keylink? keychain)]}
;;    (log/debug "ctor-local.entity-map " keychain em)
;;    (if (empty? keychain)
;;      (throw (IllegalArgumentException. "keychain vector must not be empty"))
;;      (let [k (keychain-to-key keychain)
;;            ;; foo (log/trace "k: " k)
;;            e (Entity. k)]
;;        (doseq [[k v] em]
;;          (.setProperty e (subs (str k) 1) (get-val-ds v)))
;;        (->PersistentEntityMap e nil)))))
