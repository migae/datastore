(ns migae.datastore.keychain
  (:refer-clojure :exclude [name])
  (:import [com.google.appengine.api.datastore
            Entity
            Key KeyFactory KeyFactory$Builder]
           [migae.datastore IPersistentEntityMap]
           )
  (:require [clojure.core :as clj]
            [clojure.stacktrace :as stack]
            [clojure.tools.reader.edn :as edn]
            [clojure.tools.logging :as log :only [trace debug info]]
            ;;migae.datastore.PersistentEntityMap
            [migae.datastore.service :as ds]))

;; TODO: implement keychain as a deftype?  so we get
;; migae.datastore.Keychain rather than clojure.lang.PersistentVector

(clojure.core/println "reloading keychain")

(declare keychain-to-key identifier)

;; for repl:
;; (require '(migae.datastore [keychain :as k]))
;; (defn keylink?
;;   [k]
;;   ;; (log/trace "keylink? k" k)
;;   (and (keyword? k)
;;        (not (nil? (clj/namespace k)))))

;; (defn proper-keychain?
;;   [k]
;;   {:pre [(and (vector? k) (not (empty? k)))]}
;;   ;; (if (and  (vector? k) (not (empty? k)))
;;   (if (every? keylink? k)
;;       true
;;       false))
;; (defn improper-keychain?
;;   [k]
;;   {:pre [(and (vector? k) (not (empty? k)))]}
;;   (log/trace "improper-keychain?: " k)
;;   (if (every? keylink? (butlast k))
;;     (let [dogtag (last k)]
;;       (log/trace "DOGTAG K" dogtag)
;;       (and (keyword? dogtag)
;;            (nil? (clj/namespace dogtag))))
;;     false))

;; (defn keychain?
;;   [k]
;;   {:pre [(and (vector? k) (not (empty? k)))]}
;;   (or (proper-keychain? k) (improper-keychain? k)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; (declare keyword-to-key)
;; (defn add-child-keylink
;;   [^KeyFactory$Builder builder chain]
;;   ;; (log/trace "add-child-keylink builder:" builder)
;;   ;; (log/trace "add-child-keylink chain:" chain (type chain) (type (first chain)))
;;   (doseq [kw chain]
;;     (if (nil? kw)
;;       nil
;;       (if (keylink? kw)
;;         (let [k (keyword-to-key kw)]
;;           (.addChild builder
;;                      (.getKind k)
;;                      (identifier k)))
;;         (throw (IllegalArgumentException.
;;                 (str "not a clojure.lang.Keyword: " kw)))))))
;; ;; (throw (RuntimeException. (str "Bad child keylink (not a clojure.lang.Keyword): " kw)))))))

;; ;; FIXME: make this an internal helper method
;; ;; (defmethod keychain-to-key clojure.lang.Keyword
;; (defn keyword-to-key
;;   [^clojure.lang.Keyword k]
;;    "map single keyword to key."
;; ;;     {:pre [(= (type k) clojure.lang.Keyword)]}
;;      ;; (log/trace "keyword-to-key:" k (type k))
;;      (if (not (= (type k) clojure.lang.Keyword))
;;         (throw (IllegalArgumentException.
;;                 (str "not a clojure.lang.Keyword: " k))))
;;        ;; (throw (RuntimeException. (str "Bad keylink (not a clojure.lang.Keyword): " k))))
;;      (let [kind (clojure.core/namespace k)
;;            ident (edn/read-string (clojure.core/name k))]
;;        ;; (log/trace (format "keychain-to-key 1: kind=%s, ident=%s" kind ident))
;;        (cond
;;         (nil? kind)
;;         ;;(throw (RuntimeException. (str "Improper keylink (missing namespace): " k)))
;;         (throw (IllegalArgumentException.
;;                 (str "missing namespace: " k)))
;;         (integer? ident)
;;         (KeyFactory/createKey kind ident)
;;         (symbol? ident)                  ;; edn reader makes symbols
;;         (let [s (str ident)]
;;           (cond
;;            (= (first s) \d)
;;            (let [id (edn/read-string (apply str (rest s)))]
;;              (if (= (type id) java.lang.Long)
;;                (KeyFactory/createKey kind id)
;;                (KeyFactory/createKey kind s)))
;;            (= (first s) \x)
;;            (let [id (edn/read-string (str "0" s))]
;;              (if (= (type id) java.lang.Long)
;;                (KeyFactory/createKey kind id)
;;                (KeyFactory/createKey kind s)))
;;            :else
;;            (KeyFactory/createKey kind s)))
;;         :else
;;         (throw (IllegalArgumentException.
;;                 (str k))))))
;;         ;; (throw (RuntimeException. (str "Bad keylink: " k))))))

;; ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; ;; FIXME: restrict keychain-to-key to proper keychains
;; ;; (defmethod keychain-to-key clojure.lang.PersistentVector
;; (defn keychain-to-key
;;   ;; FIXME: validate keychain only keylinks
;;   ([keychain]
;;   ;; (log/trace "keychain-to-key: " keychain (type keychain) " : " (vector? keychain))
;;   ;; (stack/print-cause-trace (throw (RuntimeException. "foo")) 99)
;;    (if (proper-keychain? keychain)
;;      (let [k (keyword-to-key (first keychain))
;;            root (KeyFactory$Builder. k)]
;;        (.getKey (doto root (add-child-keylink (rest keychain)))))
;;      (throw (IllegalArgumentException.
;;              (str "Invalid keychain: " keychain))))))
;; ;; (throw (RuntimeException. (str "Bad keychain (not a vector of keywords): " keychain))))))



;; (defmulti keychain-to-key
;;   "Make a datastore Key from a Clojure vector of keywords.  For
;;   numeric IDs with keywords use e.g. :Foo/d123 (decimal) or :Foo/x0F (hex)"
;;   class)

  ;; (fn [keychain]
  ;;   (log/trace "keychain: " keychain (type keychain))
  ;;   (if (= (type keychain) clojure.lang.Keyword)
  ;;     clojure.lang.Keyword
  ;;     (do
  ;;       (if (every? keylink? keychain)
  ;;         (do
  ;;           (log/trace "proper keychain " keychain)
  ;;           (type keychain))
  ;;         ;;[(type arg) (type keychain)])
  ;;         (do
  ;;           (log/trace "throw bad keychain arg")))))))

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

;; (defmethod keychain-to-key [Key nil]
;;   [k] k)

;; (defmethod keychain-to-key [clojure.lang.PersistentVector nil]
;;   ([^clojure.lang.Keyword k]
;;    (
;;    ))

;; (defmethod keychain-to-key clojure.lang.Keyword ;; clojure.lang.PersistentVector$ChunkedSeq]
;;   ;; vector of keywords, string pairs, or both
;;   ;; ([head & chain]
;;   ([keylink]
;;      (log/trace "keychain-to-key Keyword ChunkedSeq" keylink) ;; head chain)
;;      (flush)
;;      ;; (let [root (KeyFactory$Builder. (clj/namespace head)
;;      ;;                                 ;; FIXME: check for IDs too, e.g. :Foo/d99, :Foo/x0F
;;      ;;                                 (clj/name head))]
;;      (let [k (keychain-to-key head)
;;            root (KeyFactory$Builder. k)]
;;        (.getKey (doto root (add-child-keylink chain))))))


;; (defmethod keychain-to-key [clojure.lang.Keyword clojure.lang.ArraySeq]
;;   ;; vector of keywords, string pairs, or both
;;   ([head & chain]
;;      ;; (log/trace "kch Keyword ArraySeq" head chain)
;;      ;; (let [root (KeyFactory$Builder. (clj/namespace head)
;;      ;;                                 ;; FIXME: check for IDs too, e.g. :Foo/d99, :Foo/x0F
;;      ;;                                 (clj/name head))]
;;      (let [k (keychain-to-key head)
;;            root (KeyFactory$Builder. k)]
;;        (.getKey (doto root (add-child-keylink chain))))))


;; (if (empty? (first (seq chain)))
;;   head
;;   (key (first chain) (rest chain)))))

;; (defmethod keychain-to-key [clojure.lang.MapEntry nil]
;;   [^clojure.lang.MapEntry k]
;;   (log/trace "keychain-to-key MapEntry nil" k) (flush)
;;   (let [kind (clojure.core/namespace k)
;;         ident (edn/read-string (clojure.core/name k))]
;;     ;; (log/trace (format "keychain-to-key 1: kind=%s, ident=%s" kind ident))
;;     (cond
;;      (nil? kind)
;;      k ;; Keyword arg is of form :Foo, interpreted as Kind
;;      ;; (let [e (Entity. (str ident))
;;      ;;       k (.put (ds/datastore) e)]
;;      ;;   (log/trace "created entity with key " k)
;;      ;;   k)
;;      (integer? ident)
;;      (KeyFactory/createKey kind ident)
;;      (symbol? ident)                  ;; edn reader makes symbols
;;      (let [s (str ident)]
;;        (cond
;;         (= (first s) \d)
;;         (let [id (edn/read-string (apply str (rest s)))]
;;           (if (= (type id) java.lang.Long)
;;             (KeyFactory/createKey kind id)
;;             (KeyFactory/createKey kind s)))
;;         (= (first s) \x)
;;         (let [id (edn/read-string (str "0" s))]
;;           (if (= (type id) java.lang.Long)
;;             (KeyFactory/createKey kind id)
;;             (KeyFactory/createKey kind s)))
;;         :else
;;         (KeyFactory/createKey kind s))))))

;; (defmethod keychain-to-key [com.google.appengine.api.datastore.Key clojure.lang.ArraySeq]
;;   ;; vector of keywords, string pairs, or both
;;   ([head & chain]
;;      ;; (log/trace "keychain-to-key Key ArraySeq" head chain)
;;      (let [root (KeyFactory$Builder. head)
;;            k (if (> (count chain) 1)
;;                (.getKey (doto root (add-child-keylink chain)))
;;                (.getKey (doto root (add-child-keylink chain))))]
;;        ;; (add-child-keylink root chain))]
;;        ;; (log/trace "keychain-to-key Key ArraySeq result: " k)
;;        k)))
;; ;; (let [k (first chain)]
;; ;;   (if

;; (defmethod keychain-to-key [java.lang.String clojure.lang.ArraySeq]
;;   ;; vector of keywords, string pairs, or both
;;   ([head & chain]
;;      (log/trace "str str")))

;; (defmethod keychain-to-key [clojure.lang.ArraySeq nil]
;;   ;; vector of keywords, string pairs, or both
;;   ([head & chain]
;;      (log/trace "seq nil" head chain)))

;; (defmethod keychain-to-key [clojure.lang.PersistentVector$ChunkedSeq nil]
;;   ;; vector of keywords, string pairs, or both
;;   ([head & chain]
;;      (log/trace "keychain-to-key ChunkedSeq nil:" head chain)
;;      (keychain-to-key (first head) (rest head))))

;; (defmethod keychain-to-key [clojure.lang.PersistentList$EmptyList clojure.lang.ArraySeq]
;;   ([head & chain]
;;      (log/trace "emptylist arrayseq: " head chain)
;;      ))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; (defmulti identifier class)

;; (defmethod identifier Key
;;   [^Key k]
;;   ;; (log/trace "Key identifier" k)
;;   (let [nm (.getName k)
;;         id (.getId k)]
;;     (if (nil? nm) id nm)))

;; (defmethod identifier migae.datastore.PersistentEntityMap
;;   [^PersistentEntityMap em]
;;   (log/trace "EM identifier" (.content em))
;;   (let [k (.getKey (.content em))
;;         nm (.getName k)
;;         id (.getId k)]
;;     (if (nil? nm) id nm)))

;; (defn- make-embedded-entity
;;   [m]
;;   {:pre [(map? m)]}
;;   (let [embed (EmbeddedEntity.)]
;;     (doseq [[k v] m]
;;       ;; FIXME:  (if (map? v) then recur
;;       (.setProperty embed (subs (str k) 1) (get-val-ds v)))
;;     embed))
