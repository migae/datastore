(ns migae.datastore.keychain
  (:import [com.google.appengine.api.datastore
            Entity
            Key KeyFactory KeyFactory$Builder])
  (:require [clojure.core :as clj]
            [clojure.tools.reader.edn :as edn]
            [clojure.tools.logging :as log :only [trace debug info]]))

(declare keychain-to-key identifier)
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defmulti to-keychain
  "to-keychain converts a DS Key to a vector of Clojure keywords"
  class)

;; (defmethod to-keychain migae.datastore.EntityMap
;;   [^EntityMap em]
;;   (log/trace "to-keychain EntityMap: " em)
;;   (to-keychain (.getKey (.entity em))))

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

;; (defmethod identifier migae.datastore.EntityMap
;;   [^EntityMap em]
;;   (log/trace "EM identifier" (.entity em))
;;   (let [k (.getKey (.entity em))
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
