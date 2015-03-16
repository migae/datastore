(ns migae.datastore.key
  (:refer-clojure :exclude [name hash])
  (:require [clojure.tools.logging :as log :only [trace debug info]]
            [clojure.core :as clj])
  (:import java.lang.NumberFormatException
           [com.google.appengine.api.datastore
            KeyFactory
            KeyFactory$Builder
            Key]))

(defn kind
  [^Key key]
  (keyword (.getKind key)))

(defn name
  [^Key key]
  (.getName key))

(defn id
  [^Key key]
  (.getId key))

(defn name-space
  [^Key key]
  (.getNamespace key))

(defn parent
  [^Key key]
  (.getParent key))

(defn compareTo
  [^Key key]
  (.compareTo key))

(defn equals
  [^Object obj]
  (.equals obj))

(defn appId
  [^Key key]
  (.getAppId key))

(defn hash
  [^Key key]
  (.hashCode key))

;; ################
(defmulti child
  (fn [key {kind :_kind name :_name id :_id}]
    (cond
     name :name
     id :id)))

(defmethod child :name
 [key {kind :_kind name :_name}]
 (.getChild key (clojure.core/name kind) name))

(defmethod child :id
 [key {kind :_kind id :_id}]
 (.getChild key kind id))

;; ################
(defmulti make
  (fn
    ;; one arg, a map
    ([{_kind :_kind key val :as args}]
     (if (symbol? args) :sym
         (if _kind :kind)))
    ;; two or more args - ancestor-descendant chain
    ;; ([parent child gc ggc] :chain)
    ;; ([parent child gc] :chain)
    ([parent & descendants] :chain)
    ;; ([{ :_key _kind :_kind _name :_name _id :_id _parent :_parent :as args}]
    ;; ([{_kind :_kind _name :_name _id :_id} children]
    ;;  (cond
    ;;    ;; (= (type args) com.google.appengine.api.datastore.Key) :key
    ;;     ;; _parent :parent
    ;;    ;; _key  :keymap
    ;;    _kind :kind
    ;;    _name :name
    ;;    _id   :id
    ;;    :else :bug))))
    ))

    ;; (cond
    ;;  ;;  (type (first args)) com.google.appengine.api.datastore.Key) :parent
    ;;  (= (type (first args)) java.lang.String) :child
    ;;  (= (type (first args)) java.lang.String) :name
    ;;  (= (type (first args))java.lang.Long) :id)))

;; (defmethod make :parent
;;  [{parent :_parent kind :_kind name :_name}]
;;  (KeyFactory/createKey parent (clojure.core/name kind) _name))

(defmethod make :parent
  [{_key :_key _kind :_kind _name :_name _id :_id _parent :_parent :as args}]
  (log/trace (str "key/make :parent (" _parent ")"))
  (cond
    (= (type _parent) com.google.appengine.api.datastore.Key)
    (let [pkind (_kind _parent)
          pname (_name _parent)
          pKey (KeyFactory/createKey (clojure.core/name pkind) pname)]
      (KeyFactory/createKey pKey (clojure.core/name _kind) _name))
    :else
    (let [pkind (:_kind _parent)
          pname (:_name _parent)
          pKey (KeyFactory/createKey (clojure.core/name pkind) pname)]
      (KeyFactory/createKey pKey (clojure.core/name _kind) _name))))

(defmethod make :name
  [{_key :_key _kind :_kind _name :_name _id :_id _parent :_parent :as args}]
  (log/trace (str "key/make :name"))
  (KeyFactory/createKey (clojure.core/name _kind) _name))

(defmethod make :sym
  [sym]
  (log/trace (str "key/make :sym ") sym)
  (let [ns (clojure.core/namespace sym)
        n  (clojure.core/name sym)
        nrest (apply str (rest n))
        nbr (try (Long/parseLong nrest)
                 (catch NumberFormatException e
                   nil))]
    (log/trace "ns " ns " n: " n ", first n: " (first n))
    (if (= (first n) "_")
      (KeyFactory/createKey ns nbr)
      (KeyFactory/createKey ns n))))

;; (defmethod make :_kind [args]
;;   (log/trace "key/make :_kind"))

(defn add-children [parent descendants]
  (doseq [sym descendants]
    (log/trace "sym " sym)
    (.addChild parent (clj/namespace sym) (clj/name sym))))

(defmethod make :chain
  [parent & descendants]
  (log/trace (str "parent kind: " (clj/namespace parent) " ident: " (clj/name parent)))
  (let [root (KeyFactory$Builder. (clj/namespace parent) (clj/name parent))]
    (.getKey (doto root (add-children descendants)))))

(defmethod make :kind
  [{_key :_key _kind :_kind _name :_name _id :_id _parent :_parent :as args}]
  (if (= (count args) 2)
    (KeyFactory/createKey (clojure.core/name _kind) (if _name _name _id))
    (let [l (doseq [[k v] args]
              (log/trace (str "key/make :kind (k = " k ", v = " v ")"))
              (cond
                (= k :_kind)

                (= k :_child)))]
                (log/trace l)
                l)))

;;              (KeyFactory/createKey (clojure.core/name _kind) _name)))]

(defmethod make :id
  [{_key :_key _kind :_kind _name :_name _id :_id _parent :_parent :as args}]
  (log/trace (str "key/make :kind (" _kind " " _id")"))
 (KeyFactory/createKey (clojure.core/name _kind) _id))


;; Key k = new KeyFactory.Builder("Person", "GreatGrandpa")
;;                       .addChild("Person", "Grandpa")
;;                       .addChild("Person", "Dad")
;;                       .addChild("Person", "Me")
;;                       .getKey();
;; (defmethod make :path
;;  [{path :_path}]
;;  (-> KeyFactory$Builder (clojure.core/name kind) id)
;;  )

;; ################
(defn to-string
  [^Key key]
  (.keyToString key))

(defn string-to-key
  [^String s]
  (.stringToKey s))

;; ################
;;  KeyFactory$Builder
