(ns migae.datastore.key
  (:refer-clojure :exclude [name hash])
  (:import [com.google.appengine.api.datastore
            KeyFactory
            Key]))

(defn kind
  [^Key key]
  (.getKind key))

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
  (fn [{key :_key kind :_kind name :_name id :_id parent :_parent :as args}]
    (cond
     (= (type args) com.google.appengine.api.datastore.Key) :key
     parent :parent
     key  :keymap
     name :name
     id   :id
     kind :pred
     :else :bug)))
    ;; (cond
    ;;  ;;  (type (first args)) com.google.appengine.api.datastore.Key) :parent
    ;;  (= (type (first args)) java.lang.String) :child
    ;;  (= (type (first args)) java.lang.String) :name
    ;;  (= (type (first args))java.lang.Long) :id)))

;; (defmethod make :parent
;;  [{parent :_parent kind :_kind name :_name}]
;;  (KeyFactory/createKey parent (clojure.core/name kind) name))

(defmethod make :parent
  [{key :_key kind :_kind name :_name id :_id parent :_parent :as args}]
 (let [pkind (:_kind parent)
       pname (:_name parent)
       pKey (KeyFactory/createKey (clojure.core/name pkind) pname)]
    (KeyFactory/createKey pKey (clojure.core/name kind) name)))

(defmethod make :name
  [{key :_key kind :_kind name :_name id :_id parent :_parent :as args}]
; [kind name]
 (KeyFactory/createKey (clojure.core/name kind) name))

(defmethod make :id
  [{key :_key kind :_kind name :_name id :_id parent :_parent :as args}]
; [kind id]
 (KeyFactory/createKey (clojure.core/name kind) id))


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
