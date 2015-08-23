(ns migae.datastore.entity
  (:refer-clojure :exclude [name hash key])
  (:import [com.google.appengine.api.datastore
            Entity
            Key])
  (:require [clojure.tools.logging :as log :only [trace debug info]]))


(defmulti key
  (fn [entity]
    (do ; (log/trace "dispatching entity " entity (meta entity))
        (cond
         (= (type entity) com.google.appengine.api.datastore.Entity) :entity
         (map? entity) :map
         :else :bug))))

(defmethod key :entity
  [ent] (.getKey ent))

(defmethod key :map [entity]
  (do ;;(log/trace "key :map " entity)
      ;;(log/trace "meta: " (:_entity (meta entity)))
;      (:_key (meta ent))))
      (.getKey (:_entity (meta entity)))))

(defmethod key :bug [ent] (log/trace "method :bug, " ent))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defmulti name
  (fn [entity]
    (do
        (cond
         (= (type entity) com.google.appengine.api.datastore.Entity) :entity
         (map? entity) :map
         :else :bug))))

(defmethod name :entity
  [ent] (.getName (.getKey ent)))

(defmethod name :map [entity]
  (do
    (:_name (meta entity))))


;; (defmethod key :kindname
;;   [ent] (.getKey ent))

(defn prop [ent prop] (.getProperty ent prop))
