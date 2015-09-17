(ns migae.datastore.impl.map
  (:require [clojure.tools.logging :as log :only [debug info]]))

(clojure.core/println "loading migae.datastore.impl.map")

(defn entity-map? [m]
  (log/debug "IPersistentMap.entity-map?" (meta m) m (type m))
  (not (nil? (:migae/keychain (meta m)))))


(defn entity-map
  "IPersistentMap.entity-map: local constructor"
  ;; ([k]
  ;;  (log/debug "IPersistentMap.entity-map" (meta k) k (type k))
  ;;  (if (entity-map? k) k nil))
  ([m]
   (log/debug "IPersistentMap.entity-map" (meta m) m (type m))
   (if (entity-map? m)
     m
     nil))) ;; FIXME: throw exception
