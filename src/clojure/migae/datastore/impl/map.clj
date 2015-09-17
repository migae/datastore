(ns migae.datastore.impl.map
  (:require [clojure.tools.logging :as log :only [debug info]]))

(clojure.core/println "loading migae.datastore.impl.map")

(defn keylink?
  [k]
  (and (keyword? k)
       (not (nil? (namespace k)))))

(defn proper-keychain?
  [k]
  (and (vector? k)
       (not (empty? k))
       (every? keylink? k)))

(defn improper-keychain?
  [k]
  (and (vector? k)
       ;; (not (empty? k))
       (or (nil? (butlast k))
           (every? keylink? (butlast k)))
       (and (keyword? (last k))
            (nil? (namespace (last k))))))

(defn keychain?
  [k]
  (and
   (vector? k)
   (or (proper-keychain? k) (improper-keychain? k))))

(defn entity-map? [m]
  ;; FIXME: improper key - is (entity-map? [:a]) true?
  (log/debug "IPersistentMap.entity-map?" (meta m) m (type m))
  (if (proper-keychain? (:migae/keychain (meta m)))
    true
    false))

    ;; (if (every? keylink? (butlast keychain))
    ;;   (when-let [dogtag (last keychain)]
    ;;     (not (nil? (namespace dogtag))))

(defn entity-map
  "IPersistentMap.entity-map: local constructor"
  ([m]
   (log/debug "IPersistentMap.entity-map" (meta m) m (type m))
   (if (entity-map? m)
     m
     (if-let [keychain (:migae/keychain (meta m))]
       (if (improper-keychain? keychain)
         (throw (IllegalArgumentException. (str "Improper keychain: " keychain)))
         (throw (IllegalArgumentException.
                 (str "Invalid :migae/keychain " keychain " - all links must be namespaced keywords"))))
       (throw (IllegalArgumentException.
               (str "Missing metadata key ':migae/keychain'")))))))

(defn keychain?
  [k]
  (vector? k))
;; FIXME
  ;; (and
  ;;  (vector? k)
  ;;  (or (proper-keychain? k) (improper-keychain? k))))


(defn kind
  "IPersistentMap.entity-map.kind co-ctor"
  [m]
  (log/debug "IPersistentMap.entity-map.kind co-ctor" (meta m) m)
  (if-let [k (:migae/keychain (meta m))]
         (if (keychain? k)
           (keyword (namespace (last k))))))

(defn identifier
  "IPersistentMap.entity-map.identifier co-ctor"
  [m]
  (log/debug "IPersistentMap.entity-map.identifier co-ctor" (meta m) m)
  ;; FIXME
  (keyword (name (last (:migae/keychain (meta m))))))
