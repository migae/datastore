(ns migae.datastore.structure.map
  (:refer-clojure :exclude [read read-string])
  (:require [clojure.tools.logging :as log :only [debug info]]
            [clojure.tools.reader.edn :refer [read read-string]]
            [migae.datastore.keys :as k]))

(clojure.core/println "loading migae.datastore.structure.map")

(declare dump dump-str)

(defn valid-emap?
  [m]
  ;; (log/debug "valid-emap?" m)
  ;; insert schema validation here
  (map? m))

(defn entity-map? [m]
  ;; FIXME: improper key - is (entity-map? [:a]) true?
  ;; (log/debug "entity-map?" (meta m) m (type m))
  (if (k/proper-keychain? (:migae/keychain (meta m)))
    true
    false))

    ;; (if (every? keylink? (butlast keychain))
    ;;   (when-let [dogtag (last keychain)]
    ;;     (not (nil? (namespace dogtag))))

(defn entity-map
  "entity-map: local constructor"
  ([m]
   ;; (log/debug "entity-map" (meta m) m (type m))
   (if (entity-map? m)
     m
     (if-let [keychain (:migae/keychain (meta m))]
       (if (k/improper-keychain? keychain)
         (throw (IllegalArgumentException. (str "Improper keychain: " keychain)))
         (throw (IllegalArgumentException.
                 (str "Invalid :migae/keychain " keychain " - all links must be namespaced keywords"))))
       (throw (IllegalArgumentException.
               (str "Missing metadata key ':migae/keychain'")))))))

(defn keychain? [k] (k/keychain? k))

(defn keychain=?
  [k1 k2]
  (throw (RuntimeException. "not implemented yet"))
  (let [kch1 (if (entity-map? k1)
               ;; recur with .getParent
               (if (map? k1)
                 (:migae/key (meta k1))))
        kch2 (if (entity-map? k2)
               ;; recur with .getParent
               (if (map? k2)
                 (:migae/key (meta k2))))]
    ))

(defn keychain
  [m]
  ;; (log/debug "keychain: " m)
  (let [k (:migae/keychain (meta m))]
    (if (k/keychain? k) k nil)))

(defn key=?
  [em1 em2]
  ;; FIXME:  pre: validate types
  (if (entity-map? em1)
    (if (entity-map? em2)
      (.equals (.content em1) (.content em2))
      (keychain=? em1 em2))
    (if (map? em1)
      (keychain=? em1 em2)
      (log/debug "EXCEPTION: key= applies only to maps and emaps"))))

(defn keychain=?
  [k1 k2]
  (let [kch1 (if (entity-map? k1)
               ;; recur with .getParent
               (if (map? k1)
                 (:migae/key (meta k1))))
        kch2 (if (entity-map? k2)
               ;; recur with .getParent
               (if (map? k2)
                 (:migae/key (meta k2))))]
    ))

(defn kind
  "co-construct kind from an entity-map"
  [m]
  ;; (log/debug "entity-map.kind" (meta m) m)
  (if-let [ky (:migae/keychain (meta m))]
         (if (k/keychain? ky)
           (keyword (namespace (last ky))))))

;; (defn identifier
;;   "entity-map.identifier co-ctor"
;;   [m]
;;   (log/debug "entity-map.identifier co-ctor" (meta m) m)
;;   ;; FIXME
;;   (let [dogtag (last (:migae/keychain (meta m)))]
;;         nm (.getName k)
;;         id (.getId k)]
;;     (if (nil? nm) id (str nm)))

(defn identifier
  "identifier co-ctor"
  [m]
  ;; (log/debug "identifier co-ctor" (dump-str m))
  ;; FIXME coverage and tests
  (let [dogtag (last  (:migae/keychain (meta m)))]
    (when-let [ns (namespace dogtag)]
      (let [id (name dogtag)
            base (first (take 1 id))]
        ;; (log/debug "base: " base)
        (if (= base \#)
          (let [n (read-string (drop 1 id))]
            ;; (log/debug "n: " n (type n))
            (if (number? n)
              (do ;; (log/debug "number: " n)
                  n)
              (do ;; (log/debug "Invalid number string after #: " id)
                  (throw (IllegalArgumentException. (str "Invalid number string after #: " id))))))
          (let [edn-id (read-string id)]
            (if (number? edn-id)
              (do ;; (log/debug "numeric id: " edn-id)
                  edn-id)
              (do ;; (log/debug "string id: " id)
                  id))))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;  utils
(defn dump
  [m]
  (binding [*print-meta* true]
    (prn m)))

(defn dump-str
  [m]
  (binding [*print-meta* true]
    (pr-str m)))
