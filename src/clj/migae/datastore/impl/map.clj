(ns migae.datastore.impl.map
  (:refer-clojure :exclude [read read-string])
  (:require [clojure.tools.logging :as log :only [debug trace info]]
            [clojure.tools.reader.edn :as edn :refer [read read-string]]
            [migae.datastore :as ds]
            ;; [migae.datastore.entity-map :as em]
            [migae.datastore.impl.vector :as v]
            #_[migae.datastore.impl.utils]))

(clojure.core/println "loading migae.datastore.impl.map")

(declare dump dump-str)

(defn entity-map? [m]
  ;; FIXME: improper key - is (entity-map? [:a]) true?
  ;; (log/debug "entity-map?" (meta m) m (type m))
  (if (ds/proper-keychain? (:migae/keychain (meta m)))
    true
    false))

    ;; (if (every? keylink? (butlast keychain))
    ;;   (when-let [dogtag (last keychain)]
    ;;     (not (nil? (namespace dogtag))))

(defn entity-map
  "entity-map: local constructor"
  ([m]
   (log/debug "entity-map" (meta m) m (type m))
   (if (entity-map? m)
     m
     (if-let [keychain (:migae/keychain (meta m))]
       (if (ds/improper-keychain? keychain)
         (throw (IllegalArgumentException. (str "Improper keychain: " keychain)))
         (throw (IllegalArgumentException.
                 (str "Invalid :migae/keychain " keychain " - all links must be namespaced keywords"))))
       (throw (IllegalArgumentException.
               (str "Missing metadata key ':migae/keychain'"))))))
  ([m m2] (log/trace "entity-map local ctor")))

;; FIXME
(defn entity-map!
  "entity-map bulk push ctor"
  [{kind :kind,
    type :type,
    ;; schema :schema,
    ;; {ns :ns schema :schema} :record,
    data :data
    :as arg}]
 ;; schema registered separately:
  ;; {:kind :Participant
  ;;  :type :Participant#1
  ;;  :prefix {:entity [:study/#12345]}
  ;;  :data [["Libbie" "Greenlee" "Greenlee@example.org"]
  ;;         ["Drucilla" "Sebastian" "Sebastian@example.org"]]}
  (log/debug "entity-map!" arg)
  (log/debug "    kind" kind)
  ;; (log/debug "    ns" ns)
  ;; (log/debug "    keys" keys)
  ;; (log/debug "    schema" schema)
  (log/debug "    data" data)
  ;; (log/debug "schemata: " (sch/dump-schemata))
  ;; FIXME: schema stuff
  (throw (Exception. "FIXME: reimplement w/o schema"))
  #_(let [;; ps (sch/schema type)
        v (try (s/validator ps)
               (catch Exception x (log/debug "bad schema: " (.getMessage x))))
        kws (into [] (for [fld ps] (keyword (:name fld))))
        emseq (into []
                    (for [datum data]
                      (let [rec (zipmap kws datum)
                            ;; m (with-meta rec {:migae/keychain [kind]})
                            em (v/entity-map! [kind] rec)]
                        ;; (log/debug "raw datum:" datum)
                        ;; (log/debug "entity-map:" (dump-str em))
                        em)))]
    ;; (log/debug "emseq: " emseq)
    emseq))

(defn entity-maps=?
  [em1 em2]
  (and (= (meta em1) (meta em2))
       (= em1 em2)))

;; (defn keychain? [k] (ds/keychain? k))

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
    (if (ds/keychain? k) k nil)))

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
         (if (ds/keychain? ky)
           (str (namespace (last ky))))))

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
    (with-out-str
      (prn m))))

(defn dump-str
  [m]
  (binding [*print-meta* true]
    (with-out-str
      (pr-str m))))
