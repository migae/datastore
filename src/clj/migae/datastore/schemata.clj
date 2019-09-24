(ns migae.datastore.schemata
  (:require [clojure.tools.logging :as log :only [debug info]]
            ;; [clojure.tools.reader.edn :as edn]
            ;; [schema.core :as s]
            )) ;; :include-macros true]

(clojure.core/println "loading migae.datastore.schemata")

;;  keys:  :Schema/#version-number, e.g. :Persons/#1
(defonce schemata (atom {}))

(defn register-schema
  [k schema]
  ;; FIXME: validation
  (if (and (vector? k)
           (not (empty? k))
           (every?
            #(and (keyword? %) (not (nil? (namespace %))))
           k))
    (swap! schemata assoc k schema)
    (throw (IllegalArgumentException. (str "First arg must be keyword")))))

(defn dump-schemata
  []
  (log/debug "schemata: " @schemata))

(defn schema
  [kw]
  (@schemata kw))

