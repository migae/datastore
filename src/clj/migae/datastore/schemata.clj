(ns migae.datastore.schemata
  (:require [clojure.tools.logging :as log :only [debug info]]
            ;; [clojure.tools.reader.edn :as edn]
            [schema.core :as s])) ;; :include-macros true]

(clojure.core/println "loading migae.datastore.schemata")

;;  keys:  :Schema/#version-number, e.g. :Persons/#1
(defonce schemata (atom {}))

(defn schema
  [kw]
  (@schemata kw))

(defn register-schema
  [kw schema]
  (log/debug "register-schema: " kw schema)
  ;; FIXME: validation
  (swap! schemata assoc kw schema))

(defn dump [] (log/debug "schemata: " @schemata))
