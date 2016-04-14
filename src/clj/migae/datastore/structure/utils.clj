(ns migae.datastore.structure.utils
  (:refer-clojure :exclude [read read-string])
  (:require [clojure.tools.logging :as log :only [debug info]]
            [clojure.tools.reader.edn :as edn :refer [read read-string]]
            [schema.core :as s])) ;; :include-macros true]
            ;; [migae.datastore.schemata :as schemata]))

(clojure.core/println "loading migae.datastore.structure.utils")

(defn valid-emap?
  [m]
  ;; (log/debug "valid-emap?" m)
  ;; insert schema validation here
  (map? m))

