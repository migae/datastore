(ns migae.datastore.impl.utils
  (:refer-clojure :exclude [read read-string])
  (:require [clojure.tools.logging :as log :only [debug info]]
            [clojure.tools.reader.edn :as edn :refer [read read-string]]))

(clojure.core/println "loading migae.datastore.impl.utils")

(defn valid-emap?
  [m]
  ;; (log/debug "valid-emap?" m)
  ;; insert schema validation here
  (map? m))

