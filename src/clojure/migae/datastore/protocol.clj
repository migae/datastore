(in-ns 'migae.datastore)
;; (ns migae.datastore.protocol
;;   (:require ;;[migae.datastore :as ds]
;;             [migae.datastore.impl.map :as emap]
;;             [clojure.tools.logging :as log :only [debug info]]
;;             ))

(clojure.core/println "loading migae.datastore.protocol")

(defprotocol Entity-Map
  "protocol for entity maps"
  (entity-map? [em])
  (entity-map [k] [k m] [k m mode])
  )

(extend clojure.lang.IPersistentMap
  Entity-Map
  {:entity-map? emap/entity-map?
   :entity-map emap/entity-map})

(extend clojure.lang.IPersistentVector
  Entity-Map
  {:entity-map? evec/entity-map?
   :entity-map evec/entity-map}
  ;; clojure.lang.IPersistentVector
  ;; {:reduce (fn [this] (log/debug "FOOOO REDUCE"))}
  )

(extend migae.datastore.IPersistentEntityMap
  Entity-Map
  {:entity-map? (fn [em]
                             ;; true
                             ;; )
                             ;;(defn entity-map?
                             ;; [em]
                             (log/debug "PersistentEntityMap.entity-map?" em (type em)
                                        (instance? migae.datastore.IPersistentEntityMap em))
                             (instance? migae.datastore.IPersistentEntityMap em))
   :entity-map (fn [em] em)
    ;; "PersistentEntityMap local constructor"
    ;; ([k]
    ;;  {:pre [(keychain? k)]}
    ;;  "Construct empty entity-map"
    ;;  ;; (log/debug "ctor-local.entity-map k" k (type k))
    ;;  (if (empty? k)
    ;;    (throw (IllegalArgumentException. "keychain vector must not be empty"))
    ;;    (let [k (keychain-to-key k)
    ;;          e (Entity. k)]
    ;;      (PersistentEntityMap. e nil)))))
  ;;  :entity-map
  ;; (entity-map entity-map)
    ;; ([keychain em]
    ;;  ;; {:pre [(map? em)
    ;;  ;;        (vector? keychain)
    ;;  ;;        (not (empty? keychain))
    ;;  ;;        (every? keylink? keychain)]}
    ;;  ;; (if (empty? keychain)
    ;;  ;;   (throw (IllegalArgumentException. "keychain vector must not be empty"))
    ;;  ;;   (let [k (keychain-to-key keychain)
    ;;  ;;         ;; foo (log/trace "k: " k)
    ;;  ;;         e (Entity. k)]
    ;;  ;;     (doseq [[k v] em]
    ;;  ;;       (.setProperty e (subs (str k) 1) (get-val-ds v)))
    ;;  ;;     (PersistentEntityMap. e nil)))))
  })

