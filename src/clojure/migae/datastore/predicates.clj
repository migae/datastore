(in-ns 'migae.datastore)

(defn entity-map?
  [em]
  (= (instance? migae.datastore.IPersistentEntityMap em)))

(defn emap? ;; OBSOLETE - use entity-map?
  [em]
  (entity-map? em))

(defn entity?
  [e]
  (= (type e) Entity))

