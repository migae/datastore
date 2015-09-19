(in-ns 'datastore)

(defn keychain=?
  [k1 k2]
  (let [kch1 (if (emap? k1)
               ;; recur with .getParent
               (if (map? k1)
                 (:migae/key (meta k1))))
        kch2 (if (emap? k2)
               ;; recur with .getParent
               (if (map? k2)
                 (:migae/key (meta k2))))]
    ))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn entity-map=?
  [em1 em2]
  ;; FIXME:  pre: validate types
  (and (key=? em1 em2) (map=? em1 em2)))
      ;; (log/debug "EXCEPTION: key= applies only to maps and emaps"))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defmulti map=?
  "Datastore entity equality predicate.  True if comparands have same
  keys and values.  See also key=? and map=?"
  (fn [em1 em2]
    [(type em1) (type em2)]))

(defmethod map=? [PersistentEntityMap PersistentEntityMap]
  [em1 em2]
  (if (instance? migae.datastore.IPersistentEntityMap em1)
    (if (instance? migae.datastore.IPersistentEntityMap em2)
      (do
        (log/debug "comparing 2 PersistentEntityMaps")
        (let [this-map (.getProperties (.content em1))
              that-map (.getProperties (.content em2))]
          (log/debug "this-map:" this-map (type this-map))
          (log/debug "that-map:" that-map (type that-map))
          (log/debug "(= this-map that-map)" (clojure.core/= this-map that-map))
          (clojure.core/= this-map that-map)))
      false) ; FIXME
    false)  ; FIXME
  )

(defn hybrid=
  [em1 em2]
  (let [this-map (ds-to-clj (.getProperties (.content em1)))
        ;; FIXME: is ds-to-clj appropriate for this?  is props-to-map a better name/concept?
        that-map em2]
    (log/debug "this-map:" this-map (type this-map))
    (log/debug "that-map:" em2 (type em2))
    (log/debug "(= this-map em2)" (clojure.core/= this-map em2))
    (clojure.core/= this-map em2))
  )

(defmethod map=? [PersistentEntityMap clojure.lang.PersistentArrayMap]
  [^PersistentEntityMap pem ^clojure.lang.PersistentArrayMap pam]
  (log/debug "comparing: PersistentEntityMap PersistentArrayMap")
  (hybrid= pem pam))

(defmethod map=? [clojure.lang.PersistentArrayMap PersistentEntityMap]
  [^clojure.lang.PersistentArrayMap pam ^PersistentEntityMap pem]
  (log/debug "comparing: PersistentEntityMap PersistentArrayMap")
  (hybrid= pem pam))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; (defmethod map=? [PersistentEntityMap clojure.lang.PersistentHashMap]
;;   [^PersistentEntityMap pem ^clojure.lang.PersistentHashMap phm]
;;   (log/debug "comparing: PersistentEntityMap PersistentHashMap")
;;   (hybrid= pem phm))

;; (defmethod map=? [clojure.lang.PersistentHashMap PersistentEntityMap]
;;   [^clojure.lang.PersistentHashMap phm ^PersistentEntityMap pem]
;;   (log/debug "comparing: PersistentEntityMap PersistentHashMap")
;;   (hybrid= pem phm))

