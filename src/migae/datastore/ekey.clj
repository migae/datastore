(in-ns 'migae.datastore)

(load "datastore/emap")

(declare keychain=)

(defn key? [^com.google.appengine.api.datastore.Key k]
  (= (type k) com.google.appengine.api.datastore.Key))

(defmulti key class)
(defmethod key Key
  [^Key k]
  k)
(defmethod key migae.datastore.EntityMap
  [^EntityMap e]
  (.getKey (.entity e)))
(defmethod key com.google.appengine.api.datastore.Entity
  [^Entity e]
  (.getKey e))
(defmethod key clojure.lang.Keyword
  [^clojure.lang.Keyword k]
  (keychain-to-key k))
(defmethod key clojure.lang.PersistentVector
  [kchain]
  (keychain-to-key kchain))

(defn key=
  [em1 em2]
  (if (emap? em1)
    (if (emap? em2)
      (.equals (.entity em1) (.entity em2))
      (keychain= em1 em2))
    (if (map? em1)
      (keychain= em1 em2)
      (log/trace "EXCEPTION: key= applies only to maps and emaps"))))

(defn keylink?
  [k]
  ;; (log/trace "keylink?" k (and (keyword k)
  ;;                              (not (nil? (clj/namespace k)))))
  (and (keyword k)
       (not (nil? (clj/namespace k)))))

(defn keychain? [k]
  ;; k is vector of DS Keys and clojure keywords
  (every? #(or (keyword? %) (= (type %) Key)) k))

(defn keychain=
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

;; (let [kind (edn/read-string fst)
;;       name (edn/read-string )]
;;   (KeyFactory/createKey (str kind) ident))))

;; ([^String k ^Long ident]
;;  (let [kind (edn/read-string k)
;;        name (edn/read-string ident)]
;;    (KeyFactory/createKey (str kind) ident))))

;; (if (= (type name) java.lang.Long)
;;       (KeyFactory/createKey ns n)))
;;   (KeyFactory/createKey ns n)))))

;; (log/trace "ns " ns " n: " n ", first n: " (first n))

