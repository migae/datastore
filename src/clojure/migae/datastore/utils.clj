(in-ns 'migae.datastore)

(defn print
  [^migae.datastore.PersistentEntityMap em]
  (do
    (binding [*print-meta* true]
      (prn-str em))))

(defn eprn
  [^migae.datastore.PersistentEntityMap em]
  (binding [*print-meta* true]
    (prn-str em)))

(defn dump
  [msg datum data]
  (binding [*print-meta* true]
    (log/trace msg (pr-str datum) (pr-str data))))
