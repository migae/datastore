(clojure.core/println "Start loading migae.datastore.protocols")

(in-ns 'migae.datastore)

(defprotocol Entity-Key
  "protocol for keychains and entity keys"
  (->keychain [k])
  (keychain? [k])
  (->Key [v])
  (Key? [v])
  ;; (entity-key? [k])
  ;; (keychains=? [k1 k2])
  (keys=? [k1 k2])
  (->kind [k])
  (->identifier [k])
  )

(defprotocol Entity-Map
  "protocol for entity maps"
  (entity-map? [em] [k m])
  (entity-map  [k] [k m] [k m mode]) ;; local collection ctor
  (entity-map! [k] [k m] [k m mode]) ;; push ctor
  (entity-map* [k] [k m] [k m mode]) ;; pull ctor
  (entity-map$ [k] [k m] [k m mode]) ;; local entity ctor
  (entity-maps=? [em1 em2])
  ;; (map=? [em1 em2])
  ;; native stuff
  (entity? [em])
  ;; key stuff
  ;; (vector->Key [em])
  (keychain [em])
  (keychains=? [k1 k2])
  (kind [em])
  (identifier [em])
  ;; utils
  (dump [arg])
  (dump-str [arg])
  )
