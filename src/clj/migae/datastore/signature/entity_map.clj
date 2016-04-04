(ns migae.datastore.signature.entity-map
  ;; (:import [com.google.appengine.api.datastore
  ;;           DatastoreFailureException
  ;;           DatastoreService
  ;;           DatastoreServiceFactory
  ;;           DatastoreServiceConfig
  ;;           DatastoreServiceConfig$Builder
  ;;           Key])
  (:require [clojure.tools.logging :as log :only [debug info]]
            [migae.datastore.schemata :as sch :refer [schemata]]
            ))

(clojure.core/println "loading migae.datastore.signature.entity-map")

;; NOTE: including these definitions effectively makes them part of
;; the entity-map signature, even though they are not part of the
;; Entity-Map protocol.  This effectively makes them "global" in the
;; sense that their semantics are fixed - that is, their model is
;; fixed and cannot be dynamically altered, unlike the semantics of
;; the Protocol operators.
(defn register-schema
  [k schema]
  ;; FIXME: validation
  (if (and (vector? k)
           (not (empty? k))
           (every?
            #(and (keyword? %) (not (nil? (namespace %))))
           k))
    (swap! sch/schemata assoc k schema)
    (throw (IllegalArgumentException. (str "First arg must be keyword")))))

(defn dump-schemata []
  (log/debug "schemata: " @sch/schemata))

(defn schema
  [kw]
  (@sch/schemata kw))

(defprotocol Entity-Map
  "protocol for entity maps"
  (entity-map? [em] [k m])
  (entity-map  [k] [k m] [k m mode]) ;; local collection ctor
  (entity-map! [k] [k m] [k m mode]) ;; push ctor
  (entity-map* [k] [k m] [k m mode]) ;; pull ctor
  (entity-map$ [k] [k m] [k m mode]) ;; local entity ctor
  (entity-map=? [em1 em2])
  (map=? [em1 em2])
  ;; native stuff
  (entity? [e])
  ;; utils
  (dump [arg])
  (dump-str [arg])
  )

(defprotocol Entity-Key
  "protocol for keychains and entity keys"
  (keychain [arg])
  (keychain? [arg])
  (keychain=? [k1 k2])
  (key=? [k1 k2])
  (kind [em])
  (identifier [em])
  (entity-key [k])
  (entity-key? [k])
  )

;; Model declarations (`extend` statements) for this signature must
;; refer to the protocols declared here, so model namespaces must
;; require the namespaces of the signatures they model.  The protocol
;; declarations, by contrast, do not and should not refer to the model
;; declarations; that would defeat the purpose of separating signature
;; (protocol) from model (implementation).  However, as a practical
;; matter, we want to make the API (that is, the signature) available
;; via a single namespace; we do not want the user to have to require
;; multiple namespaces in a particular order in order to get the
;; buttery goodness of our library.  So we want it to be enough to
;; require this ns.  But then client code will call the functions
;; declared here, and will crash if we have not also loaded the
;; models:

;; java.lang.IllegalArgumentException: No implementation of
;; method: :entity-map! of protocol:
;; #'migae.datastore.signature.entity-map/Entity-Map found for class:
;; clojure.lang.PersistentVector

;; This means that the mapping from the function declaration here to an
;; implementation has not been declared.  The way to declare such a
;; mapping is to use an `extend` expression, which is what the code in
;; our model namespace does.  So we need to load it here - but NOT
;; using (ns (:require ...)).  That would create a bogus circular
;; dependency - bogus because we do not really need to _refer_ to
;; anything in the model namespace, we just need to load it so the
;; model mappings get declared.  In other words, we're only interested
;; in the side effects of loading it.  Putting the require statement
;; here does not seem to create that circular dependency.

;; NB: we want to load this *after* the protocol declarations, since
;; it in turn loads stuff that depends on those declarations.

;; this must come first, to declare our deftypes
(require '(migae.datastore.types [entity-map-seq entity-map store-map]))

;; then comes "defmodel" - i.e. "extend"
(require '[migae.datastore.model.entity-map])
