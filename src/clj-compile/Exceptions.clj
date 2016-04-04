(ns Exceptions)

(clojure.core/println "loading Exceptions")

(gen-class :name migae.datastore.DuplicateKeyException :extends java.lang.Exception)

(gen-class :name migae.datastore.InvalidKeychainException :extends java.lang.Exception)


