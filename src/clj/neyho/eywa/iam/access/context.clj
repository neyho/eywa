(ns neyho.eywa.iam.access.context)

(defonce ^:dynamic *user* nil)
(defonce ^:dynamic *roles* nil)
(defonce ^:dynamic *groups* nil)
(defonce ^:dynamic *rules* nil)
(defonce ^:dynamic *scopes* nil)
(defonce ^:dynamic *rls* nil)
;; Whether EYWA_IAM_ENFORCE_ACCESS is on. *rules*/*scopes* are always loaded so
;; the Public role can be checked against its explicit grants regardless of
;; this flag; *enforce* only decides whether authenticated (non-public) roles
;; are also checked against them.
(defonce ^:dynamic *enforce* nil)
