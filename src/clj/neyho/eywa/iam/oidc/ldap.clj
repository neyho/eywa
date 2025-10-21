(ns neyho.eywa.iam.oidc.ldap
  (:require
    [clj-ldap.client :as ldap]
    [clojure.tools.logging :as log]
    [environ.core :refer [env]]))


(defonce ^:dynamic *pool* nil)

(defn start
  []
  (let [ldap-config {:host (str
                             (env :ldap-server)
                             (when-some [port (env :ldap-port)]
                               (str ":" port)))
                     :bind-dn (env :ldap-bind-dn)
                     :ssl? (env :ldap-ssl)
                     :startTLS? (when-some [tls (env :ldap-tls)]
                                  (if (#{"true" "TRUE" "yes" "YES" "1"} tls)
                                    true
                                    false))
                     :password (env :ldap-password)}]
    (try
      (when-some [pool (ldap/connect ldap-config)]
        (alter-var-root #'*pool* (fn [_] pool)))
      (catch Throwable ex
        (log/errorf ex "[OIDC LDAP] Failed to initialize pool %s" ldap-config)
        nil))))

(defn validate-credentials
  ([data] (validate-credentials *pool* data))
  ([pool {:keys [username password]}]
   (when pool (ldap/bind? pool username password))))

(comment
  (validate-credentials
    pool
    {:username "xxx"
     :password "yyy"}))
