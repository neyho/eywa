(ns neyho.eywa.server.interceptors.authentication
  (:require
   clojure.string
   clojure.java.io
   clojure.pprint
   clojure.data.json
   [clojure.tools.logging :as log]
   [environ.core :refer [env]]
   [neyho.eywa.data :refer [*PUBLIC_ROLE* *PUBLIC_USER*]]
   [neyho.eywa.iam :as iam]
   [neyho.eywa.iam.access :as access]
   [neyho.eywa.iam.oauth.core :refer [get-resource-owner]]
   [neyho.eywa.iam.oauth.token :refer [*tokens*]]
   [io.pedestal.interceptor.chain :as chain]))

(def user-data
  {:enter (fn [ctx]
            (assoc ctx :response
                   {:status 200
                    :headers {"Content-Type" "application/json"}
                    :body (dissoc (get-resource-owner (:euuid (:eywa/user ctx))) :_eid)}))})

(defn get-token [{{:keys [headers]} :request}]
  (let [{auth "authorization"} headers]
    (when (not-empty auth) (clojure.string/replace auth #"^Bearer\s+" ""))))

(defn owner->context
  "The four keys wrapped-query-executor binds into *user* / *roles* / *groups*
   / *rls*. Sole place an identity is turned into request context, so
   impersonation produces exactly the same shape as normal authentication."
  [{:keys [groups roles rls] :as user}]
  #:eywa {:user (select-keys user [:_eid :euuid :name :active])
          :roles roles
          :groups groups
          :rls rls})

(defn get-token-context
  [token]
  (let [{:keys [sub client_id]
         sub-uuid "sub:uuid"} (when token
                                (try
                                  (iam/unsign-data token)
                                  (catch Throwable _ nil)))]
    (when (some? sub)
      (let [user (get-resource-owner (or
                                      (when sub-uuid (java.util.UUID/fromString sub-uuid))
                                      sub))]
        ;; :eywa/client is the authenticated CLIENT, kept separate from the
        ;; identity so the impersonation guard can check it after the identity
        ;; has been swapped out.
        (assoc (owner->context user) :eywa/client client_id)))))

(def ^:private truthy-strings #{"true" "TRUE" "YES" "yes" "y" "1"})

(defn- env-flag-on? [k] (contains? truthy-strings (env k)))

(def authenticate
  {:name :authenticate
   :enter
   (fn [ctx]
     (let [not-authorized (assoc ctx :response {:status 403
                                                :headers {"WWW-Authenticate" "Bearer"}
                                                :body "Not authorized"})
           token (get-token ctx)
           has-token (> (count token) 6)]
       (cond
         ;;
         (and has-token (contains? (get @*tokens* :access_token) token))
         (let [token-context (get-token-context token)]
           (if (nil? token-context)
             (chain/terminate not-authorized)
             (merge ctx token-context)))
         ;;
         (and has-token (not (contains? (get @*tokens* :access_token) token)))
         (chain/terminate not-authorized)
         ;; PUBLIC fallback. access/entity-allows? etc. always check the
         ;; Public role against its explicit grants regardless of
         ;; EYWA_IAM_ENFORCE_ACCESS, so this alone can't grant more than
         ;; what Public was actually given.
         (env-flag-on? :eywa-iam-allow-public)
         (let [public (:euuid *PUBLIC_ROLE*)
               public-user #:eywa {:user (select-keys *PUBLIC_USER* [:_eid :euuid :name :active])
                                   :roles #{public}
                                   :groups #{}
                                   :rls {:user (:_eid *PUBLIC_USER*)
                                         :groups #{}
                                         :roles #{}}}]
           (merge ctx public-user))
         ;;
         :else
         (chain/terminate not-authorized))))})

;; ============================================================================
;; Impersonation — `acting_as` on the GraphQL request body
;;
;; A confidential client that is explicitly allowed may run a request as
;; another user by adding a top-level "acting_as" alongside "query"/
;; "variables". The identity is swapped WHOLE (user + roles + groups + rls),
;; so the client acts exactly as that person — RBAC and RLS both follow,
;; because everything downstream reads only the dynamic vars bound from these
;; four keys. Nothing else in the system needs to know impersonation exists.
;;
;; The `impersonate` flag is therefore the entire security boundary: a leaked
;; secret of an allowed client is as good as any account it can name. Keep it
;; off unless a service genuinely needs it.
;; ============================================================================

(defn- impersonation-error
  [status code message]
  {:status status
   :headers {"Content-Type" "application/json"}
   :body (clojure.data.json/write-str {:errors [{:message message
                                                 :extensions {:code code}}]})})

(defn acting-as
  "Top-level `acting_as` from the JSON body, or nil. Reads [:request :body],
   which lacinia's body-data-interceptor has already slurped to a string and
   leaves in place; extra keys are ignored by graphql-data-interceptor.

   This sits on every GraphQL request, and nearly none of them impersonate —
   so a substring test rules the common case out before paying for a parse
   that graphql-data-interceptor is about to do again anyway."
  [ctx]
  (let [body (get-in ctx [:request :body])]
    (when (and (string? body)
               (clojure.string/includes? body "acting_as"))
      (try
        (let [v (get (clojure.data.json/read-str body) "acting_as")]
          (when-not (clojure.string/blank? v) v))
        (catch Throwable _ nil)))))

(def impersonate
  {:name ::impersonate
   :enter
   (fn [ctx]
     (if-let [target (acting-as ctx)]
       (let [client-id (:eywa/client ctx)
             client (when client-id (iam/get-client client-id))
             deny (fn [status code message]
                    (log/warnf "[Impersonation] Refused %s acting as %s: %s"
                               client-id target code)
                    (chain/terminate
                     (assoc ctx :response (impersonation-error status code message))))]
         ;; Severity order, first match wins — same cascade as Synthigy's
         ;; server/data.clj so behaviour is comparable across the two.
         (cond
           (nil? client-id)
           (deny 403 "CLIENT_REQUIRED" "acting_as requires a client-authenticated token")

           (nil? client)
           (deny 403 "CLIENT_NOT_FOUND" "Client not found")

           (false? (:active client))
           (deny 403 "CLIENT_INACTIVE" "Client is inactive")

           (#{:public "public"} (:type client))
           (deny 403 "PUBLIC_CLIENT_FORBIDDEN" "Public clients cannot impersonate users")

           (not (true? (get-in client [:settings "impersonate"])))
           (deny 403 "NOT_TRUSTED" "Client is not authorized for impersonation")

           :else
           (let [user (get-resource-owner target)]
             (cond
               (nil? (:euuid user))
               (deny 404 "USER_NOT_FOUND" "User not found")

               (false? (:active user))
               (deny 403 "USER_INACTIVE" "User is inactive")

               :else
               (do
                 (log/infof "[Impersonation] %s acting as %s" client-id target)
                 (merge ctx (owner->context user)))))))
       ctx))})
