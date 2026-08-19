(ns neyho.eywa.iam.oauth.token
  (:require
   [buddy.core.codecs]
   [buddy.sign.util :refer [to-timestamp]]
   [camel-snake-kebab.core :as csk]
   [clojure.data.json :as json]
   clojure.java.io
   clojure.pprint
   [clojure.string :as str]
   [clojure.tools.logging :as log]
   [io.pedestal.interceptor.chain :as chain]
   [nano-id.core :as nano-id]
   [neyho.eywa.dataset :as dataset]
   [neyho.eywa.iam
    :as iam
    :refer [sign-data]]
   [neyho.eywa.iam.access :as access]
   [neyho.eywa.iam.oauth.core :as core
    :refer [pprint
            get-client
            session-kill-hook
            access-token-expiry
            refresh-token-expiry
            process-scope
            sign-token]]
   [neyho.eywa.iam.uuids :as iu]
   [timing.core :as vura]))

(defonce ^:dynamic *tokens* (atom nil))

;; Knob: revoke the previous (session, audience) tokens at rotation time?
;;
;; false (default, fix A): NEW tokens are stored alongside the OLD ones.
;;   The OLD access token remains usable until its natural :exp. This kills
;;   the silent-renew race that produced spurious 403s on in-flight
;;   requests (2026-06-03 demo incident: token iat 10:28:04, request
;;   10:37:16, 403 — silent renew at ~9 min revoked the in-flight token).
;;   Matches what stateless IDPs (Auth0/Okta/Cognito) do by default, just
;;   layered onto EYWA's stateful *tokens* store. Two trade-offs:
;;     1. A leaked old access token is usable until exp (up to access-token
;;        lifetime — default 1h). exp is the security boundary, same as it
;;        is for every JWT-based IDP.
;;     2. set-session-tokens overwrites [session :tokens audience], so the
;;        old token loses its back-reference to the session. kill-session
;;        on logout walks session :tokens (now only new tokens) and won't
;;        clean the old entry from *tokens*. Old entries stay in *tokens*
;;        until expired, but auth still rejects them — iam/unsign-data
;;        throws on expired exp, claims become nil, authentication.clj
;;        falls through to 403. So it's a memory leak, not a security
;;        hole. A periodic sweeper for *tokens* entries past their exp is
;;        future work; until then, restart pressure on memory comes from
;;        token volume × access-token lifetime.
;;
;; true (original behavior): every rotation revokes the previous (session,
;;   audience) tokens immediately. Reintroduces the silent-renew race.
;;   Keep this path available because:
;;     a. Security-sensitive deployments may prefer the smaller leak window
;;        and accept the UX cost (frontend must catch 403 + renew + retry).
;;     b. Once the *tokens* sweeper is implemented, this whole knob may
;;        become moot — both modes will have the same memory behavior.
;;
;; Re-bindable per-call for tests; alter-var-root to flip at runtime.
(def ^:dynamic *revoke-old-tokens-on-rotation* false)

(let [alphabet "ACDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"]
  (def gen-token (nano-id/custom alphabet 50)))

(defn token-error [status code & description]
  ; (log/debugf "Returning error: %s\n%s" code (str/join "\n" description))
  {:status (if (number? status) status 400)
   :headers {"Content-Type" "application/json;charset=UTF-8"
             "Pragma" "no-cache"
             "Cache-Control" "no-store"}
   :body (json/write-str
          {:error (if (number? status) code status)
           :error_description (str/join "\n"
                                        (if (number? status) description
                                            (conj description code)))})})

(comment
  (keys @core/*sessions*)
  (token-error
   400
   "authorization_pending"
   "Evo nekog opisa"))

(defn set-session-tokens
  ([session audience tokens]
   (swap! core/*sessions* assoc-in [session :tokens audience] tokens)
   (swap! *tokens*
          (fn [current-tokens]
            (reduce-kv
             (fn [tokens token-key data]
               (log/debugf "[%s] Adding token %s %s" session token-key data)
               (assoc-in tokens [token-key data] session))
             current-tokens
             tokens)))
   nil))

(defn get-token-session
  [token-key token]
  (get-in @*tokens* [token-key token]))

(defn get-token-audience
  [token-key token]
  (let [session (get-token-session token-key token)]
    (reduce-kv
     (fn [_ audience {_token token-key}]
       (when (= token _token)
         (reduced audience)))
     nil
     (:tokens (core/get-session session)))))

(defn get-session-access-token
  ([session] (get-session-access-token session nil))
  ([session audience]
   (get-in @core/*sessions* [session :tokens audience :access_token])))

(defn get-session-refresh-token
  ([session] (get-session-refresh-token session nil))
  ([session audience]
   (get-in @core/*sessions* [session :tokens audience :refresh_token])))

(defn revoke-token
  ([token-key token]
   (let [session (get-token-session token-key token)
         audience (get-token-audience token-key token)]
     (swap! core/*sessions* update-in [session :tokens audience] dissoc token-key)
     (when token
       (swap! *tokens* update token-key dissoc token)
       (iam/publish
        :oauth.revoke/token
        {:token/key token-key
         :token/data token
         :audience audience
         :session session})))
   nil))

(defn revoke-session-tokens
  ([session]
   (doseq [audience (keys (:tokens (core/get-session session)))]
     (revoke-session-tokens session audience)))
  ([session audience]
   (let [{{tokens audience} :tokens} (core/get-session session)]
     (doseq [[token-key token] tokens]
       (revoke-token token-key token)))
   nil))

(defmethod session-kill-hook 0
  [_ session]
  (revoke-session-tokens session))

;; ============================================================================
;; Stale token sweeper
;;
;; With *revoke-old-tokens-on-rotation* = false (the new default), rotation
;; leaves the previous (session, audience) tokens in *tokens* until their
;; natural :exp. They are auth-harmless once expired (iam/unsign-data throws
;; on expired :exp → claims become nil → authentication.clj returns 403), but
;; they sit in the map taking memory until something removes them. This
;; sweeper is that something.
;;
;; The sweep reads :exp from the unsigned base64 payload — no signature
;; verification — because:
;;   1. We don't need to trust the token; we just need to know if it's expired.
;;   2. Signature verification per-token would make the sweep O(n) crypto
;;      operations. For thousands of tokens that's expensive.
;;   3. A bogus/unparseable entry (malformed payload, wrong shape) gets
;;      treated as expired and dropped. Such an entry couldn't authenticate
;;      anyway (iam/unsign-data would reject it), so dropping it is safe.
;;
;; Only *tokens* is swept. *sessions* is not touched: set-session-tokens
;; overwrites [session :tokens audience] on every rotation, so the session
;; bookkeeping only ever holds the latest tokens. Rotated tokens stop being
;; referenced from *sessions* the moment they're rotated past.
;; ============================================================================

(defn- token-exp
  "Return the :exp claim (epoch seconds) from a JWT string without
   verifying its signature. Returns nil if the string is not a parseable
   JWT or has no :exp claim — caller treats nil as 'expired, drop it'."
  [^String token]
  (try
    (let [parts (str/split token #"\.")]
      (when (>= (count parts) 2)
        (let [payload (second parts)
              decoded (String. (.decode (java.util.Base64/getUrlDecoder) ^String payload))
              claims (json/read-str decoded :key-fn keyword)]
          (:exp claims))))
    (catch Throwable _ nil)))

(defn clean-expired-tokens
  "Sweep *tokens*: drop any entry whose JWT :exp is in the past or
   unparseable. Returns a map of {token-key evicted-count}. Cheap when
   nothing's expired (one base64+JSON decode per token, no signature
   check).

   Wired into the existing OAuth maintenance agent loop in
   neyho.eywa.iam.oauth/maintenance alongside clean-sessions and
   clean-codes. The maintenance loop runs every ~30s by default."
  []
  (let [now (quot (System/currentTimeMillis) 1000)
        evicted (atom {})]
    (swap! *tokens*
           (fn [tokens-by-key]
             (reduce-kv
              (fn [acc token-key token-map]
                (let [kept (reduce-kv
                            (fn [m token session]
                              (let [exp (token-exp token)]
                                (if (or (nil? exp) (< exp now))
                                  (do (swap! evicted update token-key (fnil inc 0)) m)
                                  (assoc m token session))))
                            {}
                            token-map)]
                  (assoc acc token-key kept)))
              {}
              tokens-by-key)))
    (let [result @evicted]
      (when (seq result)
        (log/infof "[OAUTH] Token sweep: evicted %s" result))
      result)))

(comment
  (def data (gen-token))
  (revoke-session-tokens session)
  (def session "YXldcURYFGCaMkMKwqFQvUblGOlSGh")
  (vura/value->time (* 1000 (:exp (iam/unsign-data (sign-token session :refresh_token data)))))
  (def token (first (keys (get @*tokens* :refresh_token))))
  (def token (first (keys (get @*tokens* :access_token))))
  (time (vura/value->time (* 1000 (:exp (iam/unsign-data token)))))
  (core/expires-at token)

  (java.util.Date.)
  (vura/date)
  (iam/unsign-data token))

(defmethod sign-token :refresh_token
  [session _ data]
  (let [client (get-in @core/*sessions* [session :client])]
    (sign-data
     (hash-map :value data
               :session session
               :exp (->
                     (System/currentTimeMillis)
                     (quot 1000)
                     (+ (refresh-token-expiry client))))
     {:alg :rs256})))

(defmethod sign-token :access_token
  [_ _ data]
  (sign-data data {:alg :rs256}))

(def unsupported (core/json-error 500 "unsupported" "This feature isn't supported at the moment"))

(def client-id-missmatch
  (token-error
   "unauthorized_client"
   "Refresh token that you have provided"
   "doesn't belong to given client"))

(def owner-not-authorized
  (token-error
   "resource_owner_unauthorized"
   "Provided refresh token doesn't have active user"))

(def refresh-not-supported
  (token-error
   "invalid_request"
   "The client configuration does not support"
   "token refresh requests."))

(def authorization-code-not-supported
  (token-error
   "invalid_request"
   "The client configuration does not support"
   "token authorization code requests"))

(def device-code-not-supported
  (token-error
   "invalid_request"
   "The client configuration does not support"
   "token device code requests"))

(def client-credentials-not-supported
  (token-error
   "invalid_request"
   "The client configuration does not support"
   "token client credentials requests"))

(def cookie-session-missmatch
  (token-error
   "invalid_request"
   "You session is not provided by this server."
   "This action will be logged and processed!"))

(defmulti grant-token (fn [{:keys [grant_type]}] grant_type))

(defmethod grant-token :default [_] unsupported)

; (defn- issued-at? [token] (:at (meta token)))

(defn generate
  [{{allowed-grants "allowed-grants"} :settings
    :as client} session {:keys [audience scope client_id]}]
  (let [access-exp (->
                    (System/currentTimeMillis)
                    (quot 1000)
                    (+ (access-token-expiry client)))
        {user-name :name} (core/get-session-resource-owner session)
        access-token {:session session
                      :aud audience
                      :exp access-exp
                      :iss (core/domain+)
                      :sub user-name
                      :iat (-> (vura/date) to-timestamp)
                      :jti (nano-id/nano-id 20)
                      :client_id client_id
                      :sid session
                      :scope (str/join " " scope)}
        refresh? (some #(= "refresh_token" %) allowed-grants)]
    (log/debugf "Generated access token\n%s" (pprint access-token))
    (if (pos? access-exp)
      (let [refresh-token (when (and refresh? session (contains? scope "offline_access"))
                            (log/debugf "Creating refresh token: %s" session)
                            (gen-token))
            tokens (reduce
                    (fn [tokens scope]
                      (process-scope session tokens scope))
                    (if refresh-token
                      {:access_token access-token
                       :refresh_token refresh-token}
                      {:access_token access-token})
                    scope)
            signed-tokens (reduce-kv
                           (fn [tokens token data]
                             (assoc tokens token (sign-token session token data)))
                           tokens
                           tokens)]
        (when session
          ;; Gate the immediate-revoke on *revoke-old-tokens-on-rotation*.
          ;; Default false (fix A): keep old access token valid until its
          ;; natural :exp so in-flight requests don't 403 across a silent
          ;; renew. See the var docstring at the top of this ns for the
          ;; full trade-off and the future-work sweeper note.
          (when *revoke-old-tokens-on-rotation*
            (revoke-session-tokens session audience))
          (set-session-tokens session audience signed-tokens))
        (iam/publish
         :oauth.grant/tokens
         {:tokens signed-tokens
          :session session})
        (assoc signed-tokens
               :type "Bearer"
               :scope (str/join " " scope)
               :expires_in (access-token-expiry client)))
      (let [tokens (reduce
                    (fn [tokens scope]
                      (process-scope session tokens scope))
                    {:access_token access-token}
                    scope)
            signed-tokens (reduce-kv
                           (fn [tokens token data]
                             (assoc tokens token (sign-token session token data)))
                           tokens
                           tokens)]
        (iam/publish
         :oauth.grant/tokens
         {:tokens signed-tokens
          :session session})
        (assoc signed-tokens
               :expires_in (access-token-expiry client)
               :scope (str/join " " scope)
               :type "Bearer")))))

(defmethod grant-token "refresh_token"
  [{:keys [refresh_token scope audience]
    cookie-session :idsrv/session
    :as request}]
  (if (core/expired? refresh_token)
    (do
      (core/kill-session (get-token-session :refresh_token refresh_token))
      (token-error
       400
       "invalid_request"
       "Provided token is expired!"))
    (if-let [session (get-token-session :refresh_token refresh_token)]
      (let [{{:strs [allowed-grants]} :settings
             :as client} (core/get-session-client session)
            {:keys [active]} (core/get-session-resource-owner session)
            scope (or
                   scope
                   (core/get-session-audience-scope session audience))
            audience (or
                      audience
                      (get-token-audience :refresh_token refresh_token))
            current-refresh-token (get-in
                                   (core/get-session session)
                                   [:tokens audience :refresh_token])
            grants (set allowed-grants)]
        ;; Always revoke the specific refresh token used — RFC 6749
        ;; refresh-token-reuse detection; a leaked refresh token can mint
        ;; access tokens indefinitely otherwise.
        (when current-refresh-token (revoke-token :refresh_token current-refresh-token))
        ;; Broad session+audience revoke is gated the same as the
        ;; authorization_code path — see *revoke-old-tokens-on-rotation*
        ;; docstring at top of ns.
        (when (and session *revoke-old-tokens-on-rotation*)
          (revoke-session-tokens session audience))
        (cond
          ;;
          (not (contains? grants "refresh_token"))
          (do
            (core/kill-session session)
            refresh-not-supported)
          ;;
          (not active)
          (do
            (core/kill-session session)
            owner-not-authorized)
          ;;
          ;;
          (and cookie-session (not= cookie-session session))
          cookie-session-missmatch
          ;;
          (not= refresh_token current-refresh-token)
          (token-error
           400
           "invalid_request"
           "Provided token doesn't match session refresh token"
           "Your request will be logged and processed")
          ;;
          :else
          {:status 200
           :headers {"Content-Type" "application/json;charset=UTF-8"
                     "Pragma" "no-cache"
                     "Cache-Control" "no-store"}
           :body (json/write-str (generate client session (assoc request :scope scope)))}))
      (token-error
       400
       "invalid_grant"
       "There is no valid session for refresh token that"
       "was provided"))))

(defn validate-client-credentials
  "Validates client credentials for client_credentials grant.
   Returns the client if valid, nil otherwise."
  [{:keys [client_id client_secret]}]
  (when-let [client (get-client client_id)]
    (let [{client-secret :secret
           client-type :type
           {allowed-grants "allowed-grants"} :settings} client
          grants (set allowed-grants)]
      (cond
        ;; Deactivating a client is the kill switch an admin reaches for, so it
        ;; has to actually stop token issuance. `false?` rather than `not`:
        ;; clients predating the flag have :active nil and must keep working.
        (false? (:active client))
        (do
          (log/warnf "[%s] Inactive client attempted client_credentials grant" client_id)
          nil)

        ;; Public clients can't hold a secret, so they can't authenticate as
        ;; themselves — RFC 6749 §4.4 is confidential-only.
        (#{:public "public"} client-type)
        (do
          (log/debugf "[%s] Public clients cannot use client_credentials grant" client_id)
          nil)

        (not (contains? grants "client_credentials"))
        (do
          (log/debugf "[%s] Client credentials grant not allowed for client" client_id)
          nil)

        (not (core/secret-matches? client_secret client-secret))
        (do
          (log/debugf "[%s] Invalid or missing client secret" client_id)
          nil)

        :else
        (do
          (log/debugf "[%s] Client credentials validated successfully" client_id)
          client)))))

;; The service identity is the User whose name equals the client id. Reusing
;; the resource-owner machinery means roles, groups and RLS resolve exactly as
;; they do for humans — nothing downstream has to learn about service callers.
(defn- client-credentials-session
  "Find-or-create the client's persistent :flow \"client_credentials\" session.
   Same idiom as robotics' per-robot session: the issued token is an ordinary
   store-registered access token, so the auth interceptor, websockets,
   introspection and the session sweeper all accept it untouched."
  [client owner]
  (let [now (vura/date)
        existing (some
                  (fn [session]
                    (when (= "client_credentials" (:flow (core/get-session session)))
                      session))
                  (get-in @core/*resource-owners* [(:euuid owner) :sessions]))
        session (or existing (core/gen-session-id))]
    (when-not existing
      ;; *sessions* holds the client euuid; get-client already registered the
      ;; full record in *clients* during validation.
      (core/set-session session {:flow "client_credentials"
                                 :client (:euuid client)
                                 :auth-method :programmatic
                                 :last-active now})
      (core/set-session-resource-owner session owner)
      (core/set-session-audience-scope session nil []))
    (core/set-session-authorized-at session now)
    session))

(defmethod grant-token "client_credentials"
  [{:keys [client_id scope]
    :as request}]
  (log/debugf "[%s] Processing client credentials grant request" client_id)
  (if-let [client (validate-client-credentials request)]
    ;; scope is already a set by now — core/scope->set runs on the token route
    ;; (see neyho.eywa.iam.oauth/token) and splits the raw parameter before any
    ;; grant sees it. nil when the caller sent no scope, hence the (set ...).
    ;; offline_access is dropped because RFC 6749 §4.4.3 says
    ;; client_credentials must not issue a refresh token — the client can just
    ;; re-authenticate with its secret.
    (let [scope (disj (set scope) "offline_access")
          ;; Deliberately NOT core/get-resource-owner: *resource-owners* is a
          ;; cache with no invalidation, so a service user deactivated after it
          ;; was first cached would keep getting tokens. Read the identity
          ;; fresh; set-session-resource-owner below merges it back, so the
          ;; cache is refreshed rather than bypassed. One extra read per grant,
          ;; and a grant happens once per token lifetime, not per request.
          owner (iam/get-user-details client_id)]
      (if-not (:active owner)
        (do
          (log/warnf "[%s] No active service user for client" client_id)
          (token-error
           401
           "invalid_client"
           "No active service user is configured for this client"))
        (try
          (let [session (client-credentials-session client owner)
                tokens (generate client session (assoc request :scope scope))]
            (log/debugf "[%s] Client credentials tokens generated successfully" client_id)
            {:status 200
             :headers {"Content-Type" "application/json;charset=UTF-8"
                       "Pragma" "no-cache"
                       "Cache-Control" "no-store"}
             :body (json/write-str tokens)})
          (catch Exception e
            (log/errorf e "[%s] Error generating tokens for client credentials" client_id)
            (token-error
             500
             "server_error"
             "An error occurred while generating tokens")))))

    (do
      (log/warnf "[%s] Client credentials validation failed" client_id)
      (token-error
       401
       "invalid_client"
       "Client authentication failed"))))

(defn token-endpoint
  [{{:keys [grant_type]
     :as oauth-request} :params
    :as request}]
  ; (def request request)
  ; (def grant_type grant_type)
  ; (def oauth-request oauth-request)
  (log/debugf "Received token endpoint request\n%s" (pprint request))
  (binding [core/*domain* (core/original-uri request)]
    (case grant_type
      ;; Supported grant types
      ("authorization_code" "refresh_token" "urn:ietf:params:oauth:grant-type:device_code" "client_credentials")
      (grant-token oauth-request)
      ;;else
      ;; RFC 6749 §5.2 — the token endpoint answers a back-channel POST, so an
      ;; error is a 400 with a JSON body. It must NOT go through
      ;; core/handle-request-error: that redirects, which is right for the
      ;; authorization endpoint (the error rides back to the browser's
      ;; redirect_uri) but leaves a machine client following a 302 to an HTML
      ;; status page instead of reading an error code.
      (if (str/blank? grant_type)
        (token-error
         400
         "invalid_request"
         "Missing required parameter: grant_type")
        (token-error
         400
         "unsupported_grant_type"
         (str "Unsupported grant_type: " grant_type))))))

(def token-interceptor
  {:name ::token
   :enter
   (fn [{request :request
         :as context}]
     (chain/terminate
      (assoc context :response (token-endpoint request))))})

(let [invalid-client (core/json-error "invalid_client" "Client ID is not valid")
      invalid-token (core/json-error "invalid_token" "Token is not valid")]
  (def revoke-token-interceptor
    {:enter
     (fn [{{{:keys [token_type_hint token]
             :as params} :params} :request
           :as ctx}]
       (let [token-key (when token_type_hint (keyword token_type_hint))
             tokens @*tokens*
             [token-key session] (some
                                  (fn [token-key]
                                    (when-some [session (get-in tokens [token-key token])]
                                      [token-key session]))
                                  [token-key :access_token :refresh_token])]

         (letfn [(error [response]
                   (log/errorf "[%s] Couldn't revoke token. Returning\n%s" session response)
                   (chain/terminate (assoc ctx :response response)))]
           (cond
             (nil? session) (error invalid-token)
             (core/clients-doesnt-match? session params) (error invalid-client)
             :else (do
                     (log/debugf "[%s] Revoking token %s %s" session token-key token)
                     (revoke-token token-key token)
                     (assoc ctx :response
                            {:status 200
                             :headers {"Content-Type" "application/json"
                                       "Cache-Control" "no-store"
                                       "Pragma" "no-cache"}}))))))}))

(defn delete [tokens]
  (swap! *tokens*
         (fn [state]
           (reduce-kv
            (fn [state token-type tokens-to-delete]
              (update state token-type #(apply dissoc % tokens-to-delete)))
            state
            tokens))))

;; SCOPES
(defmethod process-scope "roles"
  [session tokens _]
  (let [{:keys [roles]} (core/get-session-resource-owner session)
        dataset-roles (dataset/search-entity
                       iu/user-role
                       {:euuid {:_in roles}}
                       {:name nil})]
    (assoc-in tokens [:access_token :roles] (map (comp csk/->snake_case_keyword :name) dataset-roles))))

(defmethod process-scope "groups"
  [session tokens _]
  (let [{:keys [groups]} (core/get-session-resource-owner session)
        dataset-groups (dataset/search-entity
                        iu/user-group
                        {:euuid {:_in groups}}
                        {:name nil})]
    (assoc-in tokens [:access_token :groups] (map (comp csk/->snake_case_keyword :name) dataset-groups))))

(defmethod process-scope "sub:uuid"
  [session tokens _]
  (let [{:keys [euuid]} (core/get-session-resource-owner session)]
    (assoc-in tokens [:access_token "sub:uuid"] (str euuid))))

(defmethod process-scope "roles:uuid"
  [session tokens _]
  (let [{:keys [roles]} (core/get-session-resource-owner session)]
    (assoc-in tokens [:access_token :roles] roles)))

(defmethod process-scope "groups:uuid"
  [session tokens _]
  (let [{:keys [groups]} (core/get-session-resource-owner session)]
    (assoc-in tokens [:access_token :groups] groups)))

(defmethod process-scope "permissions"
  [session tokens _]
  (let [{:keys [roles]} (core/get-session-resource-owner session)]
    (assoc-in tokens [:access_token :permissions] (access/roles-scopes roles))))

(defmethod process-scope "super"
  [session tokens _]
  (let [{:keys [roles]} (core/get-session-resource-owner session)]
    (assoc-in tokens [:access_token :super] (access/superuser? roles))))

(comment
  (def tokens nil)
  (access/roles-scopes #{#uuid "8ebc60f1-8df0-48c8-a9b6-747a140df021"})
  (def session "RkJDHRzznXwlkatsVQnLWMmJHRWdyg"))
