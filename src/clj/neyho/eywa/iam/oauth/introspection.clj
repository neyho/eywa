(ns neyho.eywa.iam.oauth.introspection
  "RFC 7662 Token Introspection endpoint.

   Token introspection allows resource servers to query the authorization
   server about the state of an access token or refresh token.

   The endpoint returns metadata about the token, including:
   - Whether the token is active (valid and not expired)
   - Token scope
   - Client ID that requested the token
   - Username of the resource owner
   - Token expiration and issuance times

   RFC 7662: https://tools.ietf.org/html/rfc7662"
  (:require
   [buddy.hashers :as hashers]
   [clojure.data.json :as json]
   [clojure.tools.logging :as log]
   [io.pedestal.interceptor.chain :as chain]
   [neyho.eywa.iam :as iam]
   [neyho.eywa.iam.oauth.core :as core]
   [neyho.eywa.iam.oauth.token :as token]))

;; =============================================================================
;; Client Authentication
;; =============================================================================

(defn- validate-client
  "Validates client credentials for introspection request.
   Returns the client if valid, nil otherwise."
  [{:keys [client_id client_secret]}]
  (when-let [client (core/get-client client_id)]
    (let [{stored-secret :secret} client]
      (cond
        ;; No secret configured - public client (should still authenticate somehow)
        (nil? stored-secret)
        (do
          (log/warnf "[Introspect] Public client %s attempting introspection - not recommended" client_id)
          client)

        ;; No secret provided but required
        (and (some? stored-secret) (or (nil? client_secret) (empty? client_secret)))
        (do
          (log/debugf "[Introspect] Client %s requires secret but none provided" client_id)
          nil)

        ;; Validate hashed secret
        (and (some? stored-secret)
             (not (hashers/check client_secret stored-secret)))
        (do
          (log/debugf "[Introspect] Invalid client secret for %s" client_id)
          nil)

        ;; Valid
        :else
        (do
          (log/debugf "[Introspect] Client %s authenticated successfully" client_id)
          client)))))

;; =============================================================================
;; Introspection Logic
;; =============================================================================

(defn- inactive-response
  "Returns the standard inactive token response per RFC 7662 Section 2.2."
  []
  {:active false})

(defn- extract-token-claims
  "Extract claims from a JWT token. Returns nil if invalid."
  [token-string]
  (try
    (iam/unsign-data token-string)
    (catch Exception _
      nil)))

(defn- token-expired?
  "Check if token is expired based on exp claim."
  [claims]
  (when-let [exp (:exp claims)]
    (< (* 1000 exp) (System/currentTimeMillis))))

(defn introspect-token
  "Introspect an access or refresh token per RFC 7662.

   Args:
     token - The token string to introspect
     token-type-hint - Optional hint: \"access_token\" or \"refresh_token\"

   Returns:
     Map with :active and optional metadata."
  [token token-type-hint]
  (log/debugf "[Introspect] Introspecting token (hint: %s)" token-type-hint)

  (cond
    ;; No token provided
    (or (nil? token) (empty? token))
    (do
      (log/debugf "[Introspect] Empty token")
      (inactive-response))

    ;; Try to find token in our token store
    :else
    (let [token-key (when token-type-hint (keyword token-type-hint))
          tokens @token/*tokens*
          ;; Try hint first, then both token types
          [found-key session] (some
                               (fn [tk]
                                 (when-some [s (get-in tokens [tk token])]
                                   [tk s]))
                               (filter some? [token-key :access_token :refresh_token]))]

      (cond
        ;; Token not found in store
        (nil? session)
        (do
          (log/debugf "[Introspect] Token not found in store")
          (inactive-response))

        ;; Token found - get claims and check expiration
        :else
        (let [claims (extract-token-claims token)
              expired? (token-expired? claims)]

          (cond
            ;; No claims (invalid token format)
            (nil? claims)
            (do
              (log/debugf "[Introspect] Could not extract claims from token")
              (inactive-response))

            ;; Token expired
            expired?
            (do
              (log/debugf "[Introspect] Token is expired")
              (inactive-response))

            ;; Valid active token - return metadata
            :else
            (let [{:keys [scope aud iss sub exp iat jti client_id sid]} claims
                  {:keys [name]} (core/get-session-resource-owner session)]
              (log/debugf "[Introspect] Token active for user: %s" (or name sub))
              {:active true
               :scope (or scope "")
               :client_id (or client_id aud)
               :username (or name sub)
               :token_type "Bearer"
               :exp exp
               :iat iat
               :sub (or sub name)
               :iss (or iss (core/domain+))
               :aud (or aud client_id)
               :jti jti
               :sid sid})))))))

;; =============================================================================
;; Pedestal Interceptor
;; =============================================================================

(def introspection-interceptor
  {:name ::introspect
   :enter
   (fn [{{{:keys [token token_type_hint client_id client_secret] :as params} :params} :request
         :as ctx}]
     (log/debugf "[Introspect] Request from client: %s" client_id)

     (let [response
           (cond
             ;; RFC 7662 Section 2.1: Client authentication REQUIRED
             (or (nil? client_id) (empty? client_id))
             {:status 401
              :headers {"Content-Type" "application/json"
                        "WWW-Authenticate" "Basic realm=\"OAuth\""
                        "Cache-Control" "no-store"
                        "Pragma" "no-cache"}
              :body (json/write-str
                     {:error "invalid_client"
                      :error_description "Client authentication required"})}

             ;; Validate client credentials (including secret!)
             (nil? (validate-client params))
             {:status 401
              :headers {"Content-Type" "application/json"
                        "WWW-Authenticate" "Basic realm=\"OAuth\""
                        "Cache-Control" "no-store"
                        "Pragma" "no-cache"}
              :body (json/write-str
                     {:error "invalid_client"
                      :error_description "Client authentication failed"})}

             ;; Missing token parameter
             (or (nil? token) (empty? token))
             {:status 400
              :headers {"Content-Type" "application/json"
                        "Cache-Control" "no-store"
                        "Pragma" "no-cache"}
              :body (json/write-str
                     {:error "invalid_request"
                      :error_description "Missing required parameter: token"})}

             ;; Valid request - introspect
             :else
             (let [result (introspect-token token token_type_hint)]
               {:status 200
                :headers {"Content-Type" "application/json"
                          "Cache-Control" "no-store"
                          "Pragma" "no-cache"}
                :body (json/write-str result)}))]

       (chain/terminate
        (assoc ctx :response response))))})
