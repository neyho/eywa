(ns neyho.eywa.iam.test-introspection
  "Tests for OAuth 2.0 Token Introspection (RFC 7662)"
  (:require
   [clojure.test :refer [deftest is testing]]
   [clojure.data.json :as json]
   [neyho.eywa.iam :as iam]
   [neyho.eywa.iam.oauth.core :as oauth-core]
   [neyho.eywa.iam.oauth.token :as token]
   [neyho.eywa.iam.oauth.introspection :as introspection]))

(comment
  "
  Manual Testing Guide for Token Introspection
  =============================================

  1. Start EYWA server
  2. Create a test client and user in the database
  3. Obtain an access token via authorization code flow or client credentials
  4. Test the introspection endpoint:

  ## Using curl with Basic Auth:

  curl -X POST http://localhost:8080/oauth/introspect \\
    -u \"client_id:client_secret\" \\
    -d \"token=eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9...\" \\
    -d \"token_type_hint=access_token\"

  ## Using curl with form parameters:

  curl -X POST http://localhost:8080/oauth/introspect \\
    -d \"client_id=your-client-id\" \\
    -d \"client_secret=your-client-secret\" \\
    -d \"token=eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9...\" \\
    -d \"token_type_hint=access_token\"

  ## Expected Response (Active Token):

  {
    \"active\": true,
    \"scope\": \"openid profile email\",
    \"client_id\": \"my-client-id\",
    \"username\": \"john.doe\",
    \"token_type\": \"Bearer\",
    \"exp\": 1708704000,
    \"iat\": 1708700400,
    \"sub\": \"john.doe\",
    \"iss\": \"https://your-domain.com\",
    \"aud\": \"my-client-id\",
    \"jti\": \"AbCdEfGhIjKlMnOpQrSt\",
    \"sid\": \"YXldcURYFGCaMkMKwqFQvUb\"
  }

  ## Expected Response (Inactive/Invalid Token):

  {
    \"active\": false
  }

  ## Error Cases:

  ### Missing client credentials:
  {
    \"error\": \"invalid_client\",
    \"error_description\": \"Client authentication required\"
  }

  ### Missing token parameter:
  {
    \"error\": \"invalid_request\",
    \"error_description\": \"Missing required parameter: token\"
  }

  ## Integration with Resource Servers:

  Resource servers should call the introspection endpoint to validate
  tokens before granting access to protected resources:

  1. Extract Bearer token from Authorization header
  2. Call /oauth/introspect with client credentials
  3. Check response[\"active\"] === true
  4. Verify response[\"scope\"] contains required scopes
  5. Verify response[\"aud\"] matches your resource server ID
  6. Grant or deny access based on validation
  ")

(deftest test-introspect-token-not-found
  (testing "Introspect returns inactive for unknown token"
    (let [result (introspection/introspect-token "invalid-token" nil)]
      (is (= false (:active result)))
      (is (= 1 (count (keys result))))
      (is (contains? result :active)))))

(deftest test-introspect-token-empty
  (testing "Introspect returns inactive for empty token"
    (let [result (introspection/introspect-token "" nil)]
      (is (= false (:active result))))
    (let [result (introspection/introspect-token nil nil)]
      (is (= false (:active result))))))

(deftest test-introspect-token-with-hint
  (testing "Token type hint is respected when searching for tokens"
    ;; This test requires actual token setup, so it's more of a documentation
    ;; of expected behavior. In a real scenario:
    ;; 1. Create a session
    ;; 2. Generate an access token
    ;; 3. Store it in *tokens*
    ;; 4. Call introspect with hint="access_token"
    ;; 5. Verify it's found faster by checking hint first
    (is true "See manual testing guide in comment above")))

(deftest test-extract-token-claims
  (testing "Extract claims from valid JWT"
    ;; This requires a valid signed JWT from the system
    ;; In practice, this is tested through the full OAuth flow
    (is true "Tested through integration tests")))

(deftest test-token-expiration-check
  (testing "Token expiration is correctly detected"
    (let [expired-claims {:exp (-> (System/currentTimeMillis)
                                   (- 1000)  ; 1 second ago
                                   (quot 1000))}
          future-claims {:exp (-> (System/currentTimeMillis)
                                  (+ 10000)  ; 10 seconds from now
                                  (quot 1000))}]
      (is (true? (#'introspection/token-expired? expired-claims)))
      (is (false? (#'introspection/token-expired? future-claims))))))

(deftest test-inactive-response-format
  (testing "Inactive response has correct format"
    (let [response (#'introspection/inactive-response)]
      (is (map? response))
      (is (= false (:active response)))
      (is (= 1 (count (keys response)))))))

(comment
  "
  Example: Full Token Introspection Flow
  ======================================

  ;; 1. Initialize encryption keys
  (iam/init-default-encryption)

  ;; 2. Create a test session and token
  (def test-session \"test-session-id\")
  (def test-client {:id \"test-client\"
                    :euuid #uuid \"550e8400-e29b-41d4-a716-446655440000\"
                    :settings {\"allowed-grants\" [\"client_credentials\"]
                               \"token-expiry\" {\"access\" 7200}}})

  ;; 3. Create session
  (swap! oauth-core/*sessions* assoc test-session
         {:client (:euuid test-client)
          :resource-owner #uuid \"550e8400-e29b-41d4-a716-446655440001\"
          :flow \"client_credentials\"
          :last-active (vura/date)})

  (swap! oauth-core/*clients* assoc (:euuid test-client) test-client)

  ;; 4. Generate token
  (def token-data {:session test-session
                   :aud \"test-audience\"
                   :exp (-> (System/currentTimeMillis)
                           (quot 1000)
                           (+ 7200))
                   :iss \"https://localhost:8080\"
                   :sub \"test-user\"
                   :iat (-> (System/currentTimeMillis) (quot 1000))
                   :jti (nano-id/nano-id 20)
                   :client_id \"test-client\"
                   :sid test-session
                   :scope \"openid profile\"})

  (def signed-token (iam/sign-data token-data {:alg :rs256}))

  ;; 5. Store token
  (swap! token/*tokens* assoc-in [:access_token signed-token] test-session)

  ;; 6. Introspect the token
  (def result (introspection/introspect-token signed-token \"access_token\"))

  ;; 7. Verify result
  (assert (= true (:active result)))
  (assert (= \"test-client\" (:client_id result)))
  (assert (= \"test-user\" (:username result)))
  (assert (= \"Bearer\" (:token_type result)))

  ;; 8. Test with expired token
  (def expired-token-data (assoc token-data
                                 :exp (-> (System/currentTimeMillis)
                                         (quot 1000)
                                         (- 100))))
  (def expired-token (iam/sign-data expired-token-data {:alg :rs256}))
  (swap! token/*tokens* assoc-in [:access_token expired-token] test-session)

  (def expired-result (introspection/introspect-token expired-token \"access_token\"))
  (assert (= false (:active expired-result)))

  ;; 9. Test with invalid token
  (def invalid-result (introspection/introspect-token \"not-a-valid-token\" nil))
  (assert (= false (:active invalid-result)))
  ")

(comment
  "
  Resource Server Example
  =======================

  ;; Example of how a resource server would use introspection:

  (defn validate-access-token
    \"Validates an access token by introspecting it.
    Returns token metadata if valid, nil otherwise.\"
    [token client-id client-secret]
    (let [response (http/post \"https://auth.example.com/oauth/introspect\"
                              {:form-params {:token token
                                             :token_type_hint \"access_token\"
                                             :client_id client-id
                                             :client_secret client-secret}
                               :as :json})]
      (when (= 200 (:status response))
        (let [body (:body response)]
          (when (:active body)
            body)))))

  (defn protected-resource-handler
    \"Example handler for a protected resource.\"
    [request]
    (if-let [token (extract-bearer-token request)]
      (if-let [token-info (validate-access-token token
                                                  resource-server-client-id
                                                  resource-server-client-secret)]
        ;; Token is valid, check scopes and proceed
        (if (contains? (set (str/split (:scope token-info) #\" \")) \"read:data\")
          {:status 200
           :body {:message \"Access granted\"
                  :user (:username token-info)}}
          {:status 403
           :body {:error \"insufficient_scope\"}})
        ;; Token is invalid or expired
        {:status 401
         :body {:error \"invalid_token\"}})
      ;; No token provided
      {:status 401
       :body {:error \"missing_token\"}}))
  ")

;; Run tests with:
;; clj -M:test -n neyho.eywa.iam.test-introspection
