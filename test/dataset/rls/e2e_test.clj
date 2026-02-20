(ns dataset.rls.e2e-test
  "End-to-end tests for RLS (Row-Level Security) guards.
   Tests multi-hop relation paths through actual GraphQL requests."
  (:require
    [clj-http.lite.client :as http]
    [clojure.data.json :as json]
    [clojure.test :refer [deftest testing is]]
    [dataset.test-helpers :refer [make-entity make-relation add-test-attribute]]
    [neyho.eywa.data :refer [*EYWA*]]
    [neyho.eywa.dataset :as dataset]
    [neyho.eywa.dataset.core :as core]
    [neyho.eywa.iam :as iam]
    [neyho.eywa.iam.oauth.core :as oauth-core]
    [neyho.eywa.iam.oauth.token :as oauth-token]
    [neyho.eywa.iam.uuids :as iu]
    [vura.core :as vura]))


;;; ============================================================================
;;; Configuration
;;; ============================================================================

(def ^:dynamic *base-url* "http://localhost:8080")


;;; ============================================================================
;;; HTTP/GraphQL Utilities
;;; ============================================================================

(defn create-test-session!
  "Creates a test session and registers it in the OAuth session store.
   Returns the session ID."
  [user-name]
  (let [session-id (str "test-" (java.util.UUID/randomUUID))]
    ;; Set up basic session in *sessions*
    (swap! oauth-core/*sessions* assoc session-id
           {:resource-owner nil  ; Will be set when we register the user
            :client nil
            :last-active (vura/date)})
    session-id))


(defn create-test-token!
  "Creates a JWT access token for testing AND registers it in the token store.
   This is required because the server validates tokens against the session store.

   user-data should contain:
   - :name (sub claim, required)
   - :euuid (user UUID, required for RLS context)

   Returns the signed JWT token string."
  [{:keys [name euuid]}]
  (let [session-id (create-test-session! name)
        now (quot (System/currentTimeMillis) 1000)
        ;; Create access token data matching what the server expects
        token-data {:sub name
                    :iat now
                    :exp (+ now 3600) ; 1 hour
                    :session session-id
                    :iss *base-url*
                    "sub:uuid" (str euuid)}
        ;; Sign the token
        signed-token (iam/sign-data token-data {:alg :rs256})
        ;; Get full user details from database (including RLS context)
        user-details (iam/get-user-details name)]

    ;; Explicitly cache the user details for get-resource-owner lookups
    (when user-details
      (swap! oauth-core/*resource-owners*
             (fn [cache]
               (-> cache
                   (assoc (:euuid user-details) user-details)
                   (assoc-in [::oauth-core/name-mapping name] (:euuid user-details))))))

    ;; Register the token in the token store
    (swap! oauth-token/*tokens* assoc-in [:access_token signed-token] session-id)

    ;; Update session with resource owner
    (oauth-core/set-session-resource-owner session-id user-details)

    signed-token))


(defn cleanup-test-tokens!
  "Removes all test tokens from the session store."
  []
  (let [test-sessions (->> @oauth-core/*sessions*
                           (filter (fn [[k _]] (and (string? k)
                                                    (.startsWith k "test-"))))
                           (map first))]
    ;; Remove test sessions
    (swap! oauth-core/*sessions*
           (fn [sessions]
             (apply dissoc sessions test-sessions)))
    ;; Remove test tokens
    (swap! oauth-token/*tokens*
           (fn [tokens]
             (update tokens :access_token
                     (fn [access-tokens]
                       (into {}
                             (remove (fn [[_ session]]
                                       (some #(= session %) test-sessions))
                                     access-tokens))))))))


(defn graphql!
  "Execute a GraphQL query/mutation against the server.
   Returns the parsed response body.
   Throws on HTTP errors."
  ([query] (graphql! nil query nil))
  ([token query] (graphql! token query nil))
  ([token query variables]
   (let [headers (cond-> {"Content-Type" "application/json"
                          "Accept" "application/json"}
                   token (assoc "Authorization" (str "Bearer " token)))
         body (cond-> {:query query}
                variables (assoc :variables variables))
         response (http/post (str *base-url* "/graphql")
                             {:headers headers
                              :body (json/write-str body)
                              :throw-exceptions false})]
     (-> response
         :body
         (json/read-str :key-fn keyword)))))


(defn graphql-data!
  "Like graphql! but returns just the :data portion.
   Throws if there are errors in the response."
  ([token query] (graphql-data! token query nil))
  ([token query variables]
   (let [{:keys [data errors]} (graphql! token query variables)]
     (when (seq errors)
       (throw (ex-info "GraphQL errors" {:errors errors})))
     data)))


;;; ============================================================================
;;; Test User Management
;;; ============================================================================

(defn ensure-test-user!
  "Creates or updates a test user. Returns the user data."
  [{:keys [name euuid password active]
    :or {password "test123"
         active true}}]
  (let [mutation "mutation($data: UserInput!) {
                    syncUser(data: $data) {
                      euuid name active
                    }
                  }"
        variables {:data {:euuid euuid
                          :name name
                          :password password
                          :active active}}]
    ;; Use system token (or no token if server allows)
    ;; For now, we'll need a superuser token
    (:syncUser (graphql-data! nil mutation variables))))


(defn ensure-test-group!
  "Creates or updates a test user group. Returns the group data."
  [{:keys [name euuid active]
    :or {active true}}]
  (let [mutation "mutation($data: UserGroupInput!) {
                    syncUserGroup(data: $data) {
                      euuid name active
                    }
                  }"
        variables {:data {:euuid euuid
                          :name name
                          :active active}}]
    (:syncUserGroup (graphql-data! nil mutation variables))))


(defn add-user-to-group!
  "Adds a user to a group."
  [user-euuid group-euuid]
  (let [mutation "mutation($data: UserInput!) {
                    syncUser(data: $data) {
                      euuid
                      memberOf { euuid }
                    }
                  }"
        variables {:data {:euuid user-euuid
                          :memberOf [{:euuid group-euuid}]}}]
    (:syncUser (graphql-data! nil mutation variables))))


;;; ============================================================================
;;; Test Model Definition - Multi-hop RLS Test
;;; ============================================================================

;; Test model structure for 2-hop relation path:
;;
;;   RLSDocument ──belongs_to──> RLSProject ──leader──> User
;;        │                            │
;;        └── Guard: Read allowed if user is project leader (2-hop path)
;;

(def rls-project-euuid #uuid "e2e00001-0000-0000-0000-000000000001")
(def rls-document-euuid #uuid "e2e00001-0000-0000-0000-000000000002")
(def project-leader-attr-euuid #uuid "e2e00001-0000-0000-0000-000000000003")
(def project-name-attr-euuid #uuid "e2e00001-0000-0000-0000-000000000006")
(def document-title-attr-euuid #uuid "e2e00001-0000-0000-0000-000000000004")
(def document-project-rel-euuid #uuid "e2e00001-0000-0000-0000-000000000005")


(defn make-rls-test-model
  "Creates a test model with multi-hop relation for RLS testing.

   Entities:
   - RLSProject: has 'leader' attribute (ref to User)
   - RLSDocument: guarded entity with 2-hop RLS

   Relations:
   - RLSDocument belongs_to RLSProject (many-to-one)

   Guards on RLSDocument:
   - Read: allowed if user is the leader of the document's project (2-hop)"
  []
  (let [;; RLSProject entity with name and leader attributes
        project-entity (-> (make-entity {:euuid rls-project-euuid
                                         :name "RLSProject"})
                           (add-test-attribute
                             {:euuid project-name-attr-euuid
                              :name "name"
                              :type "string"
                              :constraint "optional"})
                           (add-test-attribute
                             {:euuid project-leader-attr-euuid
                              :name "leader"
                              :type "user"
                              :constraint "optional"}))

        ;; RLSDocument entity with title and RLS guards
        document-entity (-> (core/map->ERDEntity
                              {:euuid rls-document-euuid
                               :name "RLSDocument"
                               :type "STRONG"
                               :width 150.0
                               :height 100.0
                               :position {:x 300
                                          :y 100}
                               :attributes []
                               :active true
                               ;; RLS Configuration - hybrid guard (relation + ref)
                               :configuration
                               {:constraints {:unique []}
                                :rls
                                {:enabled true
                                 :guards
                                 [{:id (java.util.UUID/randomUUID)
                                   :operation #{:read :write :delete}
                                   :conditions
                                   [{:type :hybrid
                                     ;; Path: Document -> (relation) -> Project -> leader (ref)
                                     ;; Hybrid: traverse relation, then check ref attribute on final entity
                                     :steps [{:relation-euuid document-project-rel-euuid}]
                                     :attribute project-leader-attr-euuid
                                     :target :user}]}]}}})
                            (core/add-attribute
                              {:euuid document-title-attr-euuid
                               :name "title"
                               :type "string"
                               :constraint "optional"
                               :active true}))

        ;; Build ERDModel
        model (-> (core/map->ERDModel {})
                  (core/add-entity project-entity)
                  (core/add-entity document-entity)
                  (core/add-relation
                    (make-relation
                      {:euuid document-project-rel-euuid
                       :from rls-document-euuid
                       :to rls-project-euuid
                       :from-label "documents"
                       :to-label "project"
                       :cardinality "m2o"})))]

    {:euuid #uuid "e2e00000-0000-0000-0000-000000000001"
     :name "RLS E2E Test Dataset"
     :version "1.0.0"
     :dataset {:euuid #uuid "e2e00000-0000-0000-0000-000000000000"
               :name "RLSTest"}
     :model model}))


;;; ============================================================================
;;; Test Data Setup
;;; ============================================================================

(def test-user-alice-euuid #uuid "e2e00002-0000-0000-0000-000000000001")
(def test-user-bob-euuid #uuid "e2e00002-0000-0000-0000-000000000002")
(def test-project-1-euuid #uuid "e2e00003-0000-0000-0000-000000000001")
(def test-project-2-euuid #uuid "e2e00003-0000-0000-0000-000000000002")
(def test-doc-1-euuid #uuid "e2e00004-0000-0000-0000-000000000001")
(def test-doc-2-euuid #uuid "e2e00004-0000-0000-0000-000000000002")
(def test-doc-3-euuid #uuid "e2e00004-0000-0000-0000-000000000003")


(defn setup-test-data!
  "Sets up test data for multi-hop RLS testing.

   Creates:
   - Alice: leader of Project1
   - Bob: leader of Project2
   - Project1: leader=Alice
   - Project2: leader=Bob
   - Doc1, Doc2: belong to Project1 (Alice should see)
   - Doc3: belongs to Project2 (Bob should see)

   Expected behavior:
   - Alice can read Doc1, Doc2 (through Project1 -> leader=Alice)
   - Alice cannot read Doc3 (Project2 -> leader=Bob)
   - Bob can read Doc3 (through Project2 -> leader=Bob)
   - Bob cannot read Doc1, Doc2"
  [superuser-token]
  ;; Create test users
  (let [alice (graphql-data! superuser-token
                             "mutation($data: UserInput!) {
                   syncUser(data: $data) { euuid name }
                 }"
                             {:data {:euuid test-user-alice-euuid
                                     :name "alice_rls_test"
                                     :password "test123"
                                     :active true}})

        bob (graphql-data! superuser-token
                           "mutation($data: UserInput!) {
                 syncUser(data: $data) { euuid name }
               }"
                           {:data {:euuid test-user-bob-euuid
                                   :name "bob_rls_test"
                                   :password "test123"
                                   :active true}})

        ;; Create projects with leaders (type name uses camelCase: RlsProject)
        project1 (graphql-data! superuser-token
                                "mutation($data: RlsProjectInput!) {
                      syncRlsProject(data: $data) { euuid leader { euuid } }
                    }"
                                {:data {:euuid test-project-1-euuid
                                        :name "Project1"
                                        :leader {:euuid test-user-alice-euuid}}})

        project2 (graphql-data! superuser-token
                                "mutation($data: RlsProjectInput!) {
                      syncRlsProject(data: $data) { euuid leader { euuid } }
                    }"
                                {:data {:euuid test-project-2-euuid
                                        :name "Project2"
                                        :leader {:euuid test-user-bob-euuid}}})

        ;; Create documents (type name uses camelCase: RlsDocument)
        doc1 (graphql-data! superuser-token
                            "mutation($data: RlsDocumentInput!) {
                  syncRlsDocument(data: $data) { euuid title }
                }"
                            {:data {:euuid test-doc-1-euuid
                                    :title "Doc1 - Alice's Project"
                                    :project {:euuid test-project-1-euuid}}})

        doc2 (graphql-data! superuser-token
                            "mutation($data: RlsDocumentInput!) {
                  syncRlsDocument(data: $data) { euuid title }
                }"
                            {:data {:euuid test-doc-2-euuid
                                    :title "Doc2 - Alice's Project"
                                    :project {:euuid test-project-1-euuid}}})

        doc3 (graphql-data! superuser-token
                            "mutation($data: RlsDocumentInput!) {
                  syncRlsDocument(data: $data) { euuid title }
                }"
                            {:data {:euuid test-doc-3-euuid
                                    :title "Doc3 - Bob's Project"
                                    :project {:euuid test-project-2-euuid}}})]

    {:alice (:syncUser alice)
     :bob (:syncUser bob)
     :project1 (:syncRlsProject project1)
     :project2 (:syncRlsProject project2)
     :doc1 (:syncRlsDocument doc1)
     :doc2 (:syncRlsDocument doc2)
     :doc3 (:syncRlsDocument doc3)}))


;;; ============================================================================
;;; Tests
;;; ============================================================================

(deftest test-multihop-rls-read
  (testing "Multi-hop RLS: User sees only documents where they are project leader"
    ;; This test assumes:
    ;; 1. Model is deployed with RLSProject and RLSDocument entities
    ;; 2. Test data has been set up via setup-test-data!
    ;; 3. Server is running on localhost:8080

    (let [;; Create tokens for test users (registers in session store)
          alice-token (create-test-token! {:name "alice_rls_test"
                                           :euuid test-user-alice-euuid})
          bob-token (create-test-token! {:name "bob_rls_test"
                                         :euuid test-user-bob-euuid})

          ;; Query all documents as Alice (camelCase: searchRlsDocument)
          alice-docs (graphql-data! alice-token
                                    "{ searchRlsDocument { euuid title } }")

          ;; Query all documents as Bob
          bob-docs (graphql-data! bob-token
                                  "{ searchRlsDocument { euuid title } }")]

      ;; Alice should see Doc1 and Doc2 (her project's docs)
      (is (= 2 (count (:searchRlsDocument alice-docs)))
          "Alice should see exactly 2 documents")
      (is (every? #(contains? #{test-doc-1-euuid test-doc-2-euuid}
                              (java.util.UUID/fromString (:euuid %)))
                  (:searchRlsDocument alice-docs))
          "Alice should see Doc1 and Doc2")

      ;; Bob should see only Doc3 (his project's doc)
      (is (= 1 (count (:searchRlsDocument bob-docs)))
          "Bob should see exactly 1 document")
      (is (= (str test-doc-3-euuid)
             (:euuid (first (:searchRlsDocument bob-docs))))
          "Bob should see Doc3"))))


(deftest test-multihop-rls-write-denied
  (testing "Multi-hop RLS: User cannot write documents where they are not project leader"
    (let [alice-token (create-test-token! {:name "alice_rls_test"
                                           :euuid test-user-alice-euuid})

          ;; Alice tries to update Bob's document (Doc3) - camelCase: RlsDocumentInput
          result (graphql! alice-token
                           "mutation($data: RlsDocumentInput!) {
                      syncRlsDocument(data: $data) { euuid title }
                    }"
                           {:data {:euuid test-doc-3-euuid
                                   :title "Alice tries to change Bob's doc"}})]

      ;; Should get RLS error
      (is (seq (:errors result))
          "Alice should not be able to update Bob's document")
      (is (some #(re-find #"(?i)rls|access|denied" (str %)) (:errors result))
          "Error should mention RLS or access denied"))))


(deftest test-multihop-rls-write-allowed
  (testing "Multi-hop RLS: User can write documents where they are project leader"
    (let [alice-token (create-test-token! {:name "alice_rls_test"
                                           :euuid test-user-alice-euuid})

          ;; Alice updates her own document (Doc1) - camelCase: RlsDocumentInput
          result (graphql-data! alice-token
                                "mutation($data: RlsDocumentInput!) {
                      syncRlsDocument(data: $data) { euuid title }
                    }"
                                {:data {:euuid test-doc-1-euuid
                                        :title "Alice updates her own doc"}})]

      ;; Should succeed
      (is (= "Alice updates her own doc"
             (:title (:syncRlsDocument result)))
          "Alice should be able to update her own document"))))


;;; ============================================================================
;;; Model Deployment
;;; ============================================================================

(defn deploy-test-model!
  "Deploys the RLS test model to the database.
   Must be called before running tests.
   Requires the dataset namespace and a running database connection."
  []
  (let [test-version (make-rls-test-model)]
    ;; Deploy the model
    (dataset/deploy! test-version)
    (println "RLS test model deployed successfully!")
    test-version))


(defn cleanup-test-model!
  "Destroys the RLS test model and all its deployed versions.
   Removes all database tables, schema, and metadata."
  []
  (dataset/destroy-dataset nil (:dataset (make-rls-test-model)) nil)
  (println "RLS test model destroyed successfully!"))


(comment
  (dataset/deployed-model)
  (cleanup-test-model!))


;;; ============================================================================
;;; Test Runner / REPL helpers
;;; ============================================================================

(defn run-tests
  "Run all RLS E2E tests.
   Requires server running on localhost:8080 with test model deployed."
  []
  (clojure.test/run-tests 'dataset.rls.e2e-test))


(defn run-full-e2e-test!
  "Complete E2E test workflow:
   1. Deploy test model
   2. Create test users and data
   3. Run tests
   4. Cleanup tokens and model

   Requires:
   - Server running on localhost:8080
   - Database connection available"
  []
  (println "\n=== RLS E2E Test Suite ===\n")

  ;; Step 1: Deploy model
  (println "1. Deploying test model...")
  (deploy-test-model!)

  ;; Step 2: Create superuser token and setup data
  (println "\n2. Setting up test data...")
  (let [su-token (create-test-token! {:name "EYWA"
                                      :euuid (:euuid *EYWA*)})]
    (setup-test-data! su-token))
  (println "Test data created: Alice (Project1), Bob (Project2), 3 documents")

  ;; Step 3: Run tests
  (println "\n3. Running tests...\n")
  (let [results (run-tests)]
    ;; Step 4: Cleanup
    (println "\n4. Cleaning up test tokens...")
    (cleanup-test-tokens!)
    (println "5. Destroying test model...")
    (cleanup-test-model!)
    results))


(comment
  ;; ============================================================================
  ;; REPL Workflow for RLS E2E Testing
  ;; ============================================================================

  ;; Prerequisites:
  ;; - Backend running: (require '[neyho.eywa.core :as core]) (core/start)
  ;; - Database initialized

  ;; Load this namespace
  (require '[dataset.rls.e2e-test :as rls-test] :reload)

  ;; Option 1: Run full automated E2E test
  (rls-test/run-full-e2e-test!)

  ;; Option 2: Step-by-step manual testing

  ;; Step 1: Deploy the test model
  (rls-test/deploy-test-model!)

  ;; Step 2: Create superuser token and setup test data
  ;; Note: *EYWA* is from neyho.eywa.data namespace
  (require '[neyho.eywa.data :refer [*EYWA*]])
  (def su-token (rls-test/create-test-token! {:name "EYWA"
                                              :euuid (:euuid *EYWA*)}))
  (rls-test/setup-test-data! su-token)

  ;; Step 3: Test manually with different users
  (def alice-token (rls-test/create-test-token! {:name "alice_rls_test"
                                                 :euuid rls-test/test-user-alice-euuid}))
  (def bob-token (rls-test/create-test-token! {:name "bob_rls_test"
                                               :euuid rls-test/test-user-bob-euuid}))

  ;; Query as Alice - should see 2 docs (Doc1, Doc2)
  (rls-test/graphql! alice-token "{ searchRLSDocument { euuid title } }")

  ;; Query as Bob - should see 1 doc (Doc3)
  (rls-test/graphql! bob-token "{ searchRLSDocument { euuid title } }")

  ;; Alice tries to update Bob's doc - should FAIL
  (rls-test/graphql! alice-token
                     "mutation { syncRLSDocument(data: {euuid: \"e2e00004-0000-0000-0000-000000000003\", title: \"Hacked!\"}) { euuid } }")

  ;; Alice updates her own doc - should SUCCEED
  (rls-test/graphql! alice-token
                     "mutation { syncRLSDocument(data: {euuid: \"e2e00004-0000-0000-0000-000000000001\", title: \"Updated by Alice\"}) { euuid title } }")

  ;; Step 4: Cleanup
  (rls-test/cleanup-test-tokens!)
  (rls-test/cleanup-test-model!)

  ;; Run automated tests
  (rls-test/run-tests)

  ;;
  )
