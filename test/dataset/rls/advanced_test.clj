(ns dataset.rls.advanced-test
  "Advanced RLS tests covering edge cases and complex scenarios.

   Tests:
   - AND logic (multiple conditions in single guard)
   - NULL handling in guard columns
   - Aggregations with RLS
   - Deep nested GraphQL queries
   - Complex guard combinations"
  (:require
    [clojure.test :refer [deftest testing is]]
    [dataset.rls.e2e-test :as e2e]
    [neyho.eywa.data :refer [*EYWA*]]
    [neyho.eywa.dataset :as dataset]
    [neyho.eywa.dataset.core :as core]
    [dataset.test-helpers :refer [make-entity make-relation add-test-attribute]]))


;;; ============================================================================
;;; Test Model - Advanced RLS Scenarios
;;; ============================================================================

;; Entity UUIDs
(def advanced-doc-euuid #uuid "a0000001-0000-0000-0000-000000000001")
(def advanced-project-euuid #uuid "a0000001-0000-0000-0000-000000000002")
(def advanced-org-euuid #uuid "a0000001-0000-0000-0000-000000000003")

;; Attribute UUIDs
(def doc-title-euuid #uuid "a0000002-0000-0000-0000-000000000001")
(def doc-owner-euuid #uuid "a0000002-0000-0000-0000-000000000002")
(def doc-dept-euuid #uuid "a0000002-0000-0000-0000-000000000003")
(def doc-status-euuid #uuid "a0000002-0000-0000-0000-000000000004")
(def project-name-euuid #uuid "a0000002-0000-0000-0000-000000000005")
(def project-manager-euuid #uuid "a0000002-0000-0000-0000-000000000006")
(def org-name-euuid #uuid "a0000002-0000-0000-0000-000000000007")

;; Relation UUIDs
(def doc-project-rel-euuid #uuid "a0000003-0000-0000-0000-000000000001")
(def project-org-rel-euuid #uuid "a0000003-0000-0000-0000-000000000002")

;; Test data UUIDs
(def test-user-emma-euuid #uuid "a0000004-0000-0000-0000-000000000001")
(def test-user-frank-euuid #uuid "a0000004-0000-0000-0000-000000000002")
(def test-group-engineering-euuid #uuid "a0000004-0000-0000-0000-000000000003")
(def test-group-finance-euuid #uuid "a0000004-0000-0000-0000-000000000004")
(def test-doc-1-euuid #uuid "a0000005-0000-0000-0000-000000000001")
(def test-doc-2-euuid #uuid "a0000005-0000-0000-0000-000000000002")
(def test-doc-3-euuid #uuid "a0000005-0000-0000-0000-000000000003")
(def test-doc-null-euuid #uuid "a0000005-0000-0000-0000-000000000004")
(def test-project-1-euuid #uuid "a0000006-0000-0000-0000-000000000001")


(defn make-advanced-test-model
  "Creates a model for testing advanced RLS scenarios.

   Entities:
   - AdvancedOrg: top-level organization
   - AdvancedProject: belongs to org, has manager
   - AdvancedDocument: has multiple guards including AND logic

   AdvancedDocument guards:
   1. AND guard: owner=user AND dept=user's group (both must match)
   2. NULL-tolerant: owner can be NULL
   3. Nested access: through project->manager"
  []
  (let [;; Organization entity
        org-entity (-> (make-entity {:euuid advanced-org-euuid
                                     :name "AdvancedOrg"})
                       (add-test-attribute
                         {:euuid org-name-euuid
                          :name "name"
                          :type "string"
                          :constraint "optional"}))

        ;; Project entity
        project-entity (-> (make-entity {:euuid advanced-project-euuid
                                         :name "AdvancedProject"})
                           (add-test-attribute
                             {:euuid project-name-euuid
                              :name "name"
                              :type "string"
                              :constraint "optional"})
                           (add-test-attribute
                             {:euuid project-manager-euuid
                              :name "manager"
                              :type "user"
                              :constraint "optional"}))

        ;; Document entity with complex guards
        doc-entity (-> (core/map->ERDEntity
                         {:euuid advanced-doc-euuid
                          :name "AdvancedDocument"
                          :type "STRONG"
                          :width 200.0
                          :height 150.0
                          :position {:x 100 :y 100}
                          :attributes []
                          :active true
                          :configuration
                          {:constraints {:unique []}
                           :rls
                           {:enabled true
                            :guards
                            [;; Guard 1: AND logic - BOTH owner AND dept must match
                             {:id (java.util.UUID/randomUUID)
                              :operation #{:read :write}
                              :conditions
                              [{:type :ref
                                :attribute doc-owner-euuid
                                :target :user}
                               {:type :ref
                                :attribute doc-dept-euuid
                                :target :group}]}

                             ;; Guard 2: Access through project manager (hybrid path)
                             {:id (java.util.UUID/randomUUID)
                              :operation #{:read}
                              :conditions
                              [{:type :hybrid
                                :steps [{:relation-euuid doc-project-rel-euuid}]
                                :attribute project-manager-euuid
                                :target :user}]}]}}})
                       (core/add-attribute
                         {:euuid doc-title-euuid
                          :name "title"
                          :type "string"
                          :constraint "optional"
                          :active true})
                       (core/add-attribute
                         {:euuid doc-owner-euuid
                          :name "owner"
                          :type "user"
                          :constraint "optional"
                          :active true})
                       (core/add-attribute
                         {:euuid doc-dept-euuid
                          :name "department"
                          :type "group"
                          :constraint "optional"
                          :active true})
                       (core/add-attribute
                         {:euuid doc-status-euuid
                          :name "status"
                          :type "string"
                          :constraint "optional"
                          :active true}))

        ;; Build model
        model (-> (core/map->ERDModel {})
                  (core/add-entity org-entity)
                  (core/add-entity project-entity)
                  (core/add-entity doc-entity)
                  (core/add-relation
                    (make-relation
                      {:euuid doc-project-rel-euuid
                       :from advanced-doc-euuid
                       :to advanced-project-euuid
                       :from-label "documents"
                       :to-label "project"
                       :cardinality "m2o"}))
                  (core/add-relation
                    (make-relation
                      {:euuid project-org-rel-euuid
                       :from advanced-project-euuid
                       :to advanced-org-euuid
                       :from-label "projects"
                       :to-label "organization"
                       :cardinality "m2o"})))]

    {:euuid #uuid "a0000000-0000-0000-0000-000000000001"
     :name "RLS Advanced Test Dataset"
     :version "1.0.0"
     :dataset {:euuid #uuid "a0000000-0000-0000-0000-000000000000"
               :name "RLSAdvanced"}
     :model model}))


;;; ============================================================================
;;; Test Data Setup
;;; ============================================================================

(defn setup-advanced-test-data!
  "Sets up test data for advanced RLS scenarios.

   Users:
   - Emma: member of Engineering group
   - Frank: member of Finance group

   Documents:
   - Doc1: owner=Emma, department=Engineering (Emma can access via AND guard)
   - Doc2: owner=Emma, department=Finance (Emma CANNOT access - dept mismatch)
   - Doc3: owner=Frank, department=NULL (tests NULL handling)
   - DocNull: owner=NULL, department=NULL (tests all-NULL scenario)"
  [superuser-token]
  ;; Create groups
  (e2e/graphql-data! superuser-token
    "mutation($data: UserGroupInput!) {
       syncUserGroup(data: $data) { euuid name }
     }"
    {:data {:euuid test-group-engineering-euuid
            :name "RLS_Adv_Engineering"
            :active true}})

  (e2e/graphql-data! superuser-token
    "mutation($data: UserGroupInput!) {
       syncUserGroup(data: $data) { euuid name }
     }"
    {:data {:euuid test-group-finance-euuid
            :name "RLS_Adv_Finance"
            :active true}})

  ;; Create users
  (e2e/graphql-data! superuser-token
    "mutation($data: UserInput!) {
       syncUser(data: $data) { euuid name }
     }"
    {:data {:euuid test-user-emma-euuid
            :name "emma_rls_adv"
            :password "test123"
            :active true
            :groups [{:euuid test-group-engineering-euuid}]}})

  (e2e/graphql-data! superuser-token
    "mutation($data: UserInput!) {
       syncUser(data: $data) { euuid name }
     }"
    {:data {:euuid test-user-frank-euuid
            :name "frank_rls_adv"
            :password "test123"
            :active true
            :groups [{:euuid test-group-finance-euuid}]}})

  ;; Create project (for hybrid guard testing)
  (e2e/graphql-data! superuser-token
    "mutation($data: AdvancedProjectInput!) {
       syncAdvancedProject(data: $data) { euuid name }
     }"
    {:data {:euuid test-project-1-euuid
            :name "Project Alpha"
            :manager {:euuid test-user-frank-euuid}}})

  ;; Create documents
  ;; Doc1: owner=Emma, dept=Engineering (AND guard matches)
  (e2e/graphql-data! superuser-token
    "mutation($data: AdvancedDocumentInput!) {
       syncAdvancedDocument(data: $data) { euuid title }
     }"
    {:data {:euuid test-doc-1-euuid
            :title "Doc1 - Emma + Engineering"
            :owner {:euuid test-user-emma-euuid}
            :department {:euuid test-group-engineering-euuid}
            :status "active"}})

  ;; Doc2: owner=Emma, dept=Finance (AND guard FAILS - dept mismatch)
  (e2e/graphql-data! superuser-token
    "mutation($data: AdvancedDocumentInput!) {
       syncAdvancedDocument(data: $data) { euuid title }
     }"
    {:data {:euuid test-doc-2-euuid
            :title "Doc2 - Emma + Finance (mismatch)"
            :owner {:euuid test-user-emma-euuid}
            :department {:euuid test-group-finance-euuid}
            :status "active"}})

  ;; Doc3: project manager access (Frank can read via hybrid guard)
  (e2e/graphql-data! superuser-token
    "mutation($data: AdvancedDocumentInput!) {
       syncAdvancedDocument(data: $data) { euuid title }
     }"
    {:data {:euuid test-doc-3-euuid
            :title "Doc3 - Project Alpha doc"
            :project {:euuid test-project-1-euuid}
            :status "active"}})

  ;; DocNull: all guards NULL (no one can access except superuser)
  (e2e/graphql-data! superuser-token
    "mutation($data: AdvancedDocumentInput!) {
       syncAdvancedDocument(data: $data) { euuid title }
     }"
    {:data {:euuid test-doc-null-euuid
            :title "DocNull - Orphaned"
            :status "draft"}}))


;;; ============================================================================
;;; Tests - AND Logic
;;; ============================================================================

(deftest test-and-logic-both-match
  (testing "AND logic: User sees documents when BOTH conditions match"
    (let [emma-token (e2e/create-test-token! {:name "emma_rls_adv"
                                               :euuid test-user-emma-euuid})
          docs (e2e/graphql-data! emma-token
                 "{ searchAdvancedDocument { euuid title owner { name } department { name } } }")]

      ;; Emma should see Doc1 (owner=Emma AND dept=Engineering)
      (is (some #(= (str test-doc-1-euuid) (:euuid %))
                (:searchAdvancedDocument docs))
          "Emma should see Doc1 (both owner AND dept match)"))))


(deftest test-and-logic-partial-match-denied
  (testing "AND logic: User does NOT see documents when only ONE condition matches"
    (let [emma-token (e2e/create-test-token! {:name "emma_rls_adv"
                                               :euuid test-user-emma-euuid})
          docs (e2e/graphql-data! emma-token
                 "{ searchAdvancedDocument { euuid title } }")
          doc-euuids (set (map :euuid (:searchAdvancedDocument docs)))]

      ;; Emma should NOT see Doc2 (owner=Emma but dept=Finance, not Engineering)
      (is (not (contains? doc-euuids (str test-doc-2-euuid)))
          "Emma should NOT see Doc2 (dept mismatch in AND guard)"))))


;;; ============================================================================
;;; Tests - NULL Handling
;;; ============================================================================

(deftest test-null-handling-no-access
  (testing "NULL handling: Documents with NULL guard columns not accessible"
    (let [emma-token (e2e/create-test-token! {:name "emma_rls_adv"
                                               :euuid test-user-emma-euuid})
          frank-token (e2e/create-test-token! {:name "frank_rls_adv"
                                                :euuid test-user-frank-euuid})
          emma-docs (e2e/graphql-data! emma-token
                      "{ searchAdvancedDocument { euuid title } }")
          frank-docs (e2e/graphql-data! frank-token
                       "{ searchAdvancedDocument { euuid title } }")
          all-euuids (set (concat (map :euuid (:searchAdvancedDocument emma-docs))
                                  (map :euuid (:searchAdvancedDocument frank-docs))))]

      ;; Neither Emma nor Frank should see DocNull (all fields NULL)
      (is (not (contains? all-euuids (str test-doc-null-euuid)))
          "DocNull (all NULL) should not be visible to regular users"))))


(deftest test-null-handling-superuser
  (testing "NULL handling: Superuser sees all documents including NULL fields"
    (let [su-token (e2e/create-test-token! {:name "EYWA"
                                            :euuid (:euuid *EYWA*)})
          docs (e2e/graphql-data! su-token
                 "{ searchAdvancedDocument { euuid title owner { name } } }")]

      ;; Superuser should see ALL documents including DocNull
      (is (some #(= (str test-doc-null-euuid) (:euuid %))
                (:searchAdvancedDocument docs))
          "Superuser should see DocNull (NULL fields don't block superuser)"))))


;;; ============================================================================
;;; Tests - Aggregations
;;; ============================================================================

(deftest test-aggregation-respects-guards
  (testing "Aggregations respect RLS guards"
    (let [emma-token (e2e/create-test-token! {:name "emma_rls_adv"
                                               :euuid test-user-emma-euuid})
          su-token (e2e/create-test-token! {:name "EYWA"
                                            :euuid (:euuid *EYWA*)})
          emma-count (e2e/graphql-data! emma-token
                       "{ aggregateAdvancedDocument { count } }")
          su-count (e2e/graphql-data! su-token
                     "{ aggregateAdvancedDocument { count } }")]

      ;; Emma should see count reflecting only her accessible docs
      (is (<= (:count (:aggregateAdvancedDocument emma-count))
              (:count (:aggregateAdvancedDocument su-count)))
          "Emma's count should be <= superuser count")

      ;; Emma should see 1-2 docs (Doc1 for sure, maybe Doc3 if hybrid guard works)
      (is (>= (:count (:aggregateAdvancedDocument emma-count)) 1)
          "Emma should see at least 1 document")
      (is (<= (:count (:aggregateAdvancedDocument emma-count)) 2)
          "Emma should see at most 2 documents"))))


;;; ============================================================================
;;; Tests - Nested Queries
;;; ============================================================================

(deftest test-nested-query-guards
  (testing "Nested GraphQL queries: Documents show project context"
    (let [frank-token (e2e/create-test-token! {:name "frank_rls_adv"
                                                :euuid test-user-frank-euuid})
          ;; Query documents with nested project (reverse direction)
          result (e2e/graphql-data! frank-token
                   "{ searchAdvancedDocument {
                        euuid
                        title
                        project { euuid name manager { name } }
                      } }")]

      ;; Frank should see Doc3 via hybrid guard
      (is (some #(= (str test-doc-3-euuid) (:euuid %))
                (:searchAdvancedDocument result))
          "Frank should see Doc3 (hybrid guard: project manager)")

      ;; Check that project relationship is intact
      (let [doc3 (first (filter #(= (str test-doc-3-euuid) (:euuid %))
                                (:searchAdvancedDocument result)))]
        (when doc3
          (is (= (str test-project-1-euuid) (:euuid (:project doc3)))
              "Doc3 should show its project relationship")
          (is (= "frank_rls_adv" (:name (:manager (:project doc3))))
              "Project should show Frank as manager"))))))


;;; ============================================================================
;;; Tests - Hybrid Guard
;;; ============================================================================

(deftest test-hybrid-guard-access
  (testing "Hybrid guard: Access through relation path + ref column"
    (let [frank-token (e2e/create-test-token! {:name "frank_rls_adv"
                                                :euuid test-user-frank-euuid})
          docs (e2e/graphql-data! frank-token
                 "{ searchAdvancedDocument { euuid title project { name manager { name } } } }")]

      ;; Frank should see Doc3 via hybrid guard (doc->project->manager=Frank)
      (is (some #(= (str test-doc-3-euuid) (:euuid %))
                (:searchAdvancedDocument docs))
          "Frank should see Doc3 (hybrid guard: project manager)"))))


;;; ============================================================================
;;; Test Runner
;;; ============================================================================

(defn deploy-advanced-model!
  "Deploys the advanced RLS test model."
  []
  (let [test-version (make-advanced-test-model)]
    (dataset/deploy! test-version)
    (dataset/reload)
    (println "Advanced RLS test model deployed!")
    test-version))


(defn cleanup-test-users!
  "Cleans up test users and groups."
  []
  (try
    (require '[next.jdbc :as jdbc])
    (require '[neyho.eywa.db :as db])
    ((resolve 'jdbc/execute!) (deref (resolve 'db/*db*))
     ["DELETE FROM \"user\" WHERE name IN ('emma_rls_adv', 'frank_rls_adv')"])
    ((resolve 'jdbc/execute!) (deref (resolve 'db/*db*))
     ["DELETE FROM \"user_group\" WHERE name IN ('RLS_Adv_Engineering', 'RLS_Adv_Finance')"])
    (println "Test users/groups deleted.")
    (catch Exception e
      (println "Note: Some test users/groups may not exist (OK on first run):" (.getMessage e)))))


(defn cleanup-advanced-model!
  "Destroys the advanced test model."
  []
  (cleanup-test-users!)
  (dataset/destroy-dataset nil (:dataset (make-advanced-test-model)) nil)
  (dataset/reload)
  (println "Advanced RLS test model destroyed!"))


(defn run-advanced-tests!
  "Run advanced RLS test suite.

   Workflow:
   0. Cleanup previous test users
   1. Deploy advanced model
   2. Setup test data
   3. Run tests
   4. Cleanup"
  []
  (println "\n=== RLS Advanced Test Suite ===\n")

  ;; Cleanup previous run
  (println "0. Cleaning up previous test data...")
  (cleanup-test-users!)

  ;; Deploy model
  (println "\n1. Deploying advanced test model...")
  (deploy-advanced-model!)

  ;; Setup data
  (println "\n2. Setting up test data...")
  (let [su-token (e2e/create-test-token! {:name "EYWA" :euuid (:euuid *EYWA*)})]
    (setup-advanced-test-data! su-token))
  (println "Test data created: Emma, Frank, 4 documents")

  ;; Run tests
  (println "\n3. Running advanced tests...\n")
  (let [results (clojure.test/run-tests 'dataset.rls.advanced-test)]
    ;; Cleanup
    (println "\n4. Cleaning up test tokens...")
    (e2e/cleanup-test-tokens!)
    (println "\n5. Destroying test dataset...")
    (cleanup-advanced-model!)
    results))


(comment
  ;; REPL Workflow
  (require '[rls.advanced-test :as adv-test] :reload)

  ;; Run all advanced tests
  (adv-test/run-advanced-tests!)

  ;; Step by step
  (adv-test/deploy-advanced-model!)
  (def su-token (e2e/create-test-token! {:name "EYWA"
                                         :euuid (:euuid neyho.eywa.data/*EYWA*)}))
  (adv-test/setup-advanced-test-data! su-token)
  (clojure.test/run-tests 'dataset.rls.advanced-test)
  (adv-test/cleanup-advanced-model!)

  ;;
  )
