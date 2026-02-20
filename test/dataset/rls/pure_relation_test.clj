(ns dataset.rls.pure-relation-test
  "Tests for PURE RELATION RLS guards - many-to-many relation checks.

   This test includes the IAM User entity in the test dataset (by UUID)
   and creates a many-to-many relation to it, just like robotics.json does."
  (:require
    [clojure.test :refer [deftest testing is]]
    [dataset.rls.e2e-test :as e2e]
    [neyho.eywa.data :refer [*EYWA*]]
    [neyho.eywa.dataset :as dataset]
    [neyho.eywa.dataset.core :as core]
    [neyho.eywa.iam.uuids :as iu]
    [dataset.test-helpers :refer [make-entity make-relation add-test-attribute]]))


;;; ============================================================================
;;; Test Model - Pure Relation Guard
;;; ============================================================================

;; Entity UUIDs
(def document-euuid #uuid "f1000001-0000-0000-0000-000000000001")

;; Attribute UUIDs
(def doc-title-euuid #uuid "f1000002-0000-0000-0000-000000000001")
(def doc-content-euuid #uuid "f1000002-0000-0000-0000-000000000002")

;; Relation UUIDs
(def doc-watchers-rel-euuid #uuid "f1000003-0000-0000-0000-000000000001")

;; Test data UUIDs
(def test-user-alice-euuid #uuid "f1000004-0000-0000-0000-000000000001")
(def test-user-bob-euuid #uuid "f1000004-0000-0000-0000-000000000002")
(def test-doc-1-euuid #uuid "f1000005-0000-0000-0000-000000000001")
(def test-doc-2-euuid #uuid "f1000005-0000-0000-0000-000000000002")


(defn make-pure-relation-test-model
  "Creates a test model with PURE RELATION guards (m2m).

   This model includes the IAM User entity (by its known UUID)
   and creates a many-to-many 'watchers' relation, just like
   robotics.json includes User and creates m2m relations to it.

   Entities:
   - User (IAM entity - included by reference)
   - PureRelDocument (our test entity)

   Relations:
   - PureRelDocument watchers User (many-to-many)

   Guards:
   - Pure relation: user is in document.watchers"
  []
  (let [;; Get the already-deployed User entity from the system!
        user-entity (dataset/deployed-entity iu/user)

        ;; Document entity with pure relation guard
        doc-entity (-> (core/map->ERDEntity
                         {:euuid document-euuid
                          :name "PureRelDocument"
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
                            [;; PURE RELATION GUARD: user in watchers
                             {:id (java.util.UUID/randomUUID)
                              :operation #{:read :write}
                              :conditions
                              [{:type :relation
                                :steps [{:relation-euuid doc-watchers-rel-euuid}]
                                :target :user}]}]}}})
                       (core/add-attribute
                         {:euuid doc-title-euuid
                          :name "title"
                          :type "string"
                          :constraint "optional"
                          :active true})
                       (core/add-attribute
                         {:euuid doc-content-euuid
                          :name "content"
                          :type "string"
                          :constraint "optional"
                          :active true}))

        ;; Build model
        model (-> (core/map->ERDModel {})
                  (core/add-entity user-entity)
                  (core/add-entity doc-entity)
                  ;; Many-to-many: Document watchers User
                  (core/add-relation
                    (make-relation
                      {:euuid doc-watchers-rel-euuid
                       :from document-euuid
                       :to iu/user  ; Relation to IAM User entity!
                       :from-label "watched_documents"
                       :to-label "watchers"
                       :cardinality "m2m"})))]

    {:euuid #uuid "f1000000-0000-0000-0000-000000000001"
     :name "Pure Relation RLS Test Dataset"
     :version "1.0.0"
     :dataset {:euuid #uuid "f1000000-0000-0000-0000-000000000000"
               :name "PureRelRLS"}
     :model model}))


;;; ============================================================================
;;; Test Data Setup
;;; ============================================================================

(defn setup-pure-relation-test-data!
  "Sets up test data for pure relation RLS testing.

   Users:
   - Alice (watcher of Doc1)
   - Bob (watcher of Doc2)

   Documents:
   - Doc1: watchers = [Alice]
   - Doc2: watchers = [Bob]

   Expected:
   - Alice sees Doc1 (is watcher)
   - Alice does NOT see Doc2 (not watcher)
   - Bob sees Doc2 (is watcher)
   - Bob does NOT see Doc1 (not watcher)"
  [superuser-token]
  ;; Create users
  (e2e/graphql-data! superuser-token
    "mutation($data: UserInput!) {
       syncUser(data: $data) { euuid name }
     }"
    {:data {:euuid test-user-alice-euuid
            :name "alice_pure_rel"
            :password "test123"
            :active true}})

  (e2e/graphql-data! superuser-token
    "mutation($data: UserInput!) {
       syncUser(data: $data) { euuid name }
     }"
    {:data {:euuid test-user-bob-euuid
            :name "bob_pure_rel"
            :password "test123"
            :active true}})

  ;; Create documents with watchers (m2m relation)
  ;; Doc1: Alice is watcher
  (e2e/graphql-data! superuser-token
    "mutation($data: PureRelDocumentInput!) {
       syncPureRelDocument(data: $data) { euuid title }
     }"
    {:data {:euuid test-doc-1-euuid
            :title "Doc1 - Alice watching"
            :content "Secret content for Alice"
            :watchers [{:euuid test-user-alice-euuid}]}})

  ;; Doc2: Bob is watcher
  (e2e/graphql-data! superuser-token
    "mutation($data: PureRelDocumentInput!) {
       syncPureRelDocument(data: $data) { euuid title }
     }"
    {:data {:euuid test-doc-2-euuid
            :title "Doc2 - Bob watching"
            :content "Secret content for Bob"
            :watchers [{:euuid test-user-bob-euuid}]}}))


;;; ============================================================================
;;; Tests - Pure Relation Guards
;;; ============================================================================

(deftest test-pure-relation-alice-sees-doc1
  (testing "Pure relation: Alice sees Doc1 (she is watcher)"
    (let [alice-token (e2e/create-test-token! {:name "alice_pure_rel"
                                                :euuid test-user-alice-euuid})
          docs (e2e/graphql-data! alice-token
                 "{ searchPureRelDocument { euuid title watchers { name } } }")]

      ;; Alice should see Doc1 (she's in watchers list)
      (is (some #(= (str test-doc-1-euuid) (:euuid %))
                (:searchPureRelDocument docs))
          "Alice should see Doc1 (pure relation: alice IN doc1.watchers)")

      ;; Verify watchers relation is accessible
      (let [doc1 (first (filter #(= (str test-doc-1-euuid) (:euuid %))
                                (:searchPureRelDocument docs)))]
        (when doc1
          (is (some #(= "alice_pure_rel" (:name %)) (:watchers doc1))
              "Doc1 should show Alice in watchers list"))))))


(deftest test-pure-relation-alice-not-see-doc2
  (testing "Pure relation: Alice does NOT see Doc2 (not watcher)"
    (let [alice-token (e2e/create-test-token! {:name "alice_pure_rel"
                                                :euuid test-user-alice-euuid})
          docs (e2e/graphql-data! alice-token
                 "{ searchPureRelDocument { euuid title } }")
          doc-euuids (set (map :euuid (:searchPureRelDocument docs)))]

      ;; Alice should NOT see Doc2 (she's NOT in watchers list)
      (is (not (contains? doc-euuids (str test-doc-2-euuid)))
          "Alice should NOT see Doc2 (not in watchers)"))))


(deftest test-pure-relation-bob-sees-doc2
  (testing "Pure relation: Bob sees Doc2 (he is watcher)"
    (let [bob-token (e2e/create-test-token! {:name "bob_pure_rel"
                                              :euuid test-user-bob-euuid})
          docs (e2e/graphql-data! bob-token
                 "{ searchPureRelDocument { euuid title watchers { name } } }")]

      ;; Bob should see Doc2 (he's in watchers list)
      (is (some #(= (str test-doc-2-euuid) (:euuid %))
                (:searchPureRelDocument docs))
          "Bob should see Doc2 (pure relation: bob IN doc2.watchers)"))))


(deftest test-pure-relation-bob-not-see-doc1
  (testing "Pure relation: Bob does NOT see Doc1 (not watcher)"
    (let [bob-token (e2e/create-test-token! {:name "bob_pure_rel"
                                              :euuid test-user-bob-euuid})
          docs (e2e/graphql-data! bob-token
                 "{ searchPureRelDocument { euuid title } }")
          doc-euuids (set (map :euuid (:searchPureRelDocument docs)))]

      ;; Bob should NOT see Doc1 (he's NOT in watchers list)
      (is (not (contains? doc-euuids (str test-doc-1-euuid)))
          "Bob should NOT see Doc1 (not in watchers)"))))


(deftest test-pure-relation-write-for-watcher
  (testing "Pure relation: Watcher can write to watched document"
    (let [alice-token (e2e/create-test-token! {:name "alice_pure_rel"
                                                :euuid test-user-alice-euuid})]

      ;; Alice updates Doc1 (she's watcher) - should SUCCEED
      (is (= "Alice updated this"
             (:title (:syncPureRelDocument
                       (e2e/graphql-data! alice-token
                         "mutation($data: PureRelDocumentInput!) {
                            syncPureRelDocument(data: $data) { euuid title }
                          }"
                         {:data {:euuid test-doc-1-euuid
                                 :title "Alice updated this"}}))))
          "Alice should be able to update Doc1 (she's watcher)"))))


(deftest test-pure-relation-write-denied-non-watcher
  (testing "Pure relation: Non-watcher cannot write to document"
    (let [alice-token (e2e/create-test-token! {:name "alice_pure_rel"
                                                :euuid test-user-alice-euuid})]

      ;; Alice tries to update Doc2 (Bob's doc) - should FAIL
      (let [result (e2e/graphql! alice-token
                     "mutation($data: PureRelDocumentInput!) {
                        syncPureRelDocument(data: $data) { euuid title }
                      }"
                     {:data {:euuid test-doc-2-euuid
                             :title "Alice tries to hack Bob's doc"}})]
        (is (seq (:errors result))
            "Alice should NOT be able to update Doc2 (not watcher)")
        (is (some #(re-find #"(?i)rls|access|denied" (str %)) (:errors result))
            "Error should mention RLS or access denied")))))


(deftest test-pure-relation-superuser-sees-all
  (testing "Pure relation: Superuser sees all documents"
    (let [su-token (e2e/create-test-token! {:name "EYWA"
                                            :euuid (:euuid *EYWA*)})
          docs (e2e/graphql-data! su-token
                 "{ searchPureRelDocument { euuid title } }")]

      ;; Superuser should see both documents
      (is (>= (count (:searchPureRelDocument docs)) 2)
          "Superuser should see all documents")

      (let [doc-euuids (set (map :euuid (:searchPureRelDocument docs)))]
        (is (contains? doc-euuids (str test-doc-1-euuid))
            "Superuser should see Doc1")
        (is (contains? doc-euuids (str test-doc-2-euuid))
            "Superuser should see Doc2")))))


;;; ============================================================================
;;; Test Runner
;;; ============================================================================

(defn deploy-pure-relation-model!
  "Deploys the pure relation RLS test model."
  []
  (let [test-version (make-pure-relation-test-model)]
    (dataset/deploy! test-version)
    (dataset/reload)
    (println "Pure relation RLS test model deployed!")
    test-version))


(defn cleanup-test-users!
  "Cleans up test users."
  []
  (try
    (require '[next.jdbc :as jdbc])
    (require '[neyho.eywa.db :as db])
    ((resolve 'jdbc/execute!) (deref (resolve 'db/*db*))
     ["DELETE FROM \"user\" WHERE name IN ('alice_pure_rel', 'bob_pure_rel')"])
    (println "Test users deleted.")
    (catch Exception e
      (println "Note: Some test users may not exist (OK on first run):" (.getMessage e)))))


(defn cleanup-pure-relation-model!
  "Destroys the pure relation test model."
  []
  (cleanup-test-users!)
  (dataset/destroy-dataset nil (:dataset (make-pure-relation-test-model)) nil)
  (dataset/reload)
  (println "Pure relation RLS test model destroyed!"))


(defn run-pure-relation-tests!
  "Run pure relation RLS test suite.

   This test proves that:
   1. You CAN include IAM entities in other datasets
   2. You CAN create many-to-many relations to IAM entities
   3. Pure :relation guards work correctly

   The pattern (used in robotics.json):
   - Include User entity by its known UUID
   - Create m2m relation to it
   - Deploy creates junction table
   - RLS guards use the junction table"
  []
  (println "\n=== RLS Pure Relation Test Suite ===\n")

  ;; Cleanup previous run
  (println "0. Cleaning up previous test data...")
  (cleanup-test-users!)

  ;; Deploy model
  (println "\n1. Deploying pure relation test model...")
  (deploy-pure-relation-model!)

  ;; Setup data
  (println "\n2. Setting up test data...")
  (let [su-token (e2e/create-test-token! {:name "EYWA" :euuid (:euuid *EYWA*)})]
    (setup-pure-relation-test-data! su-token))
  (println "Test data created: Alice, Bob, 2 documents with watchers")

  ;; Run tests
  (println "\n3. Running pure relation tests...\n")
  (let [results (clojure.test/run-tests 'dataset.rls.pure-relation-test)]
    ;; Cleanup
    (println "\n4. Cleaning up test tokens...")
    (e2e/cleanup-test-tokens!)
    (println "\n5. Destroying test dataset...")
    (cleanup-pure-relation-model!)
    results))


(comment
  ;; REPL Workflow
  (require '[rls.pure-relation-test :as pure-test] :reload)

  ;; Run all pure relation tests
  (pure-test/run-pure-relation-tests!)

  ;; Step by step
  (pure-test/deploy-pure-relation-model!)
  (def su-token (e2e/create-test-token! {:name "EYWA"
                                         :euuid (:euuid neyho.eywa.data/*EYWA*)}))
  (pure-test/setup-pure-relation-test-data! su-token)
  (clojure.test/run-tests 'dataset.rls.pure-relation-test)
  (pure-test/cleanup-pure-relation-model!)

  ;;
  )
