(ns dataset.rls.merge-test
  "Unit tests for RLS guard merging during model join.
   Tests the merge-entity-rls function and its integration with join-models."
  (:require
    [clojure.test :refer [deftest testing is]]
    [dataset.test-helpers :refer [make-entity]]
    [neyho.eywa.dataset.core :as core]))


;;; ============================================================================
;;; Test Fixtures
;;; ============================================================================

(defn make-guard
  "Create a guard with the given id, operations, and conditions"
  [{:keys [id operation conditions]}]
  {:id id
   :operation (or operation #{})
   :conditions (or conditions [])})


(defn make-entity-with-rls
  "Create an entity with RLS configuration"
  [{:keys [euuid name enabled guards]}]
  (-> (make-entity {:euuid euuid :name name})
      (assoc-in [:configuration :rls]
                {:enabled (boolean enabled)
                 :guards (or guards [])})))


;;; ============================================================================
;;; merge-entity-rls Tests (via merge-entity-attributes)
;;; ============================================================================

(deftest test-merge-rls-empty-configs
  (testing "Merging two entities with no RLS config"
    (let [entity1 (make-entity {:euuid #uuid "11111111-1111-1111-1111-111111111111"
                                :name "Entity1"})
          entity2 (make-entity {:euuid #uuid "11111111-1111-1111-1111-111111111111"
                                :name "Entity1"})
          ;; Use the private merge function via join-models
          model1 (core/set-entity (core/map->ERDModel {}) entity1)
          model2 (core/set-entity (core/map->ERDModel {}) entity2)
          merged (core/join-models model1 model2)
          merged-entity (core/get-entity merged #uuid "11111111-1111-1111-1111-111111111111")]
      (is (= {:enabled false :guards []}
             (get-in merged-entity [:configuration :rls]))))))


(deftest test-merge-rls-union-guards
  (testing "Guards from both entities are unioned"
    (let [guard-a {:id #uuid "aaaa1111-1111-1111-1111-111111111111"
                   :operation #{:read}
                   :conditions [{:type :ref :attribute #uuid "a0000001-0001-0001-0001-000000000001"}]}
          guard-b {:id #uuid "bbbb2222-2222-2222-2222-222222222222"
                   :operation #{:read :write}
                   :conditions [{:type :ref :attribute #uuid "a0000002-0002-0002-0002-000000000002"}]}
          entity1 (make-entity-with-rls
                    {:euuid #uuid "11111111-1111-1111-1111-111111111111"
                     :name "Entity1"
                     :enabled true
                     :guards [guard-a]})
          entity2 (make-entity-with-rls
                    {:euuid #uuid "11111111-1111-1111-1111-111111111111"
                     :name "Entity1"
                     :enabled true
                     :guards [guard-b]})
          model1 (core/set-entity (core/map->ERDModel {}) entity1)
          model2 (core/set-entity (core/map->ERDModel {}) entity2)
          merged (core/join-models model1 model2)
          merged-entity (core/get-entity merged #uuid "11111111-1111-1111-1111-111111111111")
          merged-guards (get-in merged-entity [:configuration :rls :guards])]
      ;; Both guards should be present
      (is (= 2 (count merged-guards)))
      (is (some #(= (:id %) #uuid "aaaa1111-1111-1111-1111-111111111111") merged-guards))
      (is (some #(= (:id %) #uuid "bbbb2222-2222-2222-2222-222222222222") merged-guards)))))


(deftest test-merge-rls-same-guard-id-entity2-wins
  (testing "When same guard ID exists in both, entity2 wins for operations"
    (let [guard-id #uuid "aaaa1111-1111-1111-1111-111111111111"
          guard1 {:id guard-id
                  :operation #{:read}
                  :conditions [{:type :ref :attribute #uuid "a0000001-0001-0001-0001-000000000001"}]}
          guard2 {:id guard-id
                  :operation #{:read :write :delete}
                  :conditions [{:type :ref :attribute #uuid "a0000001-0001-0001-0001-000000000001"}]}
          entity1 (make-entity-with-rls
                    {:euuid #uuid "11111111-1111-1111-1111-111111111111"
                     :name "Entity1"
                     :enabled true
                     :guards [guard1]})
          entity2 (make-entity-with-rls
                    {:euuid #uuid "11111111-1111-1111-1111-111111111111"
                     :name "Entity1"
                     :enabled true
                     :guards [guard2]})
          model1 (core/set-entity (core/map->ERDModel {}) entity1)
          model2 (core/set-entity (core/map->ERDModel {}) entity2)
          merged (core/join-models model1 model2)
          merged-entity (core/get-entity merged #uuid "11111111-1111-1111-1111-111111111111")
          merged-guards (get-in merged-entity [:configuration :rls :guards])]
      ;; Should have exactly one guard
      (is (= 1 (count merged-guards)))
      ;; Operations should be from entity2
      (is (= #{:read :write :delete} (:operation (first merged-guards)))))))


(deftest test-merge-rls-enabled-entity2-wins
  (testing "Entity2's enabled flag wins (last deployed)"
    (let [entity1 (make-entity-with-rls
                    {:euuid #uuid "11111111-1111-1111-1111-111111111111"
                     :name "Entity1"
                     :enabled true
                     :guards []})
          entity2 (make-entity-with-rls
                    {:euuid #uuid "11111111-1111-1111-1111-111111111111"
                     :name "Entity1"
                     :enabled false
                     :guards []})
          model1 (core/set-entity (core/map->ERDModel {}) entity1)
          model2 (core/set-entity (core/map->ERDModel {}) entity2)
          merged (core/join-models model1 model2)
          merged-entity (core/get-entity merged #uuid "11111111-1111-1111-1111-111111111111")]
      ;; Entity2 has enabled=false, so result should be false
      (is (false? (get-in merged-entity [:configuration :rls :enabled])))))

  (testing "Entity2's enabled flag wins - enabling"
    (let [entity1 (make-entity-with-rls
                    {:euuid #uuid "11111111-1111-1111-1111-111111111111"
                     :name "Entity1"
                     :enabled false
                     :guards []})
          entity2 (make-entity-with-rls
                    {:euuid #uuid "11111111-1111-1111-1111-111111111111"
                     :name "Entity1"
                     :enabled true
                     :guards []})
          model1 (core/set-entity (core/map->ERDModel {}) entity1)
          model2 (core/set-entity (core/map->ERDModel {}) entity2)
          merged (core/join-models model1 model2)
          merged-entity (core/get-entity merged #uuid "11111111-1111-1111-1111-111111111111")]
      ;; Entity2 has enabled=true, so result should be true
      (is (true? (get-in merged-entity [:configuration :rls :enabled]))))))


(deftest test-merge-rls-entity1-only-has-guards
  (testing "When only entity1 has guards, they are preserved"
    (let [guard-a {:id #uuid "aaaa1111-1111-1111-1111-111111111111"
                   :operation #{:read}
                   :conditions [{:type :ref :attribute #uuid "a0000001-0001-0001-0001-000000000001"}]}
          entity1 (make-entity-with-rls
                    {:euuid #uuid "11111111-1111-1111-1111-111111111111"
                     :name "Entity1"
                     :enabled true
                     :guards [guard-a]})
          entity2 (make-entity-with-rls
                    {:euuid #uuid "11111111-1111-1111-1111-111111111111"
                     :name "Entity1"
                     :enabled true
                     :guards []})
          model1 (core/set-entity (core/map->ERDModel {}) entity1)
          model2 (core/set-entity (core/map->ERDModel {}) entity2)
          merged (core/join-models model1 model2)
          merged-entity (core/get-entity merged #uuid "11111111-1111-1111-1111-111111111111")
          merged-guards (get-in merged-entity [:configuration :rls :guards])]
      ;; Guard from entity1 should be preserved
      (is (= 1 (count merged-guards)))
      (is (= #uuid "aaaa1111-1111-1111-1111-111111111111" (:id (first merged-guards)))))))


(deftest test-merge-rls-entity2-only-has-guards
  (testing "When only entity2 has guards, they are kept"
    (let [guard-b {:id #uuid "bbbb2222-2222-2222-2222-222222222222"
                   :operation #{:read :write}
                   :conditions [{:type :ref :attribute #uuid "a0000002-0002-0002-0002-000000000002"}]}
          entity1 (make-entity-with-rls
                    {:euuid #uuid "11111111-1111-1111-1111-111111111111"
                     :name "Entity1"
                     :enabled false
                     :guards []})
          entity2 (make-entity-with-rls
                    {:euuid #uuid "11111111-1111-1111-1111-111111111111"
                     :name "Entity1"
                     :enabled true
                     :guards [guard-b]})
          model1 (core/set-entity (core/map->ERDModel {}) entity1)
          model2 (core/set-entity (core/map->ERDModel {}) entity2)
          merged (core/join-models model1 model2)
          merged-entity (core/get-entity merged #uuid "11111111-1111-1111-1111-111111111111")
          merged-guards (get-in merged-entity [:configuration :rls :guards])]
      ;; Guard from entity2 should be present
      (is (= 1 (count merged-guards)))
      (is (= #uuid "bbbb2222-2222-2222-2222-222222222222" (:id (first merged-guards)))))))


(deftest test-merge-rls-multiple-guards-complex
  (testing "Complex scenario: multiple guards, some shared IDs, some unique"
    (let [;; Shared guard ID - entity2 should win
          shared-guard-id #uuid "cccc3333-3333-3333-3333-333333333333"
          guard-shared-v1 {:id shared-guard-id
                           :operation #{:read}
                           :conditions [{:type :ref :attribute #uuid "a0000003-0003-0003-0003-000000000003"}]}
          guard-shared-v2 {:id shared-guard-id
                           :operation #{:read :write}
                           :conditions [{:type :ref :attribute #uuid "a0000003-0003-0003-0003-000000000003"}]}
          ;; Unique guards
          guard-a {:id #uuid "aaaa1111-1111-1111-1111-111111111111"
                   :operation #{:read}
                   :conditions [{:type :ref :attribute #uuid "a0000001-0001-0001-0001-000000000001"}]}
          guard-b {:id #uuid "bbbb2222-2222-2222-2222-222222222222"
                   :operation #{:delete}
                   :conditions [{:type :ref :attribute #uuid "a0000002-0002-0002-0002-000000000002"}]}
          entity1 (make-entity-with-rls
                    {:euuid #uuid "11111111-1111-1111-1111-111111111111"
                     :name "Entity1"
                     :enabled true
                     :guards [guard-shared-v1 guard-a]})
          entity2 (make-entity-with-rls
                    {:euuid #uuid "11111111-1111-1111-1111-111111111111"
                     :name "Entity1"
                     :enabled true
                     :guards [guard-shared-v2 guard-b]})
          model1 (core/set-entity (core/map->ERDModel {}) entity1)
          model2 (core/set-entity (core/map->ERDModel {}) entity2)
          merged (core/join-models model1 model2)
          merged-entity (core/get-entity merged #uuid "11111111-1111-1111-1111-111111111111")
          merged-guards (get-in merged-entity [:configuration :rls :guards])
          guards-by-id (into {} (map (juxt :id identity) merged-guards))]
      ;; Should have 3 guards total (1 shared + 2 unique)
      (is (= 3 (count merged-guards)))
      ;; Shared guard should have entity2's operations
      (is (= #{:read :write} (:operation (get guards-by-id shared-guard-id))))
      ;; Unique guards should be present
      (is (some? (get guards-by-id #uuid "aaaa1111-1111-1111-1111-111111111111")))
      (is (some? (get guards-by-id #uuid "bbbb2222-2222-2222-2222-222222222222"))))))


(deftest test-merge-rls-no-rls-config-in-entity1
  (testing "When entity1 has no RLS config at all"
    (let [guard-b {:id #uuid "bbbb2222-2222-2222-2222-222222222222"
                   :operation #{:read}
                   :conditions []}
          entity1 (make-entity {:euuid #uuid "11111111-1111-1111-1111-111111111111"
                                :name "Entity1"})
          entity2 (make-entity-with-rls
                    {:euuid #uuid "11111111-1111-1111-1111-111111111111"
                     :name "Entity1"
                     :enabled true
                     :guards [guard-b]})
          model1 (core/set-entity (core/map->ERDModel {}) entity1)
          model2 (core/set-entity (core/map->ERDModel {}) entity2)
          merged (core/join-models model1 model2)
          merged-entity (core/get-entity merged #uuid "11111111-1111-1111-1111-111111111111")
          merged-rls (get-in merged-entity [:configuration :rls])]
      (is (true? (:enabled merged-rls)))
      (is (= 1 (count (:guards merged-rls)))))))


(deftest test-merge-rls-no-rls-config-in-entity2
  (testing "When entity2 has no RLS config at all, entity1's enabled is preserved"
    (let [guard-a {:id #uuid "aaaa1111-1111-1111-1111-111111111111"
                   :operation #{:read}
                   :conditions []}
          entity1 (make-entity-with-rls
                    {:euuid #uuid "11111111-1111-1111-1111-111111111111"
                     :name "Entity1"
                     :enabled true
                     :guards [guard-a]})
          entity2 (make-entity {:euuid #uuid "11111111-1111-1111-1111-111111111111"
                                :name "Entity1"})
          model1 (core/set-entity (core/map->ERDModel {}) entity1)
          model2 (core/set-entity (core/map->ERDModel {}) entity2)
          merged (core/join-models model1 model2)
          merged-entity (core/get-entity merged #uuid "11111111-1111-1111-1111-111111111111")
          merged-rls (get-in merged-entity [:configuration :rls])]
      ;; Entity2 has no RLS config, so entity1's enabled value is preserved
      ;; (absence of config != explicit disable)
      (is (true? (:enabled merged-rls)))
      ;; Guard from entity1 should still be preserved
      (is (= 1 (count (:guards merged-rls)))))))
