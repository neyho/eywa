(ns dataset.test-type-conversion
  (:require
   [clojure.test :refer [deftest is testing]]
   [neyho.eywa.dataset.core :as core]
   [neyho.eywa.dataset.postgres :as pg]))

(def test-entity {:name "TestEntity"
                  :euuid "test-entity-id"})
(def test-attribute {:name "testAttribute"
                     :euuid "test-attr-id"})

(deftest test-safe-conversions
  (testing "Safe conversions should return {:safe true}"
    (is (= {:safe true} (core/validate-type-conversion "string" "string")))
    (is (= {:safe true} (core/validate-type-conversion "int" "string")))
    (is (= {:safe true} (core/validate-type-conversion "string" "avatar")))
    (is (= {:safe true} (core/validate-type-conversion "avatar" "transit")))
    (is (= {:safe true} (core/validate-type-conversion "json" "currency")))
    (is (= {:safe true} (core/validate-type-conversion "encrypted" "json")))
    (is (= {:safe true} (core/validate-type-conversion "int" "float")))
    (is (= {:safe true} (core/validate-type-conversion "enum" "string")))))

(deftest test-warning-conversions
  (testing "Risky conversions should return {:warning ...}"
    (is (:warning (core/validate-type-conversion "float" "int")))
    (is (:warning (core/validate-type-conversion "string" "int")))
    (is (:warning (core/validate-type-conversion "string" "float")))
    (is (:warning (core/validate-type-conversion "string" "boolean")))
    (is (:warning (core/validate-type-conversion "string" "timestamp")))
    (is (:warning (core/validate-type-conversion "string" "json")))
    (is (:warning (core/validate-type-conversion "user" "group")))
    (is (:warning (core/validate-type-conversion "timestamp" "string")))))

(deftest test-forbidden-conversions
  (testing "Forbidden conversions should return {:error ...}"
    (is (:error (core/validate-type-conversion "avatar" "json")))
    (is (:error (core/validate-type-conversion "json" "int")))
    (is (:error (core/validate-type-conversion "json" "boolean")))
    (is (:error (core/validate-type-conversion "timestamp" "int")))
    (is (:error (core/validate-type-conversion "boolean" "int")))
    (is (:error (core/validate-type-conversion "user" "int")))
    (is (:error (core/validate-type-conversion "encrypted" "int"))))

  (testing "Note: enum → enum at TYPE level is safe (no type change). Changing enum VALUES is handled separately in attribute-delta->ddl"
    (is (= {:safe true} (core/validate-type-conversion "enum" "enum")))))

(deftest test-check-type-conversion-throws
  (testing "check-type-conversion! should throw on forbidden conversions"
    (is (thrown? clojure.lang.ExceptionInfo
                 (pg/check-type-conversion! test-entity test-attribute "avatar" "json")))

    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo
         #"Forbidden type conversion.*avatar.*json"
         (pg/check-type-conversion! test-entity test-attribute "avatar" "json")))

    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo
         #"Cannot convert json to int"
         (pg/check-type-conversion! test-entity test-attribute "json" "int")))))

(deftest test-check-type-conversion-returns-true
  (testing "check-type-conversion! should return true for safe conversions"
    (is (true? (pg/check-type-conversion! test-entity test-attribute "string" "avatar")))
    (is (true? (pg/check-type-conversion! test-entity test-attribute "int" "float")))))

(deftest test-type-family-logic
  (testing "Type family detection"
    (is (= :text (core/get-type-family "string")))
    (is (= :text (core/get-type-family "avatar")))
    (is (= :json (core/get-type-family "json")))
    (is (= :numeric (core/get-type-family "int")))
    (is (= :reference (core/get-type-family "user")))
    (is (nil? (core/get-type-family "unknown"))))

  (testing "Same family detection"
    (is (true? (core/same-family? "string" "avatar")))
    (is (true? (core/same-family? "json" "encrypted")))
    (is (false? (core/same-family? "string" "int")))
    (is (false? (core/same-family? "user" "int")))))

(deftest test-error-info-structure
  (testing "Error info contains useful metadata"
    (let [result (core/validate-type-conversion "avatar" "json")]
      (is (:error result))
      (is (:type result))
      (is (= :neyho.eywa.dataset.core/forbidden-conversion (:type result)))
      (is (:suggestion result))
      (is (string? (:suggestion result)))))

  (testing "Thrown exception contains structured data"
    (try
      (pg/check-type-conversion! test-entity test-attribute "json" "boolean")
      (is false "Should have thrown")
      (catch clojure.lang.ExceptionInfo e
        (let [data (ex-data e)]
          (is (= "TestEntity" (:entity data)))
          (is (= "testAttribute" (:attribute data)))
          (is (= "json" (:from-type data)))
          (is (= "boolean" (:to-type data)))
          (is (string? (:suggestion data))))))))

(deftest test-helper-functions
  (testing "can-convert-type? returns correct boolean"
    (is (true? (core/can-convert-type? "string" "int")))
    (is (true? (core/can-convert-type? "string" "avatar")))
    (is (false? (core/can-convert-type? "avatar" "json")))
    (is (false? (core/can-convert-type? "json" "int"))))

  (testing "get-conversion-level returns correct level"
    (is (= :safe (core/get-conversion-level "string" "avatar")))
    (is (= :warning (core/get-conversion-level "float" "int")))
    (is (= :error (core/get-conversion-level "avatar" "json")))))

(deftest test-get-allowed-conversions
  (testing "get-allowed-conversions returns proper structure"
    (let [result (core/get-allowed-conversions "avatar")]
      (is (map? result))
      (is (contains? result :safe))
      (is (contains? result :warning))
      (is (contains? result :forbidden))
      (is (contains? result :error))
      (is (vector? (:safe result)))
      (is (vector? (:warning result)))
      (is (vector? (:error result)))))

  (testing "avatar allowed conversions are correct"
    (let [result (core/get-allowed-conversions "avatar")]
      (is (some #(= "string" %) (:safe result)))
      (is (some #(= "transit" %) (:safe result)))
      (is (some #(= "json" %) (:error result)))
      (is (some #(= "int" %) (:error result)))))

  (testing "string allowed conversions are correct"
    (let [result (core/get-allowed-conversions "string")]
      (is (some #(= "avatar" %) (:safe result)))
      (is (some #(= "int" %) (:warning result)))
      (is (some #(= "float" %) (:warning result))))))

(deftest test-get-convertible-types
  (testing "get-convertible-types returns only valid types by default"
    (let [result (core/get-convertible-types "avatar")]
      (is (sequential? result))
      (is (some #(= "string" %) result))
      (is (some #(= "transit" %) result))
      (is (not (some #(= "json" %) result)))
      (is (not (some #(= "int" %) result)))))

  (testing "get-convertible-types with include-warnings? false"
    (let [result (core/get-convertible-types "string" {:include-warnings? false})]
      (is (some #(= "avatar" %) result))
      (is (not (some #(= "int" %) result)))
      (is (not (some #(= "float" %) result)))))

  (testing "get-convertible-types with include-warnings? true"
    (let [result (core/get-convertible-types "string" {:include-warnings? true})]
      (is (some #(= "avatar" %) result))
      (is (some #(= "int" %) result))
      (is (some #(= "float" %) result)))))

(comment
  ;; Manual testing examples
  (run-tests)

  ;; Test specific conversions
  (core/validate-type-conversion "string" "int")
  (core/validate-type-conversion "avatar" "json")
  (core/validate-type-conversion "user" "group")

  ;; Test helper functions
  (core/get-allowed-conversions "avatar")
  (core/get-convertible-types "string")
  (core/get-type-conversion-info "string" "int")

  ;; Test that errors are thrown
  (try
    (pg/check-type-conversion! test-entity test-attribute "boolean" "int")
    (catch Exception e
      (println "Error:" (.getMessage e))
      (println "Data:" (ex-data e)))))
