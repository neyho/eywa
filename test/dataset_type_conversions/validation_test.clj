(ns dataset-type-conversions.validation-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [neyho.eywa.dataset.postgres :as pg]))

(def test-entity {:name "TestEntity" :euuid "test-entity-id"})
(def test-attribute {:name "testAttribute" :euuid "test-attr-id"})

(deftest test-safe-conversions
  (testing "Safe conversions should return {:safe true}"
    (is (= {:safe true} (pg/validate-type-conversion "string" "string")))
    (is (= {:safe true} (pg/validate-type-conversion "int" "string")))
    (is (= {:safe true} (pg/validate-type-conversion "string" "avatar")))
    (is (= {:safe true} (pg/validate-type-conversion "avatar" "transit")))
    (is (= {:safe true} (pg/validate-type-conversion "json" "currency")))
    (is (= {:safe true} (pg/validate-type-conversion "encrypted" "json")))
    (is (= {:safe true} (pg/validate-type-conversion "int" "float")))
    (is (= {:safe true} (pg/validate-type-conversion "enum" "string")))))

(deftest test-warning-conversions
  (testing "Risky conversions should return {:warning ...}"
    (is (:warning (pg/validate-type-conversion "float" "int")))
    (is (:warning (pg/validate-type-conversion "string" "int")))
    (is (:warning (pg/validate-type-conversion "string" "float")))
    (is (:warning (pg/validate-type-conversion "string" "boolean")))
    (is (:warning (pg/validate-type-conversion "string" "timestamp")))
    (is (:warning (pg/validate-type-conversion "string" "json")))
    (is (:warning (pg/validate-type-conversion "user" "group")))
    (is (:warning (pg/validate-type-conversion "timestamp" "string")))))

(deftest test-forbidden-conversions
  (testing "Forbidden conversions should return {:error ...}"
    (is (:error (pg/validate-type-conversion "avatar" "json")))
    (is (:error (pg/validate-type-conversion "json" "int")))
    (is (:error (pg/validate-type-conversion "json" "boolean")))
    (is (:error (pg/validate-type-conversion "timestamp" "int")))
    (is (:error (pg/validate-type-conversion "boolean" "int")))
    (is (:error (pg/validate-type-conversion "user" "int")))
    (is (:error (pg/validate-type-conversion "encrypted" "int"))))

  (testing "Note: enum → enum at TYPE level is safe (no type change). Changing enum VALUES is handled separately in attribute-delta->ddl"
    (is (= {:safe true} (pg/validate-type-conversion "enum" "enum")))))

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
    (is (= :text (pg/get-type-family "string")))
    (is (= :text (pg/get-type-family "avatar")))
    (is (= :json (pg/get-type-family "json")))
    (is (= :numeric (pg/get-type-family "int")))
    (is (= :reference (pg/get-type-family "user")))
    (is (nil? (pg/get-type-family "unknown"))))

  (testing "Same family detection"
    (is (true? (pg/same-family? "string" "avatar")))
    (is (true? (pg/same-family? "json" "encrypted")))
    (is (false? (pg/same-family? "string" "int")))
    (is (false? (pg/same-family? "user" "int")))))

(deftest test-error-info-structure
  (testing "Error info contains useful metadata"
    (let [result (pg/validate-type-conversion "avatar" "json")]
      (is (:error result))
      (is (:type result))
      (is (= :neyho.eywa.dataset.postgres/forbidden-conversion (:type result)))
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

(comment
  ;; Manual testing examples
  (run-tests)

  ;; Test specific conversions
  (pg/validate-type-conversion "string" "int")
  (pg/validate-type-conversion "avatar" "json")
  (pg/validate-type-conversion "user" "group")

  ;; Test that errors are thrown
  (try
    (pg/check-type-conversion! test-entity test-attribute "boolean" "int")
    (catch Exception e
      (println "Error:" (.getMessage e))
      (println "Data:" (ex-data e)))))
