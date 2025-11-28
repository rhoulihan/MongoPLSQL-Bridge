/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.oracle.mongodb.translator.exception.ValidationException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class FieldNameValidatorTest {

  // ==================== Field Name Validation ====================

  @ParameterizedTest
  @ValueSource(
      strings = {
        "name",
        "firstName",
        "first_name",
        "_id",
        "_private",
        "field123",
        "nested.field",
        "deeply.nested.field.path",
        "a",
        "A",
        "_"
      })
  void shouldAcceptValidFieldNames(String fieldName) {
    assertThat(FieldNameValidator.validateFieldName(fieldName)).isEqualTo(fieldName);
    assertThat(FieldNameValidator.isValidFieldName(fieldName)).isTrue();
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "123field", // starts with number
        "-field", // starts with hyphen
        "field-name", // contains hyphen
        "field name", // contains space
        "field'name", // contains quote (SQL injection attempt)
        "field\"name", // contains double quote
        "field;DROP TABLE", // SQL injection attempt
        "field\nname", // contains newline
        "field\tname", // contains tab
        "$field", // starts with $ (should be normalized first)
        ".field", // starts with dot
        "field..name", // consecutive dots
      })
  void shouldRejectInvalidFieldNames(String fieldName) {
    assertThatThrownBy(() -> FieldNameValidator.validateFieldName(fieldName))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Invalid field name");
    assertThat(FieldNameValidator.isValidFieldName(fieldName)).isFalse();
  }

  @Test
  void shouldRejectNullFieldName() {
    assertThatThrownBy(() -> FieldNameValidator.validateFieldName(null))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("null or empty");
    assertThat(FieldNameValidator.isValidFieldName(null)).isFalse();
  }

  @Test
  void shouldRejectEmptyFieldName() {
    assertThatThrownBy(() -> FieldNameValidator.validateFieldName(""))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("null or empty");
    assertThat(FieldNameValidator.isValidFieldName("")).isFalse();
  }

  @Test
  void shouldRejectFieldNameExceedingMaxLength() {
    String longName = "a".repeat(129);
    assertThatThrownBy(() -> FieldNameValidator.validateFieldName(longName))
        .isInstanceOf(ValidationException.class);
  }

  @Test
  void shouldAcceptFieldNameAtMaxLength() {
    String maxLengthName = "a".repeat(128);
    assertThat(FieldNameValidator.validateFieldName(maxLengthName)).isEqualTo(maxLengthName);
  }

  // ==================== Table Name Validation ====================

  @ParameterizedTest
  @ValueSource(
      strings = {
        "orders",
        "Orders",
        "ORDER_ITEMS",
        "table123",
        "a",
        "A",
      })
  void shouldAcceptValidTableNames(String tableName) {
    assertThat(FieldNameValidator.validateTableName(tableName)).isEqualTo(tableName);
    assertThat(FieldNameValidator.isValidTableName(tableName)).isTrue();
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "_table", // starts with underscore (not allowed for tables)
        "123table", // starts with number
        "table.name", // contains dot (not allowed for tables)
        "table-name", // contains hyphen
        "table name", // contains space
        "table'name", // SQL injection attempt
        "table;DROP", // SQL injection attempt
      })
  void shouldRejectInvalidTableNames(String tableName) {
    assertThatThrownBy(() -> FieldNameValidator.validateTableName(tableName))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Invalid table name");
    assertThat(FieldNameValidator.isValidTableName(tableName)).isFalse();
  }

  @Test
  void shouldRejectNullTableName() {
    assertThatThrownBy(() -> FieldNameValidator.validateTableName(null))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("null or empty");
  }

  // ==================== Field Path Normalization ====================

  @Test
  void shouldNormalizeFieldPathWithDollarSign() {
    assertThat(FieldNameValidator.validateAndNormalizeFieldPath("$status")).isEqualTo("status");
  }

  @Test
  void shouldNormalizeFieldPathWithDollarAndDot() {
    assertThat(FieldNameValidator.validateAndNormalizeFieldPath("$.status")).isEqualTo("status");
  }

  @Test
  void shouldNormalizeNestedFieldPath() {
    assertThat(FieldNameValidator.validateAndNormalizeFieldPath("$customer.address.city"))
        .isEqualTo("customer.address.city");
  }

  @Test
  void shouldValidateFieldPathWithoutPrefix() {
    assertThat(FieldNameValidator.validateAndNormalizeFieldPath("status")).isEqualTo("status");
  }

  @Test
  void shouldRejectInvalidFieldPathAfterNormalization() {
    assertThatThrownBy(() -> FieldNameValidator.validateAndNormalizeFieldPath("$123invalid"))
        .isInstanceOf(ValidationException.class);
  }

  // ==================== SQL Injection Prevention ====================

  @Test
  void shouldPreventSqlInjectionInFieldName() {
    assertThatThrownBy(
            () -> FieldNameValidator.validateFieldName("field' OR '1'='1' --"))
        .isInstanceOf(ValidationException.class);
  }

  @Test
  void shouldPreventJsonPathInjection() {
    assertThatThrownBy(() -> FieldNameValidator.validateFieldName("field')]/**/OR/**/1=1--"))
        .isInstanceOf(ValidationException.class);
  }

  @Test
  void shouldPreventUnionInjection() {
    assertThatThrownBy(
            () -> FieldNameValidator.validateTableName("orders UNION SELECT * FROM users"))
        .isInstanceOf(ValidationException.class);
  }

  // ==================== File Path Validation ====================

  @ParameterizedTest
  @ValueSource(
      strings = {
        "pipeline.json",
        "/home/user/pipeline.json",
        "data/pipelines/test.json",
        "C:\\Users\\test\\pipeline.json",
        "./pipeline.json",
        "relative/path/file.txt"
      })
  void shouldAcceptValidFilePaths(String filePath) {
    assertThat(FieldNameValidator.validateFilePath(filePath)).isEqualTo(filePath);
    assertThat(FieldNameValidator.isValidFilePath(filePath)).isTrue();
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "../etc/passwd", // path traversal
        "../../secret.json", // path traversal
        "data/../../../etc/passwd", // embedded path traversal
        "..\\windows\\system32", // Windows path traversal
      })
  void shouldRejectPathTraversalAttempts(String filePath) {
    assertThatThrownBy(() -> FieldNameValidator.validateFilePath(filePath))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Path traversal");
    assertThat(FieldNameValidator.isValidFilePath(filePath)).isFalse();
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "file:///etc/passwd",
        "FILE:///etc/passwd",
        "http://evil.com/payload",
        "https://evil.com/payload",
        "ftp://evil.com/file",
      })
  void shouldRejectUrlStylePaths(String filePath) {
    assertThatThrownBy(() -> FieldNameValidator.validateFilePath(filePath))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("URL-style paths are not allowed");
    assertThat(FieldNameValidator.isValidFilePath(filePath)).isFalse();
  }

  @Test
  void shouldRejectNullFilePath() {
    assertThatThrownBy(() -> FieldNameValidator.validateFilePath(null))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("null or empty");
    assertThat(FieldNameValidator.isValidFilePath(null)).isFalse();
  }

  @Test
  void shouldRejectEmptyFilePath() {
    assertThatThrownBy(() -> FieldNameValidator.validateFilePath(""))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("null or empty");
    assertThat(FieldNameValidator.isValidFilePath("")).isFalse();
  }

  @Test
  void shouldRejectPathWithNullBytes() {
    assertThatThrownBy(() -> FieldNameValidator.validateFilePath("file.json\0.txt"))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("null bytes");
    assertThat(FieldNameValidator.isValidFilePath("file.json\0.txt")).isFalse();
  }
}
