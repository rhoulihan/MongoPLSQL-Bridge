/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.util;

import com.oracle.mongodb.translator.exception.ValidationError;
import com.oracle.mongodb.translator.exception.ValidationException;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Utility class for validating and sanitizing field names used in JSON paths and SQL identifiers.
 *
 * <p>This class provides security-critical validation to prevent SQL injection and JSON path
 * injection attacks by ensuring field names contain only safe characters.
 */
public final class FieldNameValidator {

  /** Maximum length for Oracle identifiers. */
  private static final int MAX_LENGTH = 128;

  /**
   * Pattern for a single path segment (no dots). Uses simple non-repeating pattern to avoid ReDoS.
   * Must start with letter or underscore, followed by alphanumeric and underscores.
   */
  private static final Pattern SEGMENT_PATTERN = Pattern.compile("^[a-zA-Z_][a-zA-Z0-9_]*$");

  /**
   * Pattern for safe collection/table names. More restrictive than field names:
   *
   * <ul>
   *   <li>Must start with letter
   *   <li>Can contain letters, digits, and underscores only
   *   <li>No dots allowed (not a path)
   * </ul>
   */
  private static final Pattern SAFE_TABLE_PATTERN =
      Pattern.compile("^[a-zA-Z][a-zA-Z0-9_]{0,127}$");

  private FieldNameValidator() {
    // Utility class - prevent instantiation
  }

  /**
   * Validates a field name for use in JSON paths.
   *
   * @param fieldName the field name to validate
   * @return the validated field name (unchanged if valid)
   * @throws ValidationException if the field name contains invalid characters
   */
  public static String validateFieldName(String fieldName) {
    if (fieldName == null || fieldName.isEmpty()) {
      throw new ValidationException(
          List.of(new ValidationError("INVALID_FIELD", "Field name cannot be null or empty")));
    }

    if (fieldName.length() > MAX_LENGTH || !isValidFieldNameInternal(fieldName)) {
      throw new ValidationException(
          List.of(
              new ValidationError(
                  "INVALID_FIELD",
                  "Invalid field name '"
                      + sanitizeForMessage(fieldName)
                      + "': must start with letter or underscore, "
                      + "contain only alphanumeric characters, underscores, and dots, "
                      + "and be at most 128 characters")));
    }

    return fieldName;
  }

  /**
   * Internal validation that splits on dots and validates each segment. This approach avoids
   * complex repeating patterns that could be vulnerable to ReDoS.
   */
  private static boolean isValidFieldNameInternal(String fieldName) {
    // Split on dots and validate each segment
    String[] segments = fieldName.split("\\.", -1);

    // Check for empty segments (consecutive dots, leading/trailing dots)
    for (String segment : segments) {
      if (segment.isEmpty() || !SEGMENT_PATTERN.matcher(segment).matches()) {
        return false;
      }
    }
    return true;
  }

  /**
   * Validates a collection/table name for use in SQL.
   *
   * @param tableName the table name to validate
   * @return the validated table name (unchanged if valid)
   * @throws ValidationException if the table name contains invalid characters
   */
  public static String validateTableName(String tableName) {
    if (tableName == null || tableName.isEmpty()) {
      throw new ValidationException(
          List.of(new ValidationError("INVALID_TABLE", "Table name cannot be null or empty")));
    }

    if (!SAFE_TABLE_PATTERN.matcher(tableName).matches()) {
      throw new ValidationException(
          List.of(
              new ValidationError(
                  "INVALID_TABLE",
                  "Invalid table name '"
                      + sanitizeForMessage(tableName)
                      + "': must start with letter, "
                      + "contain only alphanumeric characters and underscores, "
                      + "and be at most 128 characters")));
    }

    return tableName;
  }

  /**
   * Validates and normalizes a field path (removes leading $ if present).
   *
   * @param fieldPath the field path to validate (may start with $)
   * @return the normalized field path without leading $
   * @throws ValidationException if the field path contains invalid characters
   */
  public static String validateAndNormalizeFieldPath(String fieldPath) {
    if (fieldPath == null || fieldPath.isEmpty()) {
      throw new ValidationException(
          List.of(new ValidationError("INVALID_FIELD", "Field path cannot be null or empty")));
    }

    String normalized = fieldPath;
    if (normalized.startsWith("$")) {
      normalized = normalized.substring(1);
    }
    if (normalized.startsWith(".")) {
      normalized = normalized.substring(1);
    }

    return validateFieldName(normalized);
  }

  /**
   * Checks if a field name is valid without throwing an exception.
   *
   * @param fieldName the field name to check
   * @return true if valid, false otherwise
   */
  public static boolean isValidFieldName(String fieldName) {
    return fieldName != null
        && !fieldName.isEmpty()
        && fieldName.length() <= MAX_LENGTH
        && isValidFieldNameInternal(fieldName);
  }

  /**
   * Checks if a table name is valid without throwing an exception.
   *
   * @param tableName the table name to check
   * @return true if valid, false otherwise
   */
  public static boolean isValidTableName(String tableName) {
    return tableName != null
        && !tableName.isEmpty()
        && SAFE_TABLE_PATTERN.matcher(tableName).matches();
  }

  /**
   * Sanitizes a string for inclusion in error messages to prevent log injection.
   *
   * @param input the input string
   * @return sanitized string safe for logging
   */
  private static String sanitizeForMessage(String input) {
    if (input == null) {
      return "null";
    }
    // Truncate long strings and remove control characters
    String truncated = input.length() > 50 ? input.substring(0, 50) + "..." : input;
    return truncated.replaceAll("[\\x00-\\x1F\\x7F]", "?");
  }

  /**
   * Validates a file path for CLI input to prevent path traversal attacks.
   *
   * <p>This method checks that:
   *
   * <ul>
   *   <li>The path is not null or empty
   *   <li>The path does not contain path traversal sequences (..)
   *   <li>The path does not contain null bytes
   *   <li>The path is not a URL-style path
   * </ul>
   *
   * @param filePath the file path to validate
   * @return the validated path
   * @throws ValidationException if the path is invalid or potentially malicious
   */
  public static String validateFilePath(String filePath) {
    if (filePath == null || filePath.isEmpty()) {
      throw new ValidationException(
          List.of(new ValidationError("INVALID_PATH", "File path cannot be null or empty")));
    }

    // Check for path traversal attempts
    if (filePath.contains("..")) {
      throw new ValidationException(
          List.of(
              new ValidationError(
                  "PATH_TRAVERSAL",
                  "Path traversal detected in '"
                      + sanitizeForMessage(filePath)
                      + "': '..' sequences are not allowed")));
    }

    // Check for null bytes (path truncation attack)
    if (filePath.contains("\0")) {
      throw new ValidationException(
          List.of(
              new ValidationError(
                  "INVALID_PATH", "Invalid characters in path: null bytes are not allowed")));
    }

    // Check for suspicious protocol prefixes
    String lowerPath = filePath.toLowerCase();
    if (lowerPath.startsWith("file:")
        || lowerPath.startsWith("http:")
        || lowerPath.startsWith("https:")
        || lowerPath.startsWith("ftp:")) {
      throw new ValidationException(
          List.of(
              new ValidationError(
                  "INVALID_PATH",
                  "URL-style paths are not allowed: " + sanitizeForMessage(filePath))));
    }

    return filePath;
  }

  /**
   * Checks if a file path is valid without throwing an exception.
   *
   * @param filePath the file path to check
   * @return true if valid, false otherwise
   */
  public static boolean isValidFilePath(String filePath) {
    if (filePath == null || filePath.isEmpty()) {
      return false;
    }
    if (filePath.contains("..") || filePath.contains("\0")) {
      return false;
    }
    String lowerPath = filePath.toLowerCase();
    return !lowerPath.startsWith("file:")
        && !lowerPath.startsWith("http:")
        && !lowerPath.startsWith("https:")
        && !lowerPath.startsWith("ftp:");
  }
}
