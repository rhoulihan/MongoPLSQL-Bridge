/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.api;

import java.util.Objects;

/** Represents a warning generated during translation. */
public final class TranslationWarning {

  private final String code;
  private final String message;

  private TranslationWarning(String code, String message) {
    this.code = Objects.requireNonNull(code, "code must not be null");
    this.message = Objects.requireNonNull(message, "message must not be null");
  }

  /** Creates a translation warning. */
  public static TranslationWarning of(String code, String message) {
    return new TranslationWarning(code, message);
  }

  /** Returns the warning code. */
  public String code() {
    return code;
  }

  /** Returns the warning message. */
  public String message() {
    return message;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    TranslationWarning that = (TranslationWarning) obj;
    return Objects.equals(code, that.code) && Objects.equals(message, that.message);
  }

  @Override
  public int hashCode() {
    return Objects.hash(code, message);
  }

  @Override
  public String toString() {
    return "[" + code + "] " + message;
  }
}
