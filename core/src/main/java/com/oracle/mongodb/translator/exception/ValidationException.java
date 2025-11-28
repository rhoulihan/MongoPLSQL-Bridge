/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.exception;

import java.util.List;
import java.util.stream.Collectors;

/** Thrown when pipeline validation fails. */
public class ValidationException extends TranslationException {

  private static final long serialVersionUID = 1L;

  private final transient List<ValidationError> errors;

  public ValidationException(List<ValidationError> errors) {
    super(buildMessage(errors));
    this.errors = List.copyOf(errors);
  }

  private static String buildMessage(List<ValidationError> errors) {
    return "Validation failed: "
        + errors.stream().map(ValidationError::message).collect(Collectors.joining("; "));
  }

  public List<ValidationError> getErrors() {
    return errors;
  }
}
