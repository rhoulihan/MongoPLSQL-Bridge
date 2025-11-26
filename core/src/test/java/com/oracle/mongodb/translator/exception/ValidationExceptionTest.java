/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */
package com.oracle.mongodb.translator.exception;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;

class ValidationExceptionTest {

    @Test
    void shouldContainValidationErrors() {
        var errors = List.of(
            new ValidationError("MISSING_FIELD", "_id field required in $group"),
            new ValidationError("INVALID_TYPE", "Expected object, got array")
        );

        var exception = new ValidationException(errors);

        assertThat(exception.getErrors()).hasSize(2);
        assertThat(exception.getMessage()).contains("_id field required");
    }

    @Test
    void shouldReturnImmutableErrorList() {
        var errors = List.of(
            new ValidationError("TEST", "test error")
        );

        var exception = new ValidationException(errors);

        assertThat(exception.getErrors()).isUnmodifiable();
    }
}
