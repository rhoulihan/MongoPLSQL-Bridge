/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */
package com.oracle.mongodb.translator.exception;

/**
 * Thrown when an unsupported MongoDB operator is encountered.
 */
public class UnsupportedOperatorException extends TranslationException {

    private static final long serialVersionUID = 1L;

    private final String operatorName;
    private final boolean partiallySupported;

    public UnsupportedOperatorException(String operatorName) {
        this(operatorName, false);
    }

    public UnsupportedOperatorException(String operatorName, boolean partiallySupported) {
        super(buildMessage(operatorName, partiallySupported));
        this.operatorName = operatorName;
        this.partiallySupported = partiallySupported;
    }

    private static String buildMessage(String operatorName, boolean partiallySupported) {
        if (partiallySupported) {
            return "Operator " + operatorName + " is only partially supported";
        }
        return "Operator " + operatorName + " is not supported";
    }

    public String getOperatorName() {
        return operatorName;
    }

    public boolean isPartiallySupported() {
        return partiallySupported;
    }
}
