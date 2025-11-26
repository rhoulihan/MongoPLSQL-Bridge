/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */
package com.oracle.mongodb.translator.api;

/**
 * Indicates the level of support for translating a MongoDB pipeline to Oracle SQL.
 */
public enum TranslationCapability {
    /**
     * Direct translation to SQL with full functionality.
     */
    FULL_SUPPORT,

    /**
     * Translation requires a workaround using multiple SQL constructs.
     */
    EMULATED,

    /**
     * Some functionality is available with documented limitations.
     */
    PARTIAL,

    /**
     * Feature cannot be translated and must be processed in application code.
     */
    CLIENT_SIDE_ONLY,

    /**
     * Feature is not translatable and will throw an exception.
     */
    UNSUPPORTED
}
