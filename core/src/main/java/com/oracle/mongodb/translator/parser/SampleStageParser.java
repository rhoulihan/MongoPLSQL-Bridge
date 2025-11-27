/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */
package com.oracle.mongodb.translator.parser;

import com.oracle.mongodb.translator.ast.stage.SampleStage;
import org.bson.Document;

/**
 * Parser for the $sample pipeline stage.
 */
public final class SampleStageParser implements StageParser<SampleStage> {

    @Override
    public SampleStage parse(Object stageValue) {
        if (stageValue == null) {
            throw new IllegalArgumentException("$sample requires a document with a 'size' field, got: null");
        }
        if (!(stageValue instanceof Document doc)) {
            throw new IllegalArgumentException(
                "$sample requires a document with a 'size' field, got: " + stageValue.getClass().getSimpleName());
        }

        Object sizeValue = doc.get("size");
        if (sizeValue == null) {
            throw new IllegalArgumentException("$sample requires a 'size' field");
        }

        int size;
        if (sizeValue instanceof Number num) {
            size = num.intValue();
        } else {
            throw new IllegalArgumentException(
                "$sample 'size' must be a number, got: " + sizeValue.getClass().getSimpleName());
        }

        if (size <= 0) {
            throw new IllegalArgumentException("$sample 'size' must be positive, got: " + size);
        }

        return new SampleStage(size);
    }
}
