/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.parser;

import com.oracle.mongodb.translator.ast.stage.CountStage;

/** Parser for the $count pipeline stage. */
public final class CountStageParser implements StageParser<CountStage> {

  @Override
  public CountStage parse(Object stageValue) {
    if (stageValue == null) {
      throw new IllegalArgumentException(
          "$count requires a non-empty string field name, got: null");
    }
    if (!(stageValue instanceof String fieldName)) {
      throw new IllegalArgumentException(
          "$count requires a string field name, got: " + stageValue.getClass().getSimpleName());
    }

    if (fieldName.isEmpty()) {
      throw new IllegalArgumentException("$count requires a non-empty string field name");
    }

    if (fieldName.startsWith("$")) {
      throw new IllegalArgumentException(
          "$count field name must not start with $, got: " + fieldName);
    }

    return new CountStage(fieldName);
  }
}
