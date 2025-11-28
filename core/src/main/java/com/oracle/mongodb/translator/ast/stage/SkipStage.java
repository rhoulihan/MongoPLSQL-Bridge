/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.ast.stage;

import com.oracle.mongodb.translator.generator.SqlGenerationContext;

/**
 * Represents a $skip stage that skips a number of documents. Translates to Oracle's OFFSET n ROWS
 * clause.
 */
public final class SkipStage implements Stage {

  private final int skip;

  /**
   * Creates a $skip stage.
   *
   * @param skip the number of documents to skip
   */
  public SkipStage(int skip) {
    if (skip < 0) {
      throw new IllegalArgumentException("Skip must be non-negative, got: " + skip);
    }
    this.skip = skip;
  }

  public int getSkip() {
    return skip;
  }

  @Override
  public String getOperatorName() {
    return "$skip";
  }

  @Override
  public void render(SqlGenerationContext ctx) {
    ctx.sql("OFFSET ");
    ctx.sql(String.valueOf(skip));
    ctx.sql(" ROWS");
  }

  @Override
  public String toString() {
    return "SkipStage(" + skip + ")";
  }
}
