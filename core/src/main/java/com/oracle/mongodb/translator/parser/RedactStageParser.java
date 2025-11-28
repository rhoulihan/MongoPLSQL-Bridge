/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.parser;

import com.oracle.mongodb.translator.ast.expression.Expression;
import com.oracle.mongodb.translator.ast.stage.RedactStage;
import org.bson.Document;

/** Parser for the $redact pipeline stage. */
public final class RedactStageParser implements StageParser<RedactStage> {

  private final ExpressionParser expressionParser = new ExpressionParser();

  @Override
  public RedactStage parse(Object stageValue) {
    // $redact takes an expression that evaluates to $$DESCEND, $$PRUNE, or $$KEEP
    // The expression is usually a $cond
    Expression expr;
    if (stageValue instanceof Document doc) {
      expr = expressionParser.parseValue(doc);
    } else if (stageValue instanceof String str) {
      // Direct system variable like "$$DESCEND"
      expr = expressionParser.parseValue(str);
    } else {
      throw new IllegalArgumentException(
          "$redact requires an expression document or system variable string, got: "
              + (stageValue == null ? "null" : stageValue.getClass().getSimpleName()));
    }

    return new RedactStage(expr);
  }
}
