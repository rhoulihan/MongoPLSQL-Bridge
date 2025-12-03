/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.parser;

import com.oracle.mongodb.translator.ast.expression.Expression;
import com.oracle.mongodb.translator.ast.stage.ReplaceRootStage;
import org.bson.Document;

/**
 * Parser for $replaceRoot stage.
 *
 * <p>Parses:
 *
 * <ul>
 *   <li>{@code {$replaceRoot: {newRoot: "$subdocument"}}}
 *   <li>{@code {$replaceRoot: {newRoot: {$mergeObjects: [...]}}}}
 * </ul>
 */
public final class ReplaceRootStageParser {

  private final ExpressionParser expressionParser = new ExpressionParser();

  /**
   * Parses a $replaceRoot stage document.
   *
   * @param value the stage value (document with newRoot field)
   * @return the parsed ReplaceRootStage
   */
  public ReplaceRootStage parse(Object value) {
    if (!(value instanceof Document doc)) {
      throw new IllegalArgumentException("$replaceRoot requires a document");
    }

    Object newRootValue = doc.get("newRoot");
    if (newRootValue == null) {
      throw new IllegalArgumentException("$replaceRoot requires a 'newRoot' field");
    }

    Expression newRoot = expressionParser.parseValue(newRootValue);
    return new ReplaceRootStage(newRoot);
  }
}
