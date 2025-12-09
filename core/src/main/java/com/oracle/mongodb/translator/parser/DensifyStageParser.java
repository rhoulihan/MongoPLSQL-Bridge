/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.parser;

import com.oracle.mongodb.translator.ast.stage.DensifyStage;
import java.util.List;
import org.bson.Document;

/**
 * Parser for the $densify pipeline stage.
 *
 * <p>MongoDB $densify fills gaps in a sequence of values.
 *
 * <p>Example:
 *
 * <pre>
 * {
 *   $densify: {
 *     field: "timestamp",
 *     partitionByFields: ["region"],
 *     range: {
 *       step: 1,
 *       unit: "hour",
 *       bounds: "full"
 *     }
 *   }
 * }
 * </pre>
 */
public final class DensifyStageParser implements StageParser<DensifyStage> {

  @Override
  @SuppressWarnings("unchecked")
  public DensifyStage parse(Object stageValue) {
    if (stageValue == null) {
      throw new IllegalArgumentException(
          "$densify requires a document with 'field' and 'range' fields, got: null");
    }
    if (!(stageValue instanceof Document doc)) {
      throw new IllegalArgumentException(
          "$densify requires a document, got: " + stageValue.getClass().getSimpleName());
    }

    // Parse required field
    String field = doc.getString("field");
    if (field == null || field.isEmpty()) {
      throw new IllegalArgumentException("$densify requires a 'field' field");
    }

    // Parse optional partitionByFields
    List<String> partitionByFields = null;
    Object partitionBy = doc.get("partitionByFields");
    if (partitionBy instanceof List<?> list) {
      partitionByFields = (List<String>) list;
    }

    // Parse required range
    Object rangeObj = doc.get("range");
    if (rangeObj == null) {
      throw new IllegalArgumentException("$densify requires a 'range' field");
    }
    if (!(rangeObj instanceof Document rangeDoc)) {
      throw new IllegalArgumentException(
          "$densify 'range' must be a document, got: " + rangeObj.getClass().getSimpleName());
    }

    DensifyStage.RangeSpec range = parseRange(rangeDoc);

    return new DensifyStage(field, partitionByFields, range);
  }

  private DensifyStage.RangeSpec parseRange(Document rangeDoc) {
    // Parse required step
    Object stepObj = rangeDoc.get("step");
    if (stepObj == null) {
      throw new IllegalArgumentException("$densify 'range' requires a 'step' field");
    }
    int step;
    if (stepObj instanceof Number num) {
      step = num.intValue();
    } else {
      throw new IllegalArgumentException(
          "$densify 'step' must be a number, got: " + stepObj.getClass().getSimpleName());
    }

    // Parse optional unit
    String unit = rangeDoc.getString("unit");

    // Parse optional bounds
    String bounds = rangeDoc.getString("bounds");

    return new DensifyStage.RangeSpec(step, unit, bounds);
  }
}
