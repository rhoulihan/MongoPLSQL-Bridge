/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.parser;

import com.oracle.mongodb.translator.ast.stage.Stage;
import com.oracle.mongodb.translator.ast.stage.UnionWithStage;
import java.util.ArrayList;
import java.util.List;
import org.bson.Document;

/**
 * Parser for the MongoDB $unionWith stage.
 *
 * <p>Supports two forms:
 *
 * <pre>
 * // Simple form - just collection name
 * { $unionWith: "otherCollection" }
 *
 * // Document form - with optional pipeline
 * {
 *   $unionWith: {
 *     coll: "otherCollection",
 *     pipeline: [ ... ]
 *   }
 * }
 * </pre>
 */
public final class UnionWithStageParser {

  private final PipelineParser pipelineParser;

  public UnionWithStageParser() {
    this.pipelineParser = new PipelineParser();
  }

  public UnionWithStageParser(PipelineParser pipelineParser) {
    this.pipelineParser = pipelineParser;
  }

  /**
   * Parses a $unionWith stage from the given value.
   *
   * @param value the stage value (String or Document)
   * @return the parsed UnionWithStage
   * @throws IllegalArgumentException if the value is invalid
   */
  public UnionWithStage parse(Object value) {
    if (value instanceof String collectionName) {
      // Simple form: { $unionWith: "collectionName" }
      return new UnionWithStage(collectionName);
    }

    if (value instanceof Document doc) {
      // Document form: { $unionWith: { coll: "...", pipeline: [...] } }
      return parseDocumentForm(doc);
    }

    throw new IllegalArgumentException(
        "$unionWith requires a string or document, got: "
            + (value == null ? "null" : value.getClass().getSimpleName()));
  }

  private UnionWithStage parseDocumentForm(Document doc) {
    // Get required 'coll' field
    Object collValue = doc.get("coll");
    if (collValue == null) {
      throw new IllegalArgumentException("$unionWith document form requires 'coll' field");
    }
    if (!(collValue instanceof String)) {
      throw new IllegalArgumentException(
          "$unionWith 'coll' must be a string, got: " + collValue.getClass().getSimpleName());
    }
    String collection = (String) collValue;

    // Get optional 'pipeline' field
    List<Stage> pipeline = new ArrayList<>();
    Object pipelineValue = doc.get("pipeline");
    if (pipelineValue != null) {
      if (!(pipelineValue instanceof List)) {
        throw new IllegalArgumentException(
            "$unionWith 'pipeline' must be an array, got: "
                + pipelineValue.getClass().getSimpleName());
      }
      @SuppressWarnings("unchecked")
      List<Document> pipelineDocList = (List<Document>) pipelineValue;
      // Parse the pipeline using the nested collection name
      var parsedPipeline = pipelineParser.parse(collection, pipelineDocList);
      pipeline = parsedPipeline.getStages();
    }

    return new UnionWithStage(collection, pipeline);
  }
}
