/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.api;

import java.util.List;
import org.bson.Document;

/** Main entry point for translating MongoDB aggregation pipelines to Oracle SQL. */
public interface AggregationTranslator {

  /** Creates a new translator with the given configuration. */
  static AggregationTranslator create(OracleConfiguration config) {
    return new DefaultAggregationTranslator(config);
  }

  /** Creates a new translator with the given configuration and options. */
  static AggregationTranslator create(OracleConfiguration config, TranslationOptions options) {
    return new DefaultAggregationTranslator(config, options);
  }

  /**
   * Translates a MongoDB aggregation pipeline to Oracle SQL.
   *
   * @param pipeline list of pipeline stage documents
   * @return the translation result containing SQL and bind variables
   */
  TranslationResult translate(List<Document> pipeline);

  /** Returns the Oracle configuration. */
  OracleConfiguration getConfiguration();

  /** Returns the translation options. */
  TranslationOptions getOptions();
}
