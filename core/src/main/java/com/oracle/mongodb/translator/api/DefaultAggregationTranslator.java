/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.api;

import com.oracle.mongodb.translator.ast.stage.Pipeline;
import com.oracle.mongodb.translator.generator.DefaultSqlGenerationContext;
import com.oracle.mongodb.translator.generator.PipelineRenderer;
import com.oracle.mongodb.translator.parser.PipelineParser;
import java.util.List;
import java.util.Objects;
import org.bson.Document;

/** Default implementation of AggregationTranslator. */
final class DefaultAggregationTranslator implements AggregationTranslator {

  private final OracleConfiguration config;
  private final TranslationOptions options;
  private final PipelineParser pipelineParser;
  private final PipelineRenderer pipelineRenderer;

  DefaultAggregationTranslator(OracleConfiguration config) {
    this(config, TranslationOptions.defaults());
  }

  DefaultAggregationTranslator(OracleConfiguration config, TranslationOptions options) {
    this.config = Objects.requireNonNull(config, "config must not be null");
    this.options = Objects.requireNonNull(options, "options must not be null");
    this.pipelineParser = new PipelineParser();
    this.pipelineRenderer = new PipelineRenderer(config);
  }

  @Override
  public TranslationResult translate(List<Document> pipeline) {
    Objects.requireNonNull(pipeline, "pipeline must not be null");

    // Parse pipeline documents into AST
    Pipeline pipelineAst = pipelineParser.parse(config.collectionName(), pipeline);

    // Generate SQL using the pipeline renderer
    // Always use "base" alias to ensure consistent table references
    var context =
        new DefaultSqlGenerationContext(
            options.inlineBindVariables(),
            null, // default dialect
            "base" // base table alias
            );
    pipelineRenderer.render(pipelineAst, context);

    return TranslationResult.of(context.toSql(), context.getBindVariables());
  }

  @Override
  public OracleConfiguration getConfiguration() {
    return config;
  }

  @Override
  public TranslationOptions getOptions() {
    return options;
  }
}
