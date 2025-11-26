/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */
package com.oracle.mongodb.translator.api;

import com.oracle.mongodb.translator.ast.stage.Pipeline;
import com.oracle.mongodb.translator.generator.DefaultSqlGenerationContext;
import com.oracle.mongodb.translator.parser.PipelineParser;
import org.bson.Document;
import java.util.List;
import java.util.Objects;

/**
 * Default implementation of AggregationTranslator.
 */
final class DefaultAggregationTranslator implements AggregationTranslator {

    private final OracleConfiguration config;
    private final TranslationOptions options;
    private final PipelineParser pipelineParser;

    DefaultAggregationTranslator(OracleConfiguration config) {
        this(config, TranslationOptions.defaults());
    }

    DefaultAggregationTranslator(OracleConfiguration config, TranslationOptions options) {
        this.config = Objects.requireNonNull(config, "config must not be null");
        this.options = Objects.requireNonNull(options, "options must not be null");
        this.pipelineParser = new PipelineParser();
    }

    @Override
    public TranslationResult translate(List<Document> pipeline) {
        Objects.requireNonNull(pipeline, "pipeline must not be null");

        // Parse pipeline documents into AST
        Pipeline pipelineAst = pipelineParser.parse(config.collectionName(), pipeline);

        // Generate SQL
        var context = new DefaultSqlGenerationContext(options.inlineBindVariables());

        // Build SELECT clause
        context.sql("SELECT ");
        context.sql(config.dataColumnName());
        context.sql(" FROM ");
        context.sql(config.qualifiedTableName());

        // Render stages
        if (!pipelineAst.isEmpty()) {
            context.sql(" ");
            pipelineAst.render(context);
        }

        return TranslationResult.of(
            context.toSql(),
            context.getBindVariables()
        );
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
