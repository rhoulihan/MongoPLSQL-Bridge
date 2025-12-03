/*
 * Copyright (c) 2025 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0.
 */

package com.oracle.mongodb.translator.generator;

import com.oracle.mongodb.translator.api.AggregationTranslator;
import com.oracle.mongodb.translator.api.OracleConfiguration;
import com.oracle.mongodb.translator.api.TranslationOptions;
import com.oracle.mongodb.translator.api.TranslationResult;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import org.bson.Document;
import org.junit.jupiter.api.Test;

/**
 * Utility test to generate SQL for all pipelines. Run with: ./gradlew :core:test --tests
 * "*LargePipelineSqlGenerator*" --info
 */
public class LargePipelineSqlGenerator {

  @Test
  void generateSqlForAllPipelines() throws IOException {
    String json = Files.readString(Paths.get("../query-tests/large-scale/complex-pipelines.json"));
    Document root = Document.parse(json);
    List<Document> pipelines = root.getList("pipelines", Document.class);

    System.out.println("===BEGIN_LARGE_PIPELINE_SQL===");
    System.out.println("{");
    System.out.println("  \"pipelines\": [");

    for (int i = 0; i < pipelines.size(); i++) {
      Document p = pipelines.get(i);
      String id = p.getString("id");
      String collection = p.getString("collection");
      List<Document> rawPipeline = p.getList("pipeline", Document.class);

      OracleConfiguration config = OracleConfiguration.builder().collectionName(collection).build();
      TranslationOptions opts = TranslationOptions.builder().inlineBindVariables(true).build();
      AggregationTranslator translator = AggregationTranslator.create(config, opts);

      try {
        TranslationResult result = translator.translate(rawPipeline);
        String sql =
            result
                .sql()
                .replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n")
                .replace("\r", "");

        String comma = (i < pipelines.size() - 1) ? "," : "";
        System.out.println("    {\"id\": \"" + id + "\", \"sql\": \"" + sql + "\"}" + comma);
      } catch (Exception e) {
        String error =
            e.getMessage().replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", " ");
        String comma = (i < pipelines.size() - 1) ? "," : "";
        System.out.println("    {\"id\": \"" + id + "\", \"error\": \"" + error + "\"}" + comma);
      }
    }

    System.out.println("  ]");
    System.out.println("}");
    System.out.println("===END_LARGE_PIPELINE_SQL===");
  }

  @Test
  void generateSqlForUnitTests() throws IOException {
    String json = Files.readString(Paths.get("../query-tests/tests/test-cases.json"));
    Document root = Document.parse(json);
    List<Document> testCases = root.getList("test_cases", Document.class);

    System.out.println("===BEGIN_UNIT_TEST_SQL===");
    System.out.println("{");
    System.out.println("  \"test_cases\": [");

    for (int i = 0; i < testCases.size(); i++) {
      Document t = testCases.get(i);
      String id = t.getString("id");
      String collection = t.getString("collection");
      List<Document> rawPipeline = t.getList("mongodb_pipeline", Document.class);

      OracleConfiguration config = OracleConfiguration.builder().collectionName(collection).build();
      TranslationOptions opts = TranslationOptions.builder().inlineBindVariables(true).build();
      AggregationTranslator translator = AggregationTranslator.create(config, opts);

      try {
        TranslationResult result = translator.translate(rawPipeline);
        String sql =
            result
                .sql()
                .replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n")
                .replace("\r", "");

        String comma = (i < testCases.size() - 1) ? "," : "";
        System.out.println("    {\"id\": \"" + id + "\", \"sql\": \"" + sql + "\"}" + comma);
      } catch (Exception e) {
        String msg = e.getMessage() != null ? e.getMessage() : e.getClass().getName();
        String error = msg.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", " ");
        String comma = (i < testCases.size() - 1) ? "," : "";
        System.out.println("    {\"id\": \"" + id + "\", \"error\": \"" + error + "\"}" + comma);
      }
    }

    System.out.println("  ]");
    System.out.println("}");
    System.out.println("===END_UNIT_TEST_SQL===");
  }
}
