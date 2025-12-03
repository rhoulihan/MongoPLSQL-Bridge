/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.cli;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TranslatorCliTest {

  @TempDir Path tempDir;

  private ByteArrayOutputStream outStream;
  private ByteArrayOutputStream errStream;
  private TranslatorCli cli;

  @BeforeEach
  void setUp() {
    outStream = new ByteArrayOutputStream();
    errStream = new ByteArrayOutputStream();
    cli = new TranslatorCli(new PrintStream(outStream), new PrintStream(errStream));
  }

  @Test
  void shouldShowHelpWithHelpFlag() {
    int exitCode = cli.run(new String[] {"--help"});

    assertThat(exitCode).isZero();
    assertThat(outStream.toString()).contains("MongoDB to Oracle SQL Translator");
    assertThat(outStream.toString()).contains("Usage:");
    assertThat(outStream.toString()).contains("--collection");
  }

  @Test
  void shouldShowVersionWithVersionFlag() {
    int exitCode = cli.run(new String[] {"--version"});

    assertThat(exitCode).isZero();
    assertThat(outStream.toString()).contains("mongo-oracle-translator");
  }

  @Test
  void shouldFailWithNoInputFile() {
    int exitCode = cli.run(new String[] {});

    assertThat(exitCode).isEqualTo(1);
    assertThat(errStream.toString()).contains("No input file specified");
  }

  @Test
  void shouldFailWithUnknownOption() {
    int exitCode = cli.run(new String[] {"--unknown"});

    assertThat(exitCode).isEqualTo(1);
    assertThat(errStream.toString()).contains("Unknown option");
  }

  @Test
  void shouldTranslateSinglePipelineArray() throws IOException {
    Path inputFile = tempDir.resolve("pipeline.json");
    Files.writeString(
        inputFile,
        """
        [
          {"$match": {"status": "active"}},
          {"$limit": 10}
        ]
        """);

    int exitCode = cli.run(new String[] {"--collection", "orders", inputFile.toString()});

    assertThat(exitCode).isZero();
    String output = outStream.toString();
    assertThat(output).contains("SELECT");
    assertThat(output).contains("orders");
  }

  @Test
  void shouldTranslatePipelineWithMetadata() throws IOException {
    Path inputFile = tempDir.resolve("pipeline.json");
    Files.writeString(
        inputFile,
        """
        {
          "name": "Active Orders",
          "collection": "orders",
          "pipeline": [
            {"$match": {"status": "active"}},
            {"$limit": 10}
          ]
        }
        """);

    int exitCode = cli.run(new String[] {inputFile.toString()});

    assertThat(exitCode).isZero();
    String output = outStream.toString();
    assertThat(output).contains("SELECT");
    assertThat(output).contains("orders");
  }

  @Test
  void shouldTranslateMultiplePipelines() throws IOException {
    Path inputFile = tempDir.resolve("pipelines.json");
    Files.writeString(
        inputFile,
        """
        {
          "pipelines": [
            {
              "name": "Pipeline 1",
              "collection": "orders",
              "pipeline": [{"$match": {"status": "active"}}]
            },
            {
              "name": "Pipeline 2",
              "collection": "products",
              "pipeline": [{"$limit": 5}]
            }
          ]
        }
        """);

    int exitCode = cli.run(new String[] {inputFile.toString()});

    assertThat(exitCode).isZero();
    String output = outStream.toString();
    assertThat(output).contains("-- Pipeline: Pipeline 1");
    assertThat(output).contains("-- Pipeline: Pipeline 2");
    assertThat(output).contains("orders");
    assertThat(output).contains("products");
  }

  @Test
  void shouldOverrideCollectionFromCommandLine() throws IOException {
    Path inputFile = tempDir.resolve("pipeline.json");
    Files.writeString(
        inputFile,
        """
        {
          "collection": "original",
          "pipeline": [{"$limit": 10}]
        }
        """);

    int exitCode = cli.run(new String[] {"--collection", "overridden", inputFile.toString()});

    assertThat(exitCode).isZero();
    String output = outStream.toString();
    assertThat(output).contains("overridden");
  }

  @Test
  void shouldOutputBindVariables() throws IOException {
    Path inputFile = tempDir.resolve("pipeline.json");
    Files.writeString(
        inputFile,
        """
        [
          {"$match": {"status": "active", "count": {"$gt": 5}}}
        ]
        """);

    int exitCode = cli.run(new String[] {"--collection", "test", inputFile.toString()});

    assertThat(exitCode).isZero();
    String output = outStream.toString();
    assertThat(output).contains("Bind variables");
    assertThat(output).contains(":1");
  }

  @Test
  void shouldInlineBindVariables() throws IOException {
    Path inputFile = tempDir.resolve("pipeline.json");
    Files.writeString(
        inputFile,
        """
        [
          {"$match": {"status": "active"}}
        ]
        """);

    int exitCode = cli.run(new String[] {"--collection", "test", "--inline", inputFile.toString()});

    assertThat(exitCode).isZero();
    String output = outStream.toString();
    assertThat(output).contains("'active'");
    assertThat(output).doesNotContain("Bind variables");
  }

  @Test
  void shouldWriteToOutputFile() throws IOException {
    Path inputFile = tempDir.resolve("pipeline.json");
    Path outputFile = tempDir.resolve("output.sql");
    Files.writeString(
        inputFile,
        """
        [{"$limit": 10}]
        """);

    int exitCode =
        cli.run(
            new String[] {
              "--collection", "test", "--output", outputFile.toString(), inputFile.toString()
            });

    assertThat(exitCode).isZero();
    assertThat(outStream.toString()).contains("Output written to");
    assertThat(Files.readString(outputFile)).contains("SELECT");
  }

  @Test
  void shouldFailOnInvalidJson() throws IOException {
    Path inputFile = tempDir.resolve("invalid.json");
    Files.writeString(inputFile, "not valid json");

    int exitCode = cli.run(new String[] {"--collection", "test", inputFile.toString()});

    assertThat(exitCode).isNotZero();
  }

  @Test
  void shouldFailOnNonexistentFile() {
    int exitCode = cli.run(new String[] {"nonexistent.json"});

    assertThat(exitCode).isEqualTo(2);
    assertThat(errStream.toString()).contains("Error reading input file");
  }

  @Test
  void shouldHandleComplexPipeline() throws IOException {
    Path inputFile = tempDir.resolve("complex.json");
    Files.writeString(
        inputFile,
        """
        {
          "name": "Revenue Analysis",
          "collection": "orders",
          "pipeline": [
            {"$match": {"status": {"$in": ["delivered", "shipped"]}}},
            {"$group": {
              "_id": "$category",
              "totalRevenue": {"$sum": "$amount"},
              "orderCount": {"$sum": 1}
            }},
            {"$sort": {"totalRevenue": -1}},
            {"$limit": 10}
          ]
        }
        """);

    int exitCode = cli.run(new String[] {inputFile.toString()});

    assertThat(exitCode).isZero();
    String output = outStream.toString();
    assertThat(output).contains("SELECT");
    assertThat(output).contains("GROUP BY");
    assertThat(output).contains("ORDER BY");
    assertThat(output).contains("FETCH FIRST");
  }

  @Test
  void shouldSupportShortOptions() throws IOException {
    Path inputFile = tempDir.resolve("pipeline.json");
    Files.writeString(inputFile, "[{\"$limit\": 10}]");

    int exitCode = cli.run(new String[] {"-c", "test", "-i", "-p", inputFile.toString()});

    assertThat(exitCode).isZero();
  }
}
