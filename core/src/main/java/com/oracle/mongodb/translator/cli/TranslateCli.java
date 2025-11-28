package com.oracle.mongodb.translator.cli;

import com.oracle.mongodb.translator.api.AggregationTranslator;
import com.oracle.mongodb.translator.api.OracleConfiguration;
import com.oracle.mongodb.translator.api.TranslationOptions;
import com.oracle.mongodb.translator.api.TranslationResult;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.bson.Document;

/**
 * Command-line interface for translating MongoDB aggregation pipelines to Oracle SQL.
 *
 * <p>Usage: java -cp core.jar com.oracle.mongodb.translator.cli.TranslateCli &lt;collection&gt;
 * &lt;pipeline-json&gt;
 *
 * <p>Or: java -cp core.jar com.oracle.mongodb.translator.cli.TranslateCli &lt;collection&gt; --file
 * &lt;pipeline-file&gt;
 *
 * <p>Options:
 *
 * <ul>
 *   <li>--inline: Inline bind variable values directly in SQL (default: use bind variables)
 * </ul>
 */
public final class TranslateCli {

  private TranslateCli() {
    // Utility class
  }

  /**
   * Main entry point.
   *
   * @param args command line arguments
   */
  public static void main(String[] args) {
    if (args.length < 2) {
      System.err.println("Usage: TranslateCli <collection> <pipeline-json>");
      System.err.println("   or: TranslateCli <collection> --file <pipeline-file> [--inline]");
      System.err.println("Options:");
      System.err.println("   --inline    Inline bind variable values directly in SQL");
      System.exit(1);
    }

    String collection = args[0];
    String pipelineJson = null;
    boolean inlineValues = false;

    // Parse arguments
    List<String> positionalArgs = new ArrayList<>();
    for (int i = 1; i < args.length; i++) {
      if ("--inline".equals(args[i])) {
        inlineValues = true;
      } else if ("--file".equals(args[i]) && i + 1 < args.length) {
        try {
          pipelineJson = Files.readString(Path.of(args[++i]));
        } catch (IOException e) {
          System.err.println("Error reading file: " + e.getMessage());
          System.exit(1);
          return;
        }
      } else {
        positionalArgs.add(args[i]);
      }
    }

    // If no --file was used, treat first positional arg as pipeline JSON
    if (pipelineJson == null && !positionalArgs.isEmpty()) {
      pipelineJson = positionalArgs.get(0);
    }

    if (pipelineJson == null) {
      System.err.println("Error: No pipeline provided");
      System.exit(1);
      return;
    }

    try {
      // Parse the pipeline JSON
      List<Document> pipeline = parsePipeline(pipelineJson);

      // Create translator with options
      OracleConfiguration config = OracleConfiguration.builder().collectionName(collection).build();

      TranslationOptions options =
          TranslationOptions.builder().inlineBindVariables(inlineValues).build();

      AggregationTranslator translator = AggregationTranslator.create(config, options);

      // Translate
      TranslationResult result = translator.translate(pipeline);

      // Output the SQL
      System.out.println(result.sql());

    } catch (Exception e) {
      System.err.println("Translation error: " + e.getMessage());
      System.exit(1);
    }
  }

  @SuppressWarnings("unchecked")
  private static List<Document> parsePipeline(String json) {
    // Handle both array format and object format
    String trimmed = json.trim();
    if (trimmed.startsWith("[")) {
      // Direct array format
      return Document.parse("{\"pipeline\":" + trimmed + "}").getList("pipeline", Document.class);
    } else if (trimmed.startsWith("{")) {
      // Could be a single stage or an object containing pipeline
      Document doc = Document.parse(trimmed);
      if (doc.containsKey("pipeline")) {
        Object pipelineObj = doc.get("pipeline");
        if (pipelineObj instanceof List) {
          return ((List<?>) pipelineObj)
              .stream()
                  .map(
                      item ->
                          item instanceof Document
                              ? (Document) item
                              : Document.parse(item.toString()))
                  .collect(Collectors.toList());
        }
      }
      // Single stage
      return List.of(doc);
    }
    throw new IllegalArgumentException("Invalid pipeline format");
  }
}
