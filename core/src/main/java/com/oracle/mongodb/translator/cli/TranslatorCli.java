/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */

package com.oracle.mongodb.translator.cli;

import com.oracle.mongodb.translator.api.AggregationTranslator;
import com.oracle.mongodb.translator.api.OracleConfiguration;
import com.oracle.mongodb.translator.api.TranslationOptions;
import com.oracle.mongodb.translator.api.TranslationResult;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.bson.Document;

/**
 * Command-line interface for translating MongoDB aggregation pipelines to Oracle SQL.
 *
 * <p>Usage: java -jar translator.jar [options] &lt;input-file&gt;
 *
 * <p>Options:
 *
 * <ul>
 *   <li>--collection &lt;name&gt; : Collection/table name (default: from file or "collection")
 *   <li>--inline : Inline bind variables into SQL
 *   <li>--pretty : Pretty-print the SQL output
 *   <li>--no-hints : Disable optimizer hints
 *   <li>--output &lt;file&gt; : Write output to file instead of stdout
 *   <li>--help : Show this help message
 * </ul>
 */
public final class TranslatorCli {

  private static final String VERSION = "1.0.0";

  private final PrintStream out;
  private final PrintStream err;

  public TranslatorCli() {
    this(System.out, System.err);
  }

  @SuppressFBWarnings(
      value = "EI_EXPOSE_REP2",
      justification = "CLI intentionally shares output streams")
  public TranslatorCli(PrintStream out, PrintStream err) {
    this.out = out;
    this.err = err;
  }

  /** CLI entry point. */
  public static void main(String[] args) {
    TranslatorCli cli = new TranslatorCli();
    int exitCode = cli.run(args);
    System.exit(exitCode);
  }

  /**
   * Runs the CLI with the given arguments.
   *
   * @param args command line arguments
   * @return exit code (0 for success, non-zero for error)
   */
  public int run(String[] args) {
    CliOptions options;
    try {
      options = parseArgs(args);
    } catch (IllegalArgumentException e) {
      err.println("Error: " + e.getMessage());
      err.println("Use --help for usage information.");
      return 1;
    }

    if (options.showHelp) {
      printHelp();
      return 0;
    }

    if (options.showVersion) {
      out.println("mongo-oracle-translator " + VERSION);
      return 0;
    }

    if (options.inputFile == null) {
      err.println("Error: No input file specified.");
      err.println("Use --help for usage information.");
      return 1;
    }

    try {
      List<PipelineInput> pipelines = readPipelines(options.inputFile, options.collection);
      StringBuilder output = new StringBuilder();

      for (int i = 0; i < pipelines.size(); i++) {
        PipelineInput pipeline = pipelines.get(i);

        if (pipelines.size() > 1) {
          if (i > 0) {
            output.append("\n");
          }
          output.append("-- Pipeline: ").append(pipeline.name).append("\n");
          if (pipeline.description != null) {
            output.append("-- ").append(pipeline.description).append("\n");
          }
        }

        TranslationResult result = translatePipeline(pipeline, options);
        output.append(result.sql());

        if (!result.bindVariables().isEmpty() && !options.inlineBindVariables) {
          output.append("\n\n-- Bind variables:\n");
          List<Object> bindVars = result.bindVariables();
          for (int j = 0; j < bindVars.size(); j++) {
            output.append("-- :").append(j + 1).append(" = ").append(formatValue(bindVars.get(j)));
            output.append("\n");
          }
        }

        if (result.hasWarnings()) {
          output.append("\n-- Warnings:\n");
          result.warnings().forEach(w -> output.append("-- ").append(w.message()).append("\n"));
        }

        if (i < pipelines.size() - 1) {
          output.append("\n");
        }
      }

      writeOutput(output.toString(), options.outputFile);
      return 0;

    } catch (IOException e) {
      err.println("Error reading input file: " + e.getMessage());
      return 2;
    } catch (Exception e) {
      err.println("Error translating pipeline: " + e.getMessage());
      return 3;
    }
  }

  private CliOptions parseArgs(String[] args) {
    CliOptions options = new CliOptions();

    for (int i = 0; i < args.length; i++) {
      String arg = args[i];

      switch (arg) {
        case "--help", "-h" -> options.showHelp = true;
        case "--version", "-v" -> options.showVersion = true;
        case "--inline", "-i" -> options.inlineBindVariables = true;
        case "--pretty", "-p" -> options.prettyPrint = true;
        case "--no-hints" -> options.includeHints = false;
        case "--strict" -> options.strictMode = true;
        case "--collection", "-c" -> {
          if (i + 1 >= args.length) {
            throw new IllegalArgumentException("--collection requires a value");
          }
          options.collection = args[++i];
        }
        case "--output", "-o" -> {
          if (i + 1 >= args.length) {
            throw new IllegalArgumentException("--output requires a value");
          }
          options.outputFile = args[++i];
        }
        case "--data-column" -> {
          if (i + 1 >= args.length) {
            throw new IllegalArgumentException("--data-column requires a value");
          }
          options.dataColumnName = args[++i];
        }
        default -> {
          if (arg.startsWith("-")) {
            throw new IllegalArgumentException("Unknown option: " + arg);
          }
          if (options.inputFile != null) {
            throw new IllegalArgumentException("Multiple input files not supported");
          }
          options.inputFile = arg;
        }
      }
    }

    return options;
  }

  private List<PipelineInput> readPipelines(String inputFile, String collectionOverride)
      throws IOException {
    String content = Files.readString(Path.of(inputFile));
    List<PipelineInput> pipelines = new ArrayList<>();

    // First, try to detect if this is a raw pipeline array (starts with '[')
    String trimmed = content.trim();
    if (trimmed.startsWith("[")) {
      // Parse as a raw pipeline array
      @SuppressWarnings("unchecked")
      List<Document> stages =
          Document.parse("{\"stages\":" + content + "}").getList("stages", Document.class);
      PipelineInput input = new PipelineInput();
      input.name = "Pipeline";
      input.collection = collectionOverride != null ? collectionOverride : "collection";
      input.stages = stages;
      pipelines.add(input);
      return pipelines;
    }

    // Otherwise parse as a document
    Document doc = Document.parse(content);

    // Check if this is a multi-pipeline file (has "pipelines" array)
    if (doc.containsKey("pipelines")) {
      @SuppressWarnings("unchecked")
      List<Document> pipelineDocs = (List<Document>) doc.get("pipelines");
      for (Document pipelineDoc : pipelineDocs) {
        pipelines.add(parsePipelineDoc(pipelineDoc, collectionOverride));
      }
    } else if (doc.containsKey("pipeline")) {
      // Single pipeline with metadata
      pipelines.add(parsePipelineDoc(doc, collectionOverride));
    } else {
      throw new IllegalArgumentException(
          "Invalid input file format. Expected a pipeline array, "
              + "or a document with 'pipeline' or 'pipelines' key.");
    }

    return pipelines;
  }

  private PipelineInput parsePipelineDoc(Document doc, String collectionOverride) {
    PipelineInput input = new PipelineInput();
    input.id = doc.getString("id");
    input.name = doc.getString("name");
    if (input.name == null) {
      input.name = input.id != null ? input.id : "Pipeline";
    }
    input.description = doc.getString("description");

    // Command-line collection override takes precedence over file setting
    if (collectionOverride != null) {
      input.collection = collectionOverride;
    } else {
      input.collection = doc.getString("collection");
      if (input.collection == null) {
        input.collection = "collection";
      }
    }

    @SuppressWarnings("unchecked")
    List<Document> stages = (List<Document>) doc.get("pipeline");
    input.stages = stages;

    return input;
  }

  private TranslationResult translatePipeline(PipelineInput pipeline, CliOptions options) {
    OracleConfiguration config =
        OracleConfiguration.builder()
            .collectionName(pipeline.collection)
            .dataColumnName(options.dataColumnName != null ? options.dataColumnName : "data")
            .build();

    TranslationOptions translationOptions =
        TranslationOptions.builder()
            .inlineBindVariables(options.inlineBindVariables)
            .prettyPrint(options.prettyPrint)
            .includeHints(options.includeHints)
            .strictMode(options.strictMode)
            .build();

    AggregationTranslator translator = AggregationTranslator.create(config, translationOptions);
    return translator.translate(pipeline.stages);
  }

  private String formatValue(Object value) {
    if (value == null) {
      return "NULL";
    }
    if (value instanceof String) {
      return "'" + value + "'";
    }
    return value.toString();
  }

  private void writeOutput(String output, String outputFile) throws IOException {
    if (outputFile != null) {
      Files.writeString(Path.of(outputFile), output);
      out.println("Output written to: " + outputFile);
    } else {
      out.println(output);
    }
  }

  private void printHelp() {
    out.println("MongoDB to Oracle SQL Translator");
    out.println();
    out.println("Usage: mongo2sql [options] <input-file>");
    out.println();
    out.println("Translates MongoDB aggregation pipelines to Oracle SQL/JSON statements.");
    out.println();
    out.println("Input File Format:");
    out.println("  The input file can be in one of these formats:");
    out.println();
    out.println("  1. Single pipeline (array of stages):");
    out.println("     [{\"$match\": {\"status\": \"active\"}}, {\"$limit\": 10}]");
    out.println();
    out.println("  2. Single pipeline with metadata:");
    out.println("     {");
    out.println("       \"name\": \"My Pipeline\",");
    out.println("       \"collection\": \"orders\",");
    out.println("       \"pipeline\": [{\"$match\": {...}}]");
    out.println("     }");
    out.println();
    out.println("  3. Multiple pipelines:");
    out.println("     {");
    out.println("       \"pipelines\": [");
    out.println("         {\"name\": \"P1\", \"collection\": \"orders\", \"pipeline\": [...]},");
    out.println("         {\"name\": \"P2\", \"collection\": \"products\", \"pipeline\": [...]}");
    out.println("       ]");
    out.println("     }");
    out.println();
    out.println("Options:");
    out.println("  -c, --collection <name>  Collection/table name (overrides file setting)");
    out.println("  -i, --inline             Inline bind variables into SQL");
    out.println("  -p, --pretty             Pretty-print the SQL output");
    out.println("  --no-hints               Disable Oracle optimizer hints");
    out.println("  --strict                 Fail on unsupported operators");
    out.println("  --data-column <name>     JSON data column name (default: data)");
    out.println("  -o, --output <file>      Write output to file instead of stdout");
    out.println("  -v, --version            Show version information");
    out.println("  -h, --help               Show this help message");
    out.println();
    out.println("Examples:");
    out.println("  mongo2sql pipeline.json");
    out.println("  mongo2sql --collection orders --pretty pipeline.json");
    out.println("  mongo2sql --inline --output result.sql pipeline.json");
  }

  private static class CliOptions {
    boolean showHelp;
    boolean showVersion;
    boolean inlineBindVariables;
    boolean prettyPrint;
    boolean includeHints = true;
    boolean strictMode;
    String collection;
    String dataColumnName;
    String inputFile;
    String outputFile;
  }

  private static class PipelineInput {
    String id;
    String name;
    String description;
    String collection;
    List<Document> stages;
  }
}
