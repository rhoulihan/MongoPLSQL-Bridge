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
import com.oracle.mongodb.translator.generator.ProceduralSqlConverter;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
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

    // Validate connection file early if --execute is specified
    ConnectionConfig connectionConfig = null;
    if (options.executeConnectionFile != null) {
      try {
        connectionConfig = loadConnectionConfig(options.executeConnectionFile);
      } catch (IOException e) {
        err.println("Error reading connection file: " + e.getMessage());
        return 2;
      }
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

        // Determine if procedural mode should be used
        boolean useProceduralMode = options.procedural
            || (options.autoProcedural
                && ProceduralSqlConverter.shouldUseProcedural(result.sql()));

        String sql = useProceduralMode
            ? ProceduralSqlConverter.convert(result.sql())
            : result.sql();
        output.append(sql);

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

        // Execute against Oracle if --execute was specified
        if (connectionConfig != null) {
          out.println("-- Generated SQL:");
          out.println(sql);
          out.println();
          out.println("-- Execution Results:");
          try {
            executeAndDisplayResults(sql, connectionConfig);
          } catch (SQLException e) {
            err.println("Oracle Error: " + e.getMessage());
            err.println("Error Code: " + e.getErrorCode());
            return 4;
          }
        }
      }

      if (connectionConfig == null) {
        writeOutput(output.toString(), options.outputFile);
      }
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
        case "--no-hints" -> options.includeHints = false;
        case "--strict" -> options.strictMode = true;
        case "--cte" -> options.cteMode = true; // Already default, kept for backward compatibility
        case "--no-cte", "--legacy" -> options.cteMode = false;
        case "--procedural" -> options.procedural = true; // Force procedural mode
        case "--auto-procedural" -> options.autoProcedural = true; // Auto-detect complex queries
        case "--no-auto-procedural" -> options.autoProcedural = false; // Disable auto-detection
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
        case "--execute", "-x" -> {
          if (i + 1 >= args.length) {
            throw new IllegalArgumentException("--execute requires a value");
          }
          options.executeConnectionFile = args[++i];
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
            .useCteBasedRendering(options.cteMode)
            .build();

    TranslationOptions translationOptions =
        TranslationOptions.builder()
            .inlineBindVariables(options.inlineBindVariables)
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

  private ConnectionConfig loadConnectionConfig(String connectionFile) throws IOException {
    Path path = Path.of(connectionFile);
    if (!Files.exists(path)) {
      throw new IOException("connection file not found: " + connectionFile);
    }
    String content = Files.readString(path);
    Document doc = Document.parse(content);
    return new ConnectionConfig(
        doc.getString("jdbcUrl"), doc.getString("user"), doc.getString("password"));
  }

  @SuppressFBWarnings(
      value = "SQL_INJECTION_JDBC",
      justification = "SQL generated by translator from MongoDB pipeline, not user input")
  private void executeAndDisplayResults(String sql, ConnectionConfig config) throws SQLException {
    try (Connection conn =
            DriverManager.getConnection(config.jdbcUrl, config.user, config.password);
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(sql)) {

      ResultSetMetaData meta = rs.getMetaData();
      int columnCount = meta.getColumnCount();

      // Print column headers
      StringBuilder header = new StringBuilder();
      for (int i = 1; i <= columnCount; i++) {
        if (i > 1) {
          header.append(" | ");
        }
        header.append(meta.getColumnLabel(i));
      }
      out.println(header);
      out.println("-".repeat(Math.max(header.length(), 40)));

      // Print rows
      int rowCount = 0;
      while (rs.next()) {
        StringBuilder row = new StringBuilder();
        for (int i = 1; i <= columnCount; i++) {
          if (i > 1) {
            row.append(" | ");
          }
          Object value = rs.getObject(i);
          row.append(value == null ? "NULL" : value.toString());
        }
        out.println(row);
        rowCount++;
      }

      out.println();
      out.println("(" + rowCount + " rows)");
    }
  }

  private record ConnectionConfig(String jdbcUrl, String user, String password) {}

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
    out.println("  --no-hints               Disable Oracle optimizer hints");
    out.println("  --strict                 Fail on unsupported operators");
    out.println("  --cte                    Use CTE-based SQL generation (default)");
    out.println("  --no-cte, --legacy       Use legacy SQL generation (deprecated)");
    out.println("  --procedural             Force procedural SQL with temp tables");
    out.println("  --auto-procedural        Auto-detect complex queries (default: enabled)");
    out.println("                           Uses procedural mode when CTEs >= 15");
    out.println("  --no-auto-procedural     Disable auto-detection of complex queries");
    out.println("  --data-column <name>     JSON data column name (default: data)");
    out.println("  -x, --execute <file>     Execute SQL against Oracle using connection file");
    out.println("  -o, --output <file>      Write output to file instead of stdout");
    out.println("  -v, --version            Show version information");
    out.println("  -h, --help               Show this help message");
    out.println();
    out.println("Connection File Format (JSON):");
    out.println("  {");
    out.println("    \"jdbcUrl\": \"jdbc:oracle:thin:@localhost:1521/FREEPDB1\",");
    out.println("    \"user\": \"username\",");
    out.println("    \"password\": \"password\"");
    out.println("  }");
    out.println();
    out.println("Examples:");
    out.println("  mongo2sql pipeline.json");
    out.println("  mongo2sql --collection orders --inline pipeline.json");
    out.println("  mongo2sql --inline --output result.sql pipeline.json");
  }

  private static class CliOptions {
    boolean showHelp;
    boolean showVersion;
    boolean inlineBindVariables;
    boolean includeHints = true;
    boolean strictMode;
    boolean cteMode = true; // CTE-based rendering is now the default
    boolean procedural; // Generate procedural SQL with temp tables
    boolean autoProcedural = true; // Auto-detect complex queries (default: enabled)
    String collection;
    String dataColumnName;
    String inputFile;
    String outputFile;
    String executeConnectionFile;
  }

  private static class PipelineInput {
    String id;
    String name;
    String description;
    String collection;
    List<Document> stages;
  }
}
