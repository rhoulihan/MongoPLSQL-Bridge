package com.oracle.mongodb.translator.cli;

import com.oracle.mongodb.translator.api.AggregationTranslator;
import com.oracle.mongodb.translator.api.OracleConfiguration;
import com.oracle.mongodb.translator.api.TranslationResult;
import org.bson.Document;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Command-line interface for translating MongoDB aggregation pipelines to Oracle SQL.
 *
 * <p>Usage: java -cp core.jar com.oracle.mongodb.translator.cli.TranslateCli &lt;collection&gt; &lt;pipeline-json&gt;
 * <p>Or: java -cp core.jar com.oracle.mongodb.translator.cli.TranslateCli &lt;collection&gt; --file &lt;pipeline-file&gt;
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
            System.err.println("   or: TranslateCli <collection> --file <pipeline-file>");
            System.exit(1);
        }

        String collection = args[0];
        String pipelineJson;

        if ("--file".equals(args[1]) && args.length >= 3) {
            try {
                pipelineJson = Files.readString(Path.of(args[2]));
            } catch (IOException e) {
                System.err.println("Error reading file: " + e.getMessage());
                System.exit(1);
                return;
            }
        } else {
            pipelineJson = args[1];
        }

        try {
            // Parse the pipeline JSON
            List<Document> pipeline = parsePipeline(pipelineJson);

            // Create translator
            OracleConfiguration config = OracleConfiguration.builder()
                    .collectionName(collection)
                    .build();

            AggregationTranslator translator = AggregationTranslator.create(config);

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
                    return ((List<?>) pipelineObj).stream()
                            .map(item -> item instanceof Document ? (Document) item : Document.parse(item.toString()))
                            .collect(Collectors.toList());
                }
            }
            // Single stage
            return List.of(doc);
        }
        throw new IllegalArgumentException("Invalid pipeline format");
    }
}
