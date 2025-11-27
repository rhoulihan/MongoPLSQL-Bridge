/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */
package com.oracle.mongodb.translator.parser;

import com.oracle.mongodb.translator.ast.expression.FieldPathExpression;
import com.oracle.mongodb.translator.ast.stage.LimitStage;
import com.oracle.mongodb.translator.ast.stage.LookupStage;
import com.oracle.mongodb.translator.ast.stage.MatchStage;
import com.oracle.mongodb.translator.ast.stage.SkipStage;
import com.oracle.mongodb.translator.ast.stage.SortStage;
import com.oracle.mongodb.translator.ast.stage.UnwindStage;
import com.oracle.mongodb.translator.ast.stage.AddFieldsStage;
import com.oracle.mongodb.translator.ast.stage.SortStage.SortDirection;
import com.oracle.mongodb.translator.ast.stage.SortStage.SortField;
import com.oracle.mongodb.translator.ast.stage.UnionWithStage;
import org.bson.Document;
import java.util.ArrayList;
import java.util.List;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Registry of stage parsers by operator name.
 */
public final class StageParserRegistry {

    private final Map<String, StageParser<?>> parsers = new HashMap<>();

    public StageParserRegistry() {
        registerBuiltInParsers();
    }

    private void registerBuiltInParsers() {
        // Tier 1 simple stages
        register("$limit", value -> new LimitStage(toInt(value)));
        register("$skip", value -> new SkipStage(toInt(value)));

        // $match stage - filter documents
        ExpressionParser exprParser = new ExpressionParser();
        register("$match", value -> new MatchStage(exprParser.parse((Document) value)));

        // $group stage - aggregate documents
        GroupStageParser groupParser = new GroupStageParser();
        register("$group", value -> groupParser.parse((Document) value));

        // $project stage - shape output documents
        ProjectStageParser projectParser = new ProjectStageParser();
        register("$project", value -> projectParser.parse((Document) value));

        // $sort stage - order documents
        register("$sort", this::parseSortStage);

        // $lookup stage - join with another collection
        LookupStageParser lookupParser = new LookupStageParser();
        register("$lookup", value -> lookupParser.parse((Document) value));

        // $unwind stage - deconstruct array field
        UnwindStageParser unwindParser = new UnwindStageParser();
        register("$unwind", unwindParser::parse);

        // $addFields and $set stages - add computed fields
        AddFieldsStageParser addFieldsParser = new AddFieldsStageParser();
        register("$addFields", value -> addFieldsParser.parse((Document) value));
        register("$set", value -> addFieldsParser.parse((Document) value));

        // $unionWith stage - combine with another collection
        // Use a lambda to defer PipelineParser creation to avoid circular initialization
        register("$unionWith", value -> new UnionWithStageParser(new PipelineParser(this)).parse(value));

        // $bucket stage - categorize documents into groups
        BucketStageParser bucketParser = new BucketStageParser();
        register("$bucket", value -> bucketParser.parse((Document) value));

        // $bucketAuto stage - automatically determine bucket boundaries
        BucketAutoStageParser bucketAutoParser = new BucketAutoStageParser();
        register("$bucketAuto", value -> bucketAutoParser.parse((Document) value));

        // $facet stage - multiple parallel pipelines
        register("$facet", value -> new FacetStageParser(new PipelineParser(this)).parse((Document) value));

        // $merge stage - write results with merge logic
        MergeStageParser mergeParser = new MergeStageParser();
        register("$merge", mergeParser::parse);

        // $out stage - write results to collection
        OutStageParser outParser = new OutStageParser();
        register("$out", outParser::parse);

        // $graphLookup stage - recursive search (stub)
        GraphLookupStageParser graphLookupParser = new GraphLookupStageParser();
        register("$graphLookup", value -> graphLookupParser.parse((Document) value));

        // $setWindowFields stage - window functions (stub)
        SetWindowFieldsStageParser setWindowFieldsParser = new SetWindowFieldsStageParser();
        register("$setWindowFields", value -> setWindowFieldsParser.parse((Document) value));

        // $redact stage - filter content based on document level
        RedactStageParser redactParser = new RedactStageParser();
        register("$redact", redactParser::parse);

        // $sample stage - randomly select documents
        SampleStageParser sampleParser = new SampleStageParser();
        register("$sample", value -> sampleParser.parse(value));

        // $count stage - count documents
        CountStageParser countParser = new CountStageParser();
        register("$count", countParser::parse);
    }

    private SortStage parseSortStage(Object value) {
        if (!(value instanceof Document sortDoc)) {
            throw new IllegalArgumentException("$sort requires a document");
        }

        List<SortField> sortFields = new ArrayList<>();
        for (Map.Entry<String, Object> entry : sortDoc.entrySet()) {
            String fieldName = entry.getKey();
            Object dirValue = entry.getValue();

            int direction;
            if (dirValue instanceof Number num) {
                direction = num.intValue();
            } else {
                throw new IllegalArgumentException(
                    "Sort direction must be a number (1 or -1), got: " + dirValue);
            }

            sortFields.add(new SortField(
                FieldPathExpression.of(fieldName),
                SortDirection.fromMongo(direction)
            ));
        }

        return new SortStage(sortFields);
    }

    /**
     * Registers a stage parser for the given operator name.
     */
    public void register(String operatorName, StageParser<?> parser) {
        parsers.put(operatorName, parser);
    }

    /**
     * Returns the parser for the given operator name, or null if not found.
     */
    public StageParser<?> getParser(String operatorName) {
        return parsers.get(operatorName);
    }

    /**
     * Returns true if a parser is registered for the given operator name.
     */
    public boolean hasParser(String operatorName) {
        return parsers.containsKey(operatorName);
    }

    /**
     * Returns the set of registered operator names.
     */
    public Set<String> getRegisteredOperators() {
        return Collections.unmodifiableSet(parsers.keySet());
    }

    private static int toInt(Object value) {
        if (value instanceof Integer intVal) {
            return intVal;
        }
        if (value instanceof Long longVal) {
            return longVal.intValue();
        }
        if (value instanceof Number numVal) {
            return numVal.intValue();
        }
        throw new IllegalArgumentException(
            "Expected numeric value, got: " + (value == null ? "null" : value.getClass().getName()));
    }
}
