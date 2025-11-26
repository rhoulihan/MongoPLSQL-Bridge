/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */
package com.oracle.mongodb.translator.parser;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.oracle.mongodb.translator.ast.expression.AccumulatorOp;
import com.oracle.mongodb.translator.ast.stage.GroupStage;
import com.oracle.mongodb.translator.exception.UnsupportedOperatorException;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class GroupStageParserTest {

    private GroupStageParser parser;

    @BeforeEach
    void setUp() {
        parser = new GroupStageParser();
    }

    @Test
    void shouldParseSimpleGroupById() {
        // { $group: { _id: "$status" } }
        var doc = new Document("_id", "$status");

        var stage = parser.parse(doc);

        assertThat(stage.getIdExpression()).isNotNull();
        assertThat(stage.getAccumulators()).isEmpty();
    }

    @Test
    void shouldParseGroupByNullForAllDocuments() {
        // { $group: { _id: null, count: { $count: {} } } }
        var doc = new Document()
            .append("_id", null)
            .append("count", new Document("$count", new Document()));

        var stage = parser.parse(doc);

        assertThat(stage.getIdExpression()).isNull();
        assertThat(stage.getAccumulators()).containsKey("count");
    }

    @Test
    void shouldParseSumAccumulator() {
        // { $group: { _id: "$category", total: { $sum: "$amount" } } }
        var doc = new Document()
            .append("_id", "$category")
            .append("total", new Document("$sum", "$amount"));

        var stage = parser.parse(doc);

        assertThat(stage.getAccumulators()).containsKey("total");
        assertThat(stage.getAccumulators().get("total").getOp()).isEqualTo(AccumulatorOp.SUM);
    }

    @Test
    void shouldParseAvgAccumulator() {
        // { $group: { _id: "$category", avgPrice: { $avg: "$price" } } }
        var doc = new Document()
            .append("_id", "$category")
            .append("avgPrice", new Document("$avg", "$price"));

        var stage = parser.parse(doc);

        assertThat(stage.getAccumulators().get("avgPrice").getOp()).isEqualTo(AccumulatorOp.AVG);
    }

    @Test
    void shouldParseMinMaxAccumulators() {
        // { $group: { _id: null, minVal: { $min: "$value" }, maxVal: { $max: "$value" } } }
        var doc = new Document()
            .append("_id", null)
            .append("minVal", new Document("$min", "$value"))
            .append("maxVal", new Document("$max", "$value"));

        var stage = parser.parse(doc);

        assertThat(stage.getAccumulators()).hasSize(2);
        assertThat(stage.getAccumulators().get("minVal").getOp()).isEqualTo(AccumulatorOp.MIN);
        assertThat(stage.getAccumulators().get("maxVal").getOp()).isEqualTo(AccumulatorOp.MAX);
    }

    @Test
    void shouldParseSumWithLiteralForCounting() {
        // { $group: { _id: "$status", count: { $sum: 1 } } }
        var doc = new Document()
            .append("_id", "$status")
            .append("count", new Document("$sum", 1));

        var stage = parser.parse(doc);

        assertThat(stage.getAccumulators().get("count").getOp()).isEqualTo(AccumulatorOp.SUM);
        assertThat(stage.getAccumulators().get("count").getArgument()).isNotNull();
    }

    @Test
    void shouldParseMultipleAccumulators() {
        // { $group: { _id: "$category", count: { $sum: 1 }, total: { $sum: "$amount" }, avg: { $avg: "$amount" } } }
        var doc = new Document()
            .append("_id", "$category")
            .append("count", new Document("$sum", 1))
            .append("total", new Document("$sum", "$amount"))
            .append("avg", new Document("$avg", "$amount"));

        var stage = parser.parse(doc);

        assertThat(stage.getAccumulators()).hasSize(3);
    }

    @Test
    void shouldRejectUnknownAccumulator() {
        var doc = new Document()
            .append("_id", "$status")
            .append("result", new Document("$unknown", "$field"));

        assertThatThrownBy(() -> parser.parse(doc))
            .isInstanceOf(UnsupportedOperatorException.class)
            .hasMessageContaining("$unknown");
    }

    @Test
    void shouldRejectNonDocumentAccumulator() {
        var doc = new Document()
            .append("_id", "$status")
            .append("result", "invalid");

        assertThatThrownBy(() -> parser.parse(doc))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Accumulator must be a document");
    }
}
