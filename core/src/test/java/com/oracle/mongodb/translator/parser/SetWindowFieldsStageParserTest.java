/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */
package com.oracle.mongodb.translator.parser;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import com.oracle.mongodb.translator.ast.stage.SetWindowFieldsStage;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SetWindowFieldsStageParserTest {

    private SetWindowFieldsStageParser parser;

    @BeforeEach
    void setUp() {
        parser = new SetWindowFieldsStageParser();
    }

    @Test
    void shouldParseBasicWindowFieldsWithRank() {
        var doc = Document.parse("""
            {
                "partitionBy": "$department",
                "sortBy": { "salary": -1 },
                "output": {
                    "salaryRank": { "$rank": {} }
                }
            }
            """);

        SetWindowFieldsStage stage = parser.parse(doc);

        assertThat(stage.getPartitionBy()).isEqualTo("$department");
        assertThat(stage.getSortBy()).containsEntry("salary", -1);
        assertThat(stage.getOutput()).containsKey("salaryRank");
        assertThat(stage.getOutput().get("salaryRank").operator()).isEqualTo("$rank");
    }

    @Test
    void shouldParseSumWithWindow() {
        var doc = Document.parse("""
            {
                "partitionBy": "$state",
                "sortBy": { "orderDate": 1 },
                "output": {
                    "cumulativeSum": {
                        "$sum": "$quantity",
                        "window": { "documents": ["unbounded", "current"] }
                    }
                }
            }
            """);

        SetWindowFieldsStage stage = parser.parse(doc);

        assertThat(stage.getPartitionBy()).isEqualTo("$state");
        assertThat(stage.getSortBy()).containsEntry("orderDate", 1);

        var cumulativeSum = stage.getOutput().get("cumulativeSum");
        assertThat(cumulativeSum.operator()).isEqualTo("$sum");
        assertThat(cumulativeSum.argument()).isEqualTo("$quantity");
        assertThat(cumulativeSum.window()).isNotNull();
        assertThat(cumulativeSum.window().type()).isEqualTo("documents");
        assertThat(cumulativeSum.window().bounds()).containsExactly("unbounded", "current");
    }

    @Test
    void shouldParseRangeWindow() {
        var doc = Document.parse("""
            {
                "sortBy": { "price": 1 },
                "output": {
                    "avgPrice": {
                        "$avg": "$price",
                        "window": { "range": [-10, 10] }
                    }
                }
            }
            """);

        SetWindowFieldsStage stage = parser.parse(doc);

        assertThat(stage.getPartitionBy()).isNull();
        var avgPrice = stage.getOutput().get("avgPrice");
        assertThat(avgPrice.window().type()).isEqualTo("range");
        assertThat(avgPrice.window().bounds()).containsExactly("-10", "10");
    }

    @Test
    void shouldParseWithoutPartitionBy() {
        var doc = Document.parse("""
            {
                "sortBy": { "date": 1 },
                "output": {
                    "rowNum": { "$rowNumber": {} }
                }
            }
            """);

        SetWindowFieldsStage stage = parser.parse(doc);

        assertThat(stage.getPartitionBy()).isNull();
        assertThat(stage.getSortBy()).containsEntry("date", 1);
        assertThat(stage.getOutput().get("rowNum").operator()).isEqualTo("$rowNumber");
    }

    @Test
    void shouldParseWithoutSortBy() {
        var doc = Document.parse("""
            {
                "partitionBy": "$category",
                "output": {
                    "count": { "$count": {} }
                }
            }
            """);

        SetWindowFieldsStage stage = parser.parse(doc);

        assertThat(stage.getPartitionBy()).isEqualTo("$category");
        assertThat(stage.getSortBy()).isEmpty();
    }

    @Test
    void shouldParseMultipleOutputFields() {
        var doc = Document.parse("""
            {
                "partitionBy": "$dept",
                "sortBy": { "amount": -1 },
                "output": {
                    "rank": { "$rank": {} },
                    "denseRank": { "$denseRank": {} },
                    "total": { "$sum": "$amount" }
                }
            }
            """);

        SetWindowFieldsStage stage = parser.parse(doc);

        assertThat(stage.getOutput()).hasSize(3);
        assertThat(stage.getOutput().get("rank").operator()).isEqualTo("$rank");
        assertThat(stage.getOutput().get("denseRank").operator()).isEqualTo("$denseRank");
        assertThat(stage.getOutput().get("total").operator()).isEqualTo("$sum");
    }

    @Test
    void shouldParseMultipleSortFields() {
        var doc = Document.parse("""
            {
                "sortBy": { "category": 1, "price": -1, "name": 1 },
                "output": {
                    "rank": { "$rank": {} }
                }
            }
            """);

        SetWindowFieldsStage stage = parser.parse(doc);

        assertThat(stage.getSortBy()).hasSize(3);
        assertThat(stage.getSortBy().get("category")).isEqualTo(1);
        assertThat(stage.getSortBy().get("price")).isEqualTo(-1);
        assertThat(stage.getSortBy().get("name")).isEqualTo(1);
    }

    @Test
    void shouldHandleStringArgumentForOperator() {
        var doc = Document.parse("""
            {
                "output": {
                    "firstValue": { "$first": "$amount" }
                }
            }
            """);

        SetWindowFieldsStage stage = parser.parse(doc);

        assertThat(stage.getOutput().get("firstValue").operator()).isEqualTo("$first");
        assertThat(stage.getOutput().get("firstValue").argument()).isEqualTo("$amount");
    }

    @Test
    void shouldHandleNonStringArgumentForOperator() {
        var doc = Document.parse("""
            {
                "output": {
                    "sumLiteral": { "$sum": 1 }
                }
            }
            """);

        SetWindowFieldsStage stage = parser.parse(doc);

        assertThat(stage.getOutput().get("sumLiteral").operator()).isEqualTo("$sum");
        assertThat(stage.getOutput().get("sumLiteral").argument()).isEqualTo("1");
    }

    @Test
    void shouldHandleWindowWithNullBounds() {
        var doc = Document.parse("""
            {
                "output": {
                    "runningSum": {
                        "$sum": "$value",
                        "window": { "documents": [null, "current"] }
                    }
                }
            }
            """);

        SetWindowFieldsStage stage = parser.parse(doc);

        var window = stage.getOutput().get("runningSum").window();
        assertThat(window.bounds()).containsExactly("current", "current");
    }

    @Test
    void shouldThrowOnMissingOutput() {
        var doc = Document.parse("""
            {
                "partitionBy": "$dept",
                "sortBy": { "salary": 1 }
            }
            """);

        assertThatIllegalArgumentException()
            .isThrownBy(() -> parser.parse(doc))
            .withMessageContaining("output");
    }

    @Test
    void shouldThrowOnNonDocumentOutput() {
        var doc = Document.parse("""
            {
                "output": "invalid"
            }
            """);

        assertThatIllegalArgumentException()
            .isThrownBy(() -> parser.parse(doc))
            .withMessageContaining("output")
            .withMessageContaining("document");
    }

    @Test
    void shouldThrowOnNonDocumentOutputField() {
        var doc = Document.parse("""
            {
                "output": {
                    "badField": "not a document"
                }
            }
            """);

        assertThatIllegalArgumentException()
            .isThrownBy(() -> parser.parse(doc))
            .withMessageContaining("badField")
            .withMessageContaining("document");
    }

    @Test
    void shouldReturnCorrectOperatorName() {
        var doc = Document.parse("""
            {
                "output": {
                    "rank": { "$rank": {} }
                }
            }
            """);

        SetWindowFieldsStage stage = parser.parse(doc);

        assertThat(stage.getOperatorName()).isEqualTo("$setWindowFields");
    }

    @Test
    void shouldHandleEmptyDocumentArgument() {
        var doc = Document.parse("""
            {
                "output": {
                    "docNum": { "$documentNumber": {} }
                }
            }
            """);

        SetWindowFieldsStage stage = parser.parse(doc);

        var docNum = stage.getOutput().get("docNum");
        assertThat(docNum.operator()).isEqualTo("$documentNumber");
        assertThat(docNum.argument()).isNull();
    }

    @Test
    void shouldIgnoreNonNumericSortByValues() {
        var doc = Document.parse("""
            {
                "sortBy": { "field1": 1, "field2": "invalid", "field3": -1 },
                "output": {
                    "rank": { "$rank": {} }
                }
            }
            """);

        SetWindowFieldsStage stage = parser.parse(doc);

        // Only numeric values should be in sortBy
        assertThat(stage.getSortBy()).hasSize(2);
        assertThat(stage.getSortBy()).containsEntry("field1", 1);
        assertThat(stage.getSortBy()).containsEntry("field3", -1);
    }
}
