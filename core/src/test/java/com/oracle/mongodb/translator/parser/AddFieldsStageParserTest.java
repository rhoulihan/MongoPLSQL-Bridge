/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */
package com.oracle.mongodb.translator.parser;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import com.oracle.mongodb.translator.ast.expression.ArithmeticExpression;
import com.oracle.mongodb.translator.ast.expression.FieldPathExpression;
import com.oracle.mongodb.translator.ast.expression.LiteralExpression;
import com.oracle.mongodb.translator.ast.stage.AddFieldsStage;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AddFieldsStageParserTest {

    private AddFieldsStageParser parser;

    @BeforeEach
    void setUp() {
        parser = new AddFieldsStageParser();
    }

    @Test
    void shouldParseSingleLiteralField() {
        var doc = Document.parse("""
            {
                "status": "active"
            }
            """);

        AddFieldsStage stage = parser.parse(doc);

        assertThat(stage.getFields()).hasSize(1);
        assertThat(stage.getFields()).containsKey("status");
        assertThat(stage.getFields().get("status")).isInstanceOf(LiteralExpression.class);
    }

    @Test
    void shouldParseNumericLiteral() {
        var doc = Document.parse("""
            {
                "count": 42
            }
            """);

        AddFieldsStage stage = parser.parse(doc);

        assertThat(stage.getFields()).containsKey("count");
    }

    @Test
    void shouldParseFieldReference() {
        var doc = Document.parse("""
            {
                "copyOfName": "$name"
            }
            """);

        AddFieldsStage stage = parser.parse(doc);

        assertThat(stage.getFields()).containsKey("copyOfName");
        assertThat(stage.getFields().get("copyOfName")).isInstanceOf(FieldPathExpression.class);
    }

    @Test
    void shouldParseComputedExpression() {
        var doc = Document.parse("""
            {
                "total": { "$add": ["$price", "$tax"] }
            }
            """);

        AddFieldsStage stage = parser.parse(doc);

        assertThat(stage.getFields()).containsKey("total");
        assertThat(stage.getFields().get("total")).isInstanceOf(ArithmeticExpression.class);
    }

    @Test
    void shouldParseMultipleFields() {
        var doc = Document.parse("""
            {
                "field1": "value1",
                "field2": 100,
                "field3": "$existingField"
            }
            """);

        AddFieldsStage stage = parser.parse(doc);

        assertThat(stage.getFields()).hasSize(3);
        assertThat(stage.getFields()).containsKeys("field1", "field2", "field3");
    }

    @Test
    void shouldParseNestedFieldPath() {
        var doc = Document.parse("""
            {
                "authorName": "$metadata.author.name"
            }
            """);

        AddFieldsStage stage = parser.parse(doc);

        assertThat(stage.getFields()).containsKey("authorName");
    }

    @Test
    void shouldParseBooleanLiteral() {
        var doc = Document.parse("""
            {
                "isActive": true
            }
            """);

        AddFieldsStage stage = parser.parse(doc);

        assertThat(stage.getFields()).containsKey("isActive");
    }

    @Test
    void shouldParseNullLiteral() {
        var doc = Document.parse("""
            {
                "removedField": null
            }
            """);

        AddFieldsStage stage = parser.parse(doc);

        assertThat(stage.getFields()).containsKey("removedField");
    }

    @Test
    void shouldThrowOnEmptyDocument() {
        var doc = new Document();

        assertThatIllegalArgumentException()
            .isThrownBy(() -> parser.parse(doc))
            .withMessageContaining("at least one field");
    }
}
