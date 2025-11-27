/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */
package com.oracle.mongodb.translator.ast.stage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import com.oracle.mongodb.translator.generator.DefaultSqlGenerationContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class GraphLookupStageTest {

    private DefaultSqlGenerationContext context;

    @BeforeEach
    void setUp() {
        context = new DefaultSqlGenerationContext();
    }

    @Test
    void shouldCreateWithRequiredFields() {
        var stage = new GraphLookupStage(
            "employees", "$reportsTo", "reportsTo", "name", "hierarchy"
        );

        assertThat(stage.getFrom()).isEqualTo("employees");
        assertThat(stage.getStartWith()).isEqualTo("$reportsTo");
        assertThat(stage.getConnectFromField()).isEqualTo("reportsTo");
        assertThat(stage.getConnectToField()).isEqualTo("name");
        assertThat(stage.getAs()).isEqualTo("hierarchy");
        assertThat(stage.getMaxDepth()).isNull();
        assertThat(stage.getDepthField()).isNull();
    }

    @Test
    void shouldCreateWithAllOptions() {
        var stage = new GraphLookupStage(
            "employees", "$reportsTo", "reportsTo", "name", "hierarchy", 5, "level"
        );

        assertThat(stage.getMaxDepth()).isEqualTo(5);
        assertThat(stage.getDepthField()).isEqualTo("level");
    }

    @Test
    void shouldReturnOperatorName() {
        var stage = new GraphLookupStage("col", "$f", "from", "to", "result");

        assertThat(stage.getOperatorName()).isEqualTo("$graphLookup");
    }

    @Test
    void shouldRenderAsComment() {
        var stage = new GraphLookupStage(
            "employees", "$reportsTo", "reportsTo", "name", "hierarchy"
        );

        stage.render(context);

        String sql = context.toSql();
        assertThat(sql).contains("$graphLookup");
        assertThat(sql).contains("employees");
        assertThat(sql).contains("NOT YET IMPLEMENTED");
    }

    @Test
    void shouldThrowOnNullFrom() {
        assertThatNullPointerException()
            .isThrownBy(() -> new GraphLookupStage(null, "$f", "from", "to", "result"))
            .withMessageContaining("from");
    }

    @Test
    void shouldThrowOnNullStartWith() {
        assertThatNullPointerException()
            .isThrownBy(() -> new GraphLookupStage("col", null, "from", "to", "result"))
            .withMessageContaining("startWith");
    }

    @Test
    void shouldProvideReadableToString() {
        var stage = new GraphLookupStage(
            "employees", "$reportsTo", "reportsTo", "name", "hierarchy", 5, "level"
        );

        assertThat(stage.toString())
            .contains("GraphLookupStage")
            .contains("employees")
            .contains("maxDepth=5")
            .contains("depthField=level");
    }
}
