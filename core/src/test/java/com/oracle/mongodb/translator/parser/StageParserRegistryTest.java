/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */
package com.oracle.mongodb.translator.parser;

import static org.assertj.core.api.Assertions.assertThat;

import com.oracle.mongodb.translator.ast.stage.LimitStage;
import com.oracle.mongodb.translator.ast.stage.SkipStage;
import org.junit.jupiter.api.Test;

class StageParserRegistryTest {

    @Test
    void shouldContainLimitParser() {
        var registry = new StageParserRegistry();

        assertThat(registry.hasParser("$limit")).isTrue();
    }

    @Test
    void shouldContainSkipParser() {
        var registry = new StageParserRegistry();

        assertThat(registry.hasParser("$skip")).isTrue();
    }

    @Test
    void shouldReturnNullForUnknownStage() {
        var registry = new StageParserRegistry();

        assertThat(registry.getParser("$unknown")).isNull();
    }

    @Test
    void shouldParseLimitValue() {
        var registry = new StageParserRegistry();
        var parser = registry.getParser("$limit");

        var stage = parser.parse(10);

        assertThat(stage).isInstanceOf(LimitStage.class);
        assertThat(((LimitStage) stage).getLimit()).isEqualTo(10);
    }

    @Test
    void shouldParseSkipValue() {
        var registry = new StageParserRegistry();
        var parser = registry.getParser("$skip");

        var stage = parser.parse(20);

        assertThat(stage).isInstanceOf(SkipStage.class);
        assertThat(((SkipStage) stage).getSkip()).isEqualTo(20);
    }

    @Test
    void shouldRegisterCustomParser() {
        var registry = new StageParserRegistry();
        registry.register("$custom", value -> new LimitStage(999));

        assertThat(registry.hasParser("$custom")).isTrue();
    }

    @Test
    void shouldReturnRegisteredParserNames() {
        var registry = new StageParserRegistry();

        var names = registry.getRegisteredOperators();

        assertThat(names).contains("$limit", "$skip");
    }
}
