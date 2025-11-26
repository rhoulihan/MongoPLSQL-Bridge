/*
 * Copyright (c) 2024 Oracle and/or its affiliates.
 * Licensed under the Universal Permissive License v 1.0 as shown at
 * https://oss.oracle.com/licenses/upl/
 */
package com.oracle.mongodb.translator.integration;

import static org.assertj.core.api.Assertions.assertThat;

import com.oracle.mongodb.translator.api.AggregationTranslator;
import com.oracle.mongodb.translator.api.OracleConfiguration;
import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.oracle.OracleContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

/**
 * Integration tests that run against a real Oracle database using Testcontainers.
 */
@Testcontainers
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class OracleIntegrationTest {

    @Container
    static OracleContainer oracle = new OracleContainer("gvenzl/oracle-free:23.6-slim-faststart")
        .withDatabaseName("testdb")
        .withUsername("testuser")
        .withPassword("testpass")
        .withStartupTimeoutSeconds(300);

    private Connection connection;
    private AggregationTranslator translator;

    @BeforeAll
    void setUp() throws SQLException {
        connection = DriverManager.getConnection(
            oracle.getJdbcUrl(),
            oracle.getUsername(),
            oracle.getPassword()
        );

        // Create test table with JSON column
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("""
                CREATE TABLE orders (
                    id RAW(16) DEFAULT SYS_GUID() PRIMARY KEY,
                    data JSON
                )
            """);

            // Insert test data
            stmt.execute("""
                INSERT INTO orders (data) VALUES
                ('{"orderId": 1, "status": "active", "amount": 100}')
            """);
            stmt.execute("""
                INSERT INTO orders (data) VALUES
                ('{"orderId": 2, "status": "active", "amount": 200}')
            """);
            stmt.execute("""
                INSERT INTO orders (data) VALUES
                ('{"orderId": 3, "status": "completed", "amount": 150}')
            """);

            // Only commit if auto-commit is disabled
            if (!connection.getAutoCommit()) {
                connection.commit();
            }
        }

        translator = AggregationTranslator.create(
            OracleConfiguration.builder()
                .collectionName("orders")
                .build()
        );
    }

    @AfterAll
    void tearDown() throws SQLException {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }

    @Test
    void shouldConnectToOracle() throws SQLException {
        assertThat(connection.isValid(5)).isTrue();
    }

    @Test
    void shouldQueryJsonData() throws SQLException {
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(
                 "SELECT JSON_VALUE(data, '$.status') FROM orders")) {

            assertThat(rs.next()).isTrue();
            assertThat(rs.getString(1)).isNotNull();
        }
    }

    @Test
    void shouldTranslateAndExecuteLimitPipeline() throws SQLException {
        var pipeline = List.of(
            Document.parse("{\"$limit\": 2}")
        );

        var result = translator.translate(pipeline);

        try (PreparedStatement ps = connection.prepareStatement(result.sql());
             ResultSet rs = ps.executeQuery()) {

            int count = 0;
            while (rs.next()) {
                count++;
            }

            assertThat(count).isEqualTo(2);
        }
    }

    @Test
    void shouldTranslateAndExecuteSkipPipeline() throws SQLException {
        var pipeline = List.of(
            Document.parse("{\"$skip\": 1}")
        );

        var result = translator.translate(pipeline);

        try (PreparedStatement ps = connection.prepareStatement(result.sql());
             ResultSet rs = ps.executeQuery()) {

            int count = 0;
            while (rs.next()) {
                count++;
            }

            assertThat(count).isEqualTo(2); // 3 total - 1 skipped
        }
    }

    @Test
    void shouldTranslateAndExecuteSkipLimitPipeline() throws SQLException {
        var pipeline = List.of(
            Document.parse("{\"$skip\": 1}"),
            Document.parse("{\"$limit\": 1}")
        );

        var result = translator.translate(pipeline);

        try (PreparedStatement ps = connection.prepareStatement(result.sql());
             ResultSet rs = ps.executeQuery()) {

            int count = 0;
            while (rs.next()) {
                count++;
            }

            assertThat(count).isEqualTo(1);
        }
    }

    @Test
    void shouldReturnJsonDataFromPipeline() throws SQLException {
        var pipeline = List.of(
            Document.parse("{\"$limit\": 1}")
        );

        var result = translator.translate(pipeline);

        try (PreparedStatement ps = connection.prepareStatement(result.sql());
             ResultSet rs = ps.executeQuery()) {

            assertThat(rs.next()).isTrue();
            String json = rs.getString(1);
            assertThat(json).isNotNull();
            assertThat(json).contains("orderId");
            assertThat(json).contains("status");
        }
    }
}
