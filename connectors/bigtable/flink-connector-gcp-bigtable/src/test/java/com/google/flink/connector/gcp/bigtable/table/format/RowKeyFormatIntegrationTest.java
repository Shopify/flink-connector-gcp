/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.flink.connector.gcp.bigtable.table.format;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.utils.FactoryMocks;

import com.google.flink.connector.gcp.bigtable.table.BigtableDynamicTableSink;
import com.google.flink.connector.gcp.bigtable.table.config.BigtableConnectorOptions;
import com.google.flink.connector.gcp.bigtable.testingutils.TestingUtils;
import org.assertj.core.api.Assertions;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertNotNull;

public class RowKeyFormatIntegrationTest {

    @Test
    public void testDefaultToStringFormat() {
        // No key.format specified — should default to to-string
        Map<String, String> options = getRequiredOptions();
        options.put(BigtableConnectorOptions.COLUMN_FAMILY.key(), TestingUtils.COLUMN_FAMILY);

        ResolvedSchema schema = getSinglePkSchema();

        BigtableDynamicTableSink sink =
                (BigtableDynamicTableSink) FactoryMocks.createTableSink(schema, options);
        assertNotNull(sink);
    }

    @Test
    public void testExplicitToStringFormat() {
        Map<String, String> options = getRequiredOptions();
        options.put(BigtableConnectorOptions.COLUMN_FAMILY.key(), TestingUtils.COLUMN_FAMILY);
        options.put("key.format", "to-string");

        ResolvedSchema schema = getSinglePkSchema();

        BigtableDynamicTableSink sink =
                (BigtableDynamicTableSink) FactoryMocks.createTableSink(schema, options);
        assertNotNull(sink);
    }

    @Test
    public void testZeroPaddedFormat() {
        Map<String, String> options = getRequiredOptions();
        options.put(BigtableConnectorOptions.COLUMN_FAMILY.key(), TestingUtils.COLUMN_FAMILY);
        options.put("key.format", "zero-padded");
        options.put("key.zero-padded.pad-length", "10");

        ResolvedSchema schema = getNumericPkSchema();

        BigtableDynamicTableSink sink =
                (BigtableDynamicTableSink) FactoryMocks.createTableSink(schema, options);
        assertNotNull(sink);
    }

    @Test
    public void testZeroPaddedMissingPadLengthThrows() {
        Map<String, String> options = getRequiredOptions();
        options.put(BigtableConnectorOptions.COLUMN_FAMILY.key(), TestingUtils.COLUMN_FAMILY);
        options.put("key.format", "zero-padded");

        ResolvedSchema schema = getNumericPkSchema();

        Assertions.assertThatThrownBy(() -> FactoryMocks.createTableSink(schema, options))
                .hasStackTraceContaining("pad-length");
    }

    @Test
    public void testCompositeZeroPaddedFormat() {
        Map<String, String> options = getRequiredOptions();
        options.put(BigtableConnectorOptions.COLUMN_FAMILY.key(), TestingUtils.COLUMN_FAMILY);
        options.put("key.format", "composite-zero-padded");
        options.put("key.composite-zero-padded.separator", "#");
        options.put("key.composite-zero-padded.pad-length.shop_id", "10");
        options.put("key.composite-zero-padded.pad-length.product_id", "10");

        ResolvedSchema schema = getCompositePkSchema();

        BigtableDynamicTableSink sink =
                (BigtableDynamicTableSink) FactoryMocks.createTableSink(schema, options);
        assertNotNull(sink);
    }

    @Test
    public void testToStringWithCompositePkThrows() {
        Map<String, String> options = getRequiredOptions();
        options.put(BigtableConnectorOptions.COLUMN_FAMILY.key(), TestingUtils.COLUMN_FAMILY);
        options.put("key.format", "to-string");

        ResolvedSchema schema = getCompositePkSchema();

        Assertions.assertThatThrownBy(() -> FactoryMocks.createTableSink(schema, options))
                .hasStackTraceContaining("exactly one primary key");
    }

    private static ResolvedSchema getSinglePkSchema() {
        List<Column> columns =
                Arrays.asList(
                        Column.physical(TestingUtils.ROW_KEY_FIELD, DataTypes.STRING().notNull()),
                        Column.physical(TestingUtils.STRING_FIELD, DataTypes.STRING()));
        return new ResolvedSchema(
                columns,
                Collections.emptyList(),
                UniqueConstraint.primaryKey(
                        "pk", Arrays.asList(TestingUtils.ROW_KEY_FIELD)));
    }

    private static ResolvedSchema getNumericPkSchema() {
        List<Column> columns =
                Arrays.asList(
                        Column.physical("shop_id", DataTypes.BIGINT().notNull()),
                        Column.physical(TestingUtils.STRING_FIELD, DataTypes.STRING()));
        return new ResolvedSchema(
                columns,
                Collections.emptyList(),
                UniqueConstraint.primaryKey("pk", Arrays.asList("shop_id")));
    }

    private static ResolvedSchema getCompositePkSchema() {
        List<Column> columns =
                Arrays.asList(
                        Column.physical("shop_id", DataTypes.BIGINT().notNull()),
                        Column.physical("product_id", DataTypes.BIGINT().notNull()),
                        Column.physical(TestingUtils.STRING_FIELD, DataTypes.STRING()));
        return new ResolvedSchema(
                columns,
                Collections.emptyList(),
                UniqueConstraint.primaryKey(
                        "pk", Arrays.asList("shop_id", "product_id")));
    }

    private static Map<String, String> getRequiredOptions() {
        Map<String, String> options = new HashMap<>();
        options.put(FactoryUtil.CONNECTOR.key(), "bigtable");
        options.put(BigtableConnectorOptions.INSTANCE.key(), TestingUtils.INSTANCE);
        options.put(BigtableConnectorOptions.TABLE.key(), TestingUtils.TABLE);
        options.put(BigtableConnectorOptions.PROJECT.key(), TestingUtils.PROJECT);
        return options;
    }
}
