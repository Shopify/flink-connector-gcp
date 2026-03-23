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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Factory for the {@code composite-zero-padded} row key format. Concatenates multiple PK fields
 * with a separator, optionally zero-padding numeric fields.
 */
public class CompositeZeroPaddedRowKeyFormatFactory implements BigtableRowKeyFormatFactory {

    public static final String IDENTIFIER = "composite-zero-padded";

    private static final String PAD_LENGTH_PREFIX = "key.composite-zero-padded.pad-length.";

    public static final ConfigOption<String> SEPARATOR =
            ConfigOptions.key("key.composite-zero-padded.separator")
                    .stringType()
                    .defaultValue("#")
                    .withDescription("Separator between key parts. Defaults to '#'.");

    public static final ConfigOption<String> FIELDS =
            ConfigOptions.key("key.composite-zero-padded.fields")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Comma-separated column names for the composite row key. Defaults to primary key columns.");

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return java.util.Collections.emptySet();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(SEPARATOR);
        options.add(FIELDS);
        return options;
    }

    @Override
    public BigtableRowKeyFormat createRowKeyFormat(
            ReadableConfig tableOptions,
            Map<String, String> rawTableOptions,
            ResolvedSchema schema) {
        int[] fieldIndices;
        LogicalType[] fieldTypes;
        String[] fieldNames;

        String fieldsOption = rawTableOptions.get(FIELDS.key());
        List<String> columnNames = schema.getColumnNames();

        if (fieldsOption != null) {
            String[] names = fieldsOption.split(",");
            fieldIndices = new int[names.length];
            fieldTypes = new LogicalType[names.length];
            fieldNames = new String[names.length];
            for (int i = 0; i < names.length; i++) {
                String name = names[i].trim();
                int idx = columnNames.indexOf(name);
                if (idx < 0) {
                    throw new ValidationException(
                            String.format(
                                    "Column '%s' specified in '%s' not found in schema.",
                                    name, FIELDS.key()));
                }
                fieldIndices[i] = idx;
                fieldTypes[i] = schema.getColumn(idx).get().getDataType().getLogicalType();
                fieldNames[i] = name;
            }
        } else {
            int[] pkIndices = schema.getPrimaryKeyIndexes();
            if (pkIndices.length == 0) {
                throw new ValidationException(
                        String.format(
                                "'composite-zero-padded' key format requires at least one primary key column (or set '%s').",
                                FIELDS.key()));
            }
            fieldIndices = pkIndices;
            fieldTypes = new LogicalType[pkIndices.length];
            fieldNames = new String[pkIndices.length];
            for (int i = 0; i < pkIndices.length; i++) {
                fieldTypes[i] =
                        schema.getColumn(pkIndices[i]).get().getDataType().getLogicalType();
                fieldNames[i] = schema.getColumn(pkIndices[i]).get().getName();
            }
        }

        // Validate all field types
        for (int i = 0; i < fieldTypes.length; i++) {
            LogicalType type = fieldTypes[i];
            if (!type.is(LogicalTypeFamily.CHARACTER_STRING)
                    && !type.is(LogicalTypeFamily.INTEGER_NUMERIC)) {
                throw new ValidationException(
                        String.format(
                                "Key format 'composite-zero-padded' does not support type %s for field '%s'. "
                                        + "Supported types: STRING, BIGINT, INTEGER, SMALLINT, TINYINT.",
                                type, fieldNames[i]));
            }
        }

        String separator = rawTableOptions.getOrDefault(SEPARATOR.key(), "#");

        int[] padLengths = new int[fieldNames.length];
        for (int i = 0; i < fieldNames.length; i++) {
            String padLengthStr = rawTableOptions.get(PAD_LENGTH_PREFIX + fieldNames[i]);
            padLengths[i] = padLengthStr != null ? Integer.parseInt(padLengthStr) : 0;
        }

        return new CompositeZeroPaddedRowKeyFormat(fieldIndices, fieldTypes, padLengths, separator);
    }
}
