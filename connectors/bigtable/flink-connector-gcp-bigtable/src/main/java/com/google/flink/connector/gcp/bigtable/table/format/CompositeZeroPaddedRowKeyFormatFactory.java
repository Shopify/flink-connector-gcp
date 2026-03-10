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
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;

import java.util.Collections;
import java.util.HashSet;
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

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Collections.emptySet();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(SEPARATOR);
        return options;
    }

    @Override
    public BigtableRowKeyFormat createRowKeyFormat(
            ReadableConfig tableOptions,
            Map<String, String> rawTableOptions,
            int[] keyFieldIndices,
            LogicalType[] keyFieldTypes,
            String[] keyFieldNames) {
        if (keyFieldIndices.length == 0) {
            throw new ValidationException(
                    "'composite-zero-padded' key format requires at least one primary key column.");
        }

        // Validate all key field types
        for (int i = 0; i < keyFieldTypes.length; i++) {
            LogicalType type = keyFieldTypes[i];
            if (!type.is(LogicalTypeFamily.CHARACTER_STRING)
                    && !type.is(LogicalTypeFamily.INTEGER_NUMERIC)) {
                throw new ValidationException(
                        String.format(
                                "Key format 'composite-zero-padded' does not support type %s for field '%s'. "
                                        + "Supported types: STRING, BIGINT, INTEGER, SMALLINT, TINYINT.",
                                type, keyFieldNames[i]));
            }
        }

        String separator = rawTableOptions.getOrDefault(SEPARATOR.key(), "#");

        int[] padLengths = new int[keyFieldNames.length];
        for (int i = 0; i < keyFieldNames.length; i++) {
            String padLengthStr = rawTableOptions.get(PAD_LENGTH_PREFIX + keyFieldNames[i]);
            padLengths[i] = padLengthStr != null ? Integer.parseInt(padLengthStr) : 0;
        }

        return new CompositeZeroPaddedRowKeyFormat(
                keyFieldIndices, keyFieldTypes, padLengths, separator);
    }
}
