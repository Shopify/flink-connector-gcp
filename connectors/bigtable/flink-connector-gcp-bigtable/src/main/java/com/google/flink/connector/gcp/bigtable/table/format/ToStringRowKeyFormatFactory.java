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
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * Factory for the {@code to-string} row key format. Converts a single primary key field to its
 * string representation.
 */
public class ToStringRowKeyFormatFactory implements BigtableRowKeyFormatFactory {

    public static final String IDENTIFIER = "to-string";

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
        return Collections.emptySet();
    }

    @Override
    public BigtableRowKeyFormat createRowKeyFormat(
            ReadableConfig tableOptions,
            Map<String, String> rawTableOptions,
            int[] keyFieldIndices,
            LogicalType[] keyFieldTypes,
            String[] keyFieldNames) {
        if (keyFieldIndices.length != 1) {
            throw new ValidationException(
                    String.format(
                            "'to-string' key format requires exactly one primary key column, but found %d.",
                            keyFieldIndices.length));
        }
        LogicalType type = keyFieldTypes[0];
        if (!type.is(LogicalTypeFamily.CHARACTER_STRING)
                && !type.is(LogicalTypeFamily.INTEGER_NUMERIC)) {
            throw new ValidationException(
                    String.format(
                            "Key format 'to-string' does not support type %s. "
                                    + "Supported types: STRING, BIGINT, INTEGER, SMALLINT, TINYINT.",
                            type));
        }
        return new ToStringRowKeyFormat(keyFieldIndices[0], type);
    }
}
