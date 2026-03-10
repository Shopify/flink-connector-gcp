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

package com.google.flink.connector.gcp.bigtable.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.types.logical.LogicalType;

import com.google.flink.connector.gcp.bigtable.table.config.BigtableChangelogMode;
import com.google.flink.connector.gcp.bigtable.table.config.BigtableConnectorOptions;
import com.google.flink.connector.gcp.bigtable.table.format.BigtableRowKeyFormat;
import com.google.flink.connector.gcp.bigtable.table.format.BigtableRowKeyFormatFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** Factory class to create configured instances of {@link BigtableDynamicTableSink}. */
@Internal
public class BigtableDynamicTableFactory implements DynamicTableSinkFactory {
    public static final String IDENTIFIER = "bigtable";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> requiredOptions = new HashSet<>();

        requiredOptions.add(BigtableConnectorOptions.PROJECT);
        requiredOptions.add(BigtableConnectorOptions.INSTANCE);
        requiredOptions.add(BigtableConnectorOptions.TABLE);

        return requiredOptions;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> additionalOptions = new HashSet<>();

        additionalOptions.add(BigtableConnectorOptions.COLUMN_FAMILY);
        additionalOptions.add(BigtableConnectorOptions.USE_NESTED_ROWS_MODE);
        additionalOptions.add(BigtableConnectorOptions.SINK_PARALLELISM);
        additionalOptions.add(BigtableConnectorOptions.FLOW_CONTROL);
        additionalOptions.add(BigtableConnectorOptions.APP_PROFILE_ID);
        additionalOptions.add(BigtableConnectorOptions.CREDENTIALS_FILE);
        additionalOptions.add(BigtableConnectorOptions.CREDENTIALS_KEY);
        additionalOptions.add(BigtableConnectorOptions.CREDENTIALS_ACCESS_TOKEN);
        additionalOptions.add(BigtableConnectorOptions.CHANGELOG_MODE);
        additionalOptions.add(BigtableConnectorOptions.VALUE_FORMAT);
        additionalOptions.add(BigtableConnectorOptions.KEY_FORMAT);

        return additionalOptions;
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);

        final Optional<EncodingFormat<SerializationSchema<RowData>>> valueEncodingFormat =
                helper.discoverOptionalEncodingFormat(
                        SerializationFormatFactory.class, BigtableConnectorOptions.VALUE_FORMAT);

        // Collect prefixes for dynamic options that FactoryUtil doesn't know about
        Set<String> dynamicPrefixes = new HashSet<>();

        // qualifier-field prefixes
        context.getCatalogTable().getOptions().keySet().stream()
                .filter(k -> k.endsWith(BigtableConnectorOptions.QUALIFIER_FIELD_SUFFIX))
                .map(
                        k ->
                                k.substring(
                                                0,
                                                k.length()
                                                        - BigtableConnectorOptions
                                                                .QUALIFIER_FIELD_SUFFIX
                                                                .length())
                                        + ".")
                .forEach(dynamicPrefixes::add);

        // key format prefixes (e.g. "key.zero-padded.", "key.composite-zero-padded.")
        dynamicPrefixes.add("key.zero-padded.");
        dynamicPrefixes.add("key.composite-zero-padded.");

        if (dynamicPrefixes.isEmpty()) {
            helper.validate();
        } else {
            helper.validateExcept(dynamicPrefixes.toArray(new String[0]));
        }

        final ReadableConfig tableOptions = helper.getOptions();
        final String changelogMode = tableOptions.get(BigtableConnectorOptions.CHANGELOG_MODE);
        final Map<String, String> rawTableOptions = context.getCatalogTable().getOptions();

        validateChangelogMode(changelogMode, context);

        // Discover and create row key format
        ResolvedSchema resolvedSchema = context.getCatalogTable().getResolvedSchema();
        BigtableRowKeyFormat rowKeyFormat =
                createRowKeyFormat(tableOptions, rawTableOptions, resolvedSchema);

        return new BigtableDynamicTableSink(
                resolvedSchema,
                tableOptions,
                valueEncodingFormat.orElse(null),
                rawTableOptions,
                rowKeyFormat);
    }

    private static BigtableRowKeyFormat createRowKeyFormat(
            ReadableConfig tableOptions,
            Map<String, String> rawTableOptions,
            ResolvedSchema resolvedSchema) {
        String formatIdentifier = tableOptions.get(BigtableConnectorOptions.KEY_FORMAT);

        int[] pkIndices = resolvedSchema.getPrimaryKeyIndexes();
        LogicalType[] pkTypes = new LogicalType[pkIndices.length];
        String[] pkNames = new String[pkIndices.length];
        for (int i = 0; i < pkIndices.length; i++) {
            pkTypes[i] =
                    resolvedSchema
                            .getColumn(pkIndices[i])
                            .get()
                            .getDataType()
                            .getLogicalType();
            pkNames[i] = resolvedSchema.getColumn(pkIndices[i]).get().getName();
        }

        BigtableRowKeyFormatFactory formatFactory =
                FactoryUtil.discoverFactory(
                        Thread.currentThread().getContextClassLoader(),
                        BigtableRowKeyFormatFactory.class,
                        formatIdentifier);

        return formatFactory.createRowKeyFormat(
                tableOptions, rawTableOptions, pkIndices, pkTypes, pkNames);
    }

    private static void validateChangelogMode(String changelogMode, Context context) {
        BigtableChangelogMode mode = BigtableChangelogMode.fromString(changelogMode);

        if (mode != BigtableChangelogMode.INSERT_ONLY) {
            int[] primaryKeyIndexes =
                    context.getCatalogTable().getResolvedSchema().getPrimaryKeyIndexes();
            if (primaryKeyIndexes.length == 0) {
                throw new ValidationException(
                        String.format(
                                "'bigtable' connector with changelog-mode '%s' requires a "
                                        + "PRIMARY KEY to be defined. The PRIMARY KEY specifies "
                                        + "which columns map to the Bigtable row key and "
                                        + "determines how records are updated or deleted.",
                                changelogMode));
            }
        }
    }
}
