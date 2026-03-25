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

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.DataTypes.Field;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.flink.connector.gcp.bigtable.BigtableSink;
import com.google.flink.connector.gcp.bigtable.serializers.BaseRowMutationSerializer;
import com.google.flink.connector.gcp.bigtable.serializers.FormatAwareRowMutationSerializer;
import com.google.flink.connector.gcp.bigtable.serializers.QualifierConfig;
import com.google.flink.connector.gcp.bigtable.serializers.RowDataToRowMutationSerializer;
import com.google.flink.connector.gcp.bigtable.table.config.BigtableChangelogMode;
import com.google.flink.connector.gcp.bigtable.table.config.BigtableConnectorOptions;
import com.google.flink.connector.gcp.bigtable.utils.CredentialsFactory;
import com.google.flink.connector.gcp.bigtable.utils.ErrorMessages;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.flink.connector.gcp.bigtable.serializers.RowDataToRowMutationSerializer.SUPPORTED_ROW_KEY_TYPES;
import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * A {@link DynamicTableSink} for writing data into a Google Cloud Bigtable table. It generates the
 * Sink for Table API.
 */
public class BigtableDynamicTableSink implements DynamicTableSink {
    protected final Integer parallelism;
    protected final ReadableConfig connectorOptions;
    protected final ResolvedSchema resolvedSchema;
    protected final String rowKeyField;
    protected final @Nullable EncodingFormat<SerializationSchema<RowData>> valueEncodingFormat;
    protected final Map<String, String> rawTableOptions;

    public BigtableDynamicTableSink(
            ResolvedSchema resolvedSchema, ReadableConfig connectorOptions) {
        this(resolvedSchema, connectorOptions, null, Collections.emptyMap());
    }

    public BigtableDynamicTableSink(
            ResolvedSchema resolvedSchema,
            ReadableConfig connectorOptions,
            @Nullable EncodingFormat<SerializationSchema<RowData>> valueEncodingFormat,
            Map<String, String> rawTableOptions) {
        checkArgument(
                resolvedSchema.getPrimaryKeyIndexes().length == 1,
                String.format(
                        ErrorMessages.MULTIPLE_PRIMARY_KEYS_TEMPLATE,
                        resolvedSchema.getPrimaryKeyIndexes().length));
        int rowKeyIndex = resolvedSchema.getPrimaryKeyIndexes()[0];
        DataType rowKeyDataType = resolvedSchema.getColumn(rowKeyIndex).get().getDataType();
        LogicalTypeRoot rowKeyType = rowKeyDataType.getLogicalType().getTypeRoot();
        checkArgument(
                rowKeyDataType.equals(rowKeyDataType.notNull()), ErrorMessages.ROW_KEY_NULLABLE);
        checkArgument(
                SUPPORTED_ROW_KEY_TYPES.contains(rowKeyType),
                String.format(ErrorMessages.ROW_KEY_UNSUPPORTED_TYPE_TEMPLATE, rowKeyDataType));

        this.connectorOptions = connectorOptions;
        this.resolvedSchema = resolvedSchema;
        this.parallelism = connectorOptions.get(BigtableConnectorOptions.SINK_PARALLELISM);
        this.rowKeyField = resolvedSchema.getColumn(rowKeyIndex).get().getName();
        this.valueEncodingFormat = valueEncodingFormat;
        this.rawTableOptions = rawTableOptions != null ? rawTableOptions : Collections.emptyMap();

        boolean hasPrefixedQualifierFields =
                this.rawTableOptions.keySet().stream()
                        .anyMatch(
                                k ->
                                        k.endsWith(BigtableConnectorOptions.QUALIFIER_FIELD_SUFFIX)
                                                && !k.equals(BigtableConnectorOptions.QUALIFIER_FIELD.key()));
        if (hasPrefixedQualifierFields) {
            checkArgument(
                    valueEncodingFormat != null, ErrorMessages.QUALIFIER_FIELD_REQUIRES_FORMAT);
            checkArgument(
                    connectorOptions.get(BigtableConnectorOptions.USE_NESTED_ROWS_MODE),
                    ErrorMessages.QUALIFIER_FIELD_REQUIRES_NESTED_ROWS);
            Set<String> schemaColumnNames =
                    resolvedSchema.getColumns().stream()
                            .map(c -> c.getName())
                            .collect(Collectors.toSet());
            this.rawTableOptions.keySet().stream()
                    .filter(k -> k.endsWith(BigtableConnectorOptions.QUALIFIER_FIELD_SUFFIX)
                            && !k.equals(BigtableConnectorOptions.QUALIFIER_FIELD.key()))
                    .forEach(k -> {
                        String family = k.substring(
                                0, k.length() - BigtableConnectorOptions.QUALIFIER_FIELD_SUFFIX.length());
                        checkArgument(
                                schemaColumnNames.contains(family),
                                String.format(
                                        "Qualifier-field family '%s' not found in schema", family));
                    });
        }

        boolean hasTopLevelQualifierField =
                connectorOptions
                        .getOptional(BigtableConnectorOptions.QUALIFIER_FIELD)
                        .isPresent();
        if (hasTopLevelQualifierField) {
            checkArgument(
                    valueEncodingFormat != null, ErrorMessages.QUALIFIER_FIELD_REQUIRES_FORMAT);
            checkArgument(
                    connectorOptions.getOptional(BigtableConnectorOptions.COLUMN_FAMILY).isPresent(),
                    "qualifier-field requires column-family to be set");
            checkArgument(
                    !connectorOptions.get(BigtableConnectorOptions.USE_NESTED_ROWS_MODE),
                    "qualifier-field is incompatible with use-nested-rows-mode=true. "
                            + "Use '<family>.qualifier-field' for nested-rows-mode instead.");
            String qualifierFieldName =
                    connectorOptions.get(BigtableConnectorOptions.QUALIFIER_FIELD);
            boolean fieldExists =
                    resolvedSchema.getColumns().stream()
                            .anyMatch(col -> col.getName().equals(qualifierFieldName));
            checkArgument(
                    fieldExists,
                    String.format(
                            "Qualifier field '%s' not found in schema", qualifierFieldName));
            checkArgument(
                    !qualifierFieldName.equals(rowKeyField),
                    "qualifier-field cannot be the same as the primary key field");
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                this.resolvedSchema,
                this.connectorOptions,
                this.valueEncodingFormat,
                this.rawTableOptions);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        BigtableDynamicTableSink other = (BigtableDynamicTableSink) obj;
        return Objects.equals(resolvedSchema, other.resolvedSchema)
                && Objects.equals(connectorOptions, other.connectorOptions)
                && Objects.equals(valueEncodingFormat, other.valueEncodingFormat)
                && Objects.equals(rawTableOptions, other.rawTableOptions);
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        BigtableChangelogMode mode =
                BigtableChangelogMode.fromString(
                        connectorOptions.get(BigtableConnectorOptions.CHANGELOG_MODE));
        switch (mode) {
            case INSERT_ONLY:
                return ChangelogMode.insertOnly();
            case UPSERT:
                return ChangelogMode.upsert();
            case ALL:
                return ChangelogMode.all();
            default:
                throw new IllegalArgumentException("Unsupported changelog mode: " + mode);
        }
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        DataType physicalSchema = resolvedSchema.toPhysicalRowDataType();

        boolean upsertMode =
                BigtableChangelogMode.fromString(
                                connectorOptions.get(BigtableConnectorOptions.CHANGELOG_MODE))
                        != BigtableChangelogMode.INSERT_ONLY;

        BaseRowMutationSerializer<RowData> serializer;

        if (valueEncodingFormat != null) {
            if (connectorOptions.get(BigtableConnectorOptions.USE_NESTED_ROWS_MODE)) {
                // Format-aware path (nested rows mode)
                Map<String, SerializationSchema<RowData>> familySerializers =
                        buildFamilySerializers(context, physicalSchema);
                Map<String, QualifierConfig> qualifierConfigs =
                        parseQualifierConfigs(rawTableOptions, physicalSchema);

                serializer =
                        new FormatAwareRowMutationSerializer(
                                physicalSchema,
                                rowKeyField,
                                familySerializers,
                                qualifierConfigs,
                                upsertMode);
            } else {
                // Flat mode: single column family, entire row as one blob
                String columnFamily = connectorOptions.get(BigtableConnectorOptions.COLUMN_FAMILY);
                DataType projectedType = projectWithoutRowKey(physicalSchema, rowKeyField);
                SerializationSchema<RowData> formatSerializer =
                        valueEncodingFormat.createRuntimeEncoder(context, projectedType);
                String qualifierFieldName =
                        connectorOptions
                                .getOptional(BigtableConnectorOptions.QUALIFIER_FIELD)
                                .orElse(null);
                QualifierConfig qualifierConfig =
                        qualifierFieldName != null
                                ? resolveTopLevelQualifierField(
                                        physicalSchema, rowKeyField, qualifierFieldName)
                                : null;
                serializer =
                        FormatAwareRowMutationSerializer.forFlatMode(
                                physicalSchema,
                                rowKeyField,
                                columnFamily,
                                formatSerializer,
                                upsertMode,
                                qualifierConfig);
            }
        } else {
            // Legacy path (unchanged)
            RowDataToRowMutationSerializer.Builder serializerBuilder =
                    RowDataToRowMutationSerializer.builder()
                            .withSchema(physicalSchema)
                            .withRowKeyField(this.rowKeyField)
                            .withUpsertMode(upsertMode);

            if (connectorOptions.get(BigtableConnectorOptions.USE_NESTED_ROWS_MODE)) {
                serializerBuilder.withNestedRowsMode();
            } else {
                serializerBuilder.withColumnFamily(
                        connectorOptions.get(BigtableConnectorOptions.COLUMN_FAMILY));
            }

            serializer = serializerBuilder.build();
        }

        BigtableSink.Builder<RowData> sinkBuilder =
                BigtableSink.<RowData>builder()
                        .setProjectId(connectorOptions.get(BigtableConnectorOptions.PROJECT))
                        .setTable(connectorOptions.get(BigtableConnectorOptions.TABLE))
                        .setInstanceId(connectorOptions.get(BigtableConnectorOptions.INSTANCE))
                        .setFlowControl(connectorOptions.get(BigtableConnectorOptions.FLOW_CONTROL))
                        .setSerializer(serializer);

        if (connectorOptions.getOptional(BigtableConnectorOptions.APP_PROFILE_ID).isPresent()) {
            sinkBuilder.setAppProfileId(
                    connectorOptions.get(BigtableConnectorOptions.APP_PROFILE_ID));
        }

        Optional<GoogleCredentials> credentials =
                CredentialsFactory.builder()
                        .setAccessToken(
                                connectorOptions.get(
                                        BigtableConnectorOptions.CREDENTIALS_ACCESS_TOKEN))
                        .setCredentialsFile(
                                connectorOptions.get(BigtableConnectorOptions.CREDENTIALS_FILE))
                        .setCredentialsKey(
                                connectorOptions.get(BigtableConnectorOptions.CREDENTIALS_KEY))
                        .build()
                        .getCredentialsOr();

        if (credentials.isPresent()) {
            sinkBuilder.setCredentials(credentials.get());
        }

        final BigtableSink<RowData> bigtableSink = sinkBuilder.build();

        if (parallelism == null) {
            return SinkV2Provider.of(bigtableSink);
        }
        return SinkV2Provider.of(bigtableSink, parallelism);
    }

    @Override
    public String asSummaryString() {
        return String.format(
                "BigtableSink("
                        + "parallelism=%s, "
                        + "connectorOptions=%s, "
                        + "resolvedSchema=%s, "
                        + "rowKeyField=%s)",
                parallelism, connectorOptions, resolvedSchema, rowKeyField);
    }

    @Override
    public DynamicTableSink copy() {
        return new BigtableDynamicTableSink(
                resolvedSchema, connectorOptions, valueEncodingFormat, rawTableOptions);
    }

    /**
     * Creates one SerializationSchema per column family by projecting the physical schema to each
     * family's sub-row type.
     */
    private Map<String, SerializationSchema<RowData>> buildFamilySerializers(
            Context context, DataType physicalSchema) {
        Map<String, SerializationSchema<RowData>> result = new HashMap<>();
        for (Field field : DataType.getFields(physicalSchema)) {
            if (field.getName().equals(rowKeyField)) {
                continue;
            }
            DataType familyType = field.getDataType();
            result.put(
                    field.getName(), valueEncodingFormat.createRuntimeEncoder(context, familyType));
        }
        return result;
    }

    /**
     * Parses qualifier-field options from raw table options. Pattern:
     * '&lt;family&gt;.qualifier-field' = '&lt;fieldName&gt;'
     */
    private static Map<String, QualifierConfig> parseQualifierConfigs(
            Map<String, String> rawOptions, DataType physicalSchema) {
        Map<String, QualifierConfig> configs = new HashMap<>();
        for (Map.Entry<String, String> entry : rawOptions.entrySet()) {
            if (entry.getKey().endsWith(BigtableConnectorOptions.QUALIFIER_FIELD_SUFFIX)) {
                String family =
                        entry.getKey()
                                .substring(
                                        0,
                                        entry.getKey().length()
                                                - BigtableConnectorOptions.QUALIFIER_FIELD_SUFFIX
                                                        .length());
                String qualifierFieldName = entry.getValue();
                configs.put(
                        family, resolveQualifierField(physicalSchema, family, qualifierFieldName));
            }
        }
        return configs;
    }

    /**
     * Creates a projected DataType that excludes the row key field. Used for flat-mode format
     * serialization.
     */
    private static DataType projectWithoutRowKey(DataType physicalSchema, String rowKeyField) {
        java.util.List<DataTypes.Field> fields = new ArrayList<>();
        for (Field field : DataType.getFields(physicalSchema)) {
            if (!field.getName().equals(rowKeyField)) {
                fields.add(DataTypes.FIELD(field.getName(), field.getDataType()));
            }
        }
        return DataTypes.ROW(fields.toArray(new DataTypes.Field[0]));
    }

    /**
     * Resolves the index and type of a qualifier field within a flat (non-nested) schema. The index
     * is adjusted to account for the row key field being excluded from the projected row.
     */
    private static QualifierConfig resolveTopLevelQualifierField(
            DataType physicalSchema, String rowKeyField, String qualifierFieldName) {
        int adjustedIdx = 0;
        for (Field field : DataType.getFields(physicalSchema)) {
            if (field.getName().equals(rowKeyField)) {
                continue;
            }
            if (field.getName().equals(qualifierFieldName)) {
                return new QualifierConfig(adjustedIdx, field.getDataType().getLogicalType());
            }
            adjustedIdx++;
        }
        throw new IllegalArgumentException(
                String.format("Qualifier field '%s' not found in schema", qualifierFieldName));
    }

    /** Resolves the index and type of a qualifier field within a column family's sub-schema. */
    private static QualifierConfig resolveQualifierField(
            DataType physicalSchema, String family, String qualifierFieldName) {
        for (Field topField : DataType.getFields(physicalSchema)) {
            if (topField.getName().equals(family)) {
                int idx = 0;
                for (Field subField : DataType.getFields(topField.getDataType())) {
                    if (subField.getName().equals(qualifierFieldName)) {
                        return new QualifierConfig(idx, subField.getDataType().getLogicalType());
                    }
                    idx++;
                }
                throw new IllegalArgumentException(
                        String.format(
                                "Qualifier field '%s' not found in column family '%s'",
                                qualifierFieldName, family));
            }
        }
        throw new IllegalArgumentException(
                String.format("Column family '%s' not found in schema", family));
    }
}
