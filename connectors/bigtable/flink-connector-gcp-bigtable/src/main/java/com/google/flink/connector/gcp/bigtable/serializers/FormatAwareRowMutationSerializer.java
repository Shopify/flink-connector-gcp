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

package com.google.flink.connector.gcp.bigtable.serializers;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.table.api.DataTypes.Field;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.ProjectedRowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.types.RowKind;

import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import com.google.flink.connector.gcp.bigtable.utils.BigtableUtils;
import com.google.protobuf.ByteString;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link BaseRowMutationSerializer} implementation that delegates cell byte encoding to a
 * format-provided {@link SerializationSchema}. One serializer is created per column family from the
 * column family's sub-schema.
 *
 * <p>When a qualifier-field is configured for a column family, the field's value is used as the
 * Bigtable column qualifier and the full sub-row is serialized as the cell value. When no
 * qualifier-field is configured, the sub-row is stored under a single "payload" qualifier.
 */
public class FormatAwareRowMutationSerializer implements BaseRowMutationSerializer<RowData> {

    private static final String DEFAULT_QUALIFIER = "payload";

    private final int rowKeyIndex;
    private final LogicalTypeRoot rowKeyTypeRoot;
    private final Map<Integer, String> indexToFamily;
    private final Map<Integer, Integer> indexToArity;
    private final Map<String, SerializationSchema<RowData>> familySerializers;
    private final Map<String, QualifierConfig> qualifierConfigs;
    private final boolean upsertMode;

    // Flat-mode fields
    private final boolean flatMode;
    private final @Nullable String flatColumnFamily;
    private final @Nullable SerializationSchema<RowData> flatSerializer;
    private final @Nullable int[] flatProjection;

    /**
     * Creates a serializer for flat mode (single column family). The entire row (minus row key) is
     * serialized as a single blob under the "_value" qualifier.
     */
    public static FormatAwareRowMutationSerializer forFlatMode(
            DataType physicalDataType,
            String rowKeyField,
            String columnFamily,
            SerializationSchema<RowData> serializer,
            boolean upsertMode) {
        return new FormatAwareRowMutationSerializer(
                physicalDataType, rowKeyField, columnFamily, serializer, upsertMode);
    }

    private FormatAwareRowMutationSerializer(
            DataType physicalDataType,
            String rowKeyField,
            String columnFamily,
            SerializationSchema<RowData> serializer,
            boolean upsertMode) {
        checkNotNull(physicalDataType, "physicalDataType must not be null");
        checkNotNull(rowKeyField, "rowKeyField must not be null");
        checkNotNull(columnFamily, "columnFamily must not be null");
        checkNotNull(serializer, "serializer must not be null");

        this.upsertMode = upsertMode;
        this.flatMode = true;
        this.flatColumnFamily = columnFamily;
        this.flatSerializer = serializer;
        this.familySerializers = Collections.emptyMap();
        this.qualifierConfigs = Collections.emptyMap();
        this.indexToFamily = Collections.emptyMap();
        this.indexToArity = Collections.emptyMap();

        // Resolve row key index and build projection array
        int resolvedRowKeyIndex = -1;
        LogicalTypeRoot resolvedRowKeyTypeRoot = null;
        List<Integer> projection = new ArrayList<>();
        int index = 0;
        for (Field field : DataType.getFields(physicalDataType)) {
            if (field.getName().equals(rowKeyField)) {
                resolvedRowKeyIndex = index;
                resolvedRowKeyTypeRoot = field.getDataType().getLogicalType().getTypeRoot();
            } else {
                projection.add(index);
            }
            index++;
        }
        checkNotNull(
                resolvedRowKeyIndex >= 0 ? resolvedRowKeyIndex : null,
                String.format("Row key field '%s' not found in schema", rowKeyField));
        this.rowKeyIndex = resolvedRowKeyIndex;
        this.rowKeyTypeRoot = resolvedRowKeyTypeRoot;
        this.flatProjection = projection.stream().mapToInt(Integer::intValue).toArray();
    }

    public FormatAwareRowMutationSerializer(
            DataType physicalDataType,
            String rowKeyField,
            Map<String, SerializationSchema<RowData>> familySerializers,
            Map<String, QualifierConfig> qualifierConfigs,
            boolean upsertMode) {
        checkNotNull(physicalDataType, "physicalDataType must not be null");
        checkNotNull(rowKeyField, "rowKeyField must not be null");
        checkNotNull(familySerializers, "familySerializers must not be null");
        checkNotNull(qualifierConfigs, "qualifierConfigs must not be null");

        this.upsertMode = upsertMode;
        this.familySerializers = familySerializers;
        this.qualifierConfigs = qualifierConfigs;
        this.indexToFamily = new HashMap<>();
        this.indexToArity = new HashMap<>();

        int resolvedRowKeyIndex = -1;
        LogicalTypeRoot resolvedRowKeyTypeRoot = null;
        int index = 0;
        for (Field field : DataType.getFields(physicalDataType)) {
            if (field.getName().equals(rowKeyField)) {
                resolvedRowKeyIndex = index;
                resolvedRowKeyTypeRoot = field.getDataType().getLogicalType().getTypeRoot();
            } else {
                indexToFamily.put(index, field.getName());
                indexToArity.put(index, DataType.getFields(field.getDataType()).size());
            }
            index++;
        }
        checkNotNull(
                resolvedRowKeyIndex >= 0 ? resolvedRowKeyIndex : null,
                String.format("Row key field '%s' not found in schema", rowKeyField));
        this.rowKeyIndex = resolvedRowKeyIndex;
        this.rowKeyTypeRoot = resolvedRowKeyTypeRoot;

        this.flatMode = false;
        this.flatColumnFamily = null;
        this.flatSerializer = null;
        this.flatProjection = null;
    }

    @Override
    public void open(SerializationSchema.InitializationContext context) throws Exception {
        if (flatMode && flatSerializer != null) {
            flatSerializer.open(context);
        }
        for (SerializationSchema<RowData> serializer : familySerializers.values()) {
            serializer.open(context);
        }
    }

    @Override
    @Nullable
    public RowMutationEntry serialize(RowData record, SinkWriter.Context context) {
        if (upsertMode) {
            RowKind kind = record.getRowKind();
            if (kind == RowKind.UPDATE_BEFORE) {
                return null;
            }
            if (kind == RowKind.DELETE) {
                return serializeDelete(record);
            }
        }

        String rowKey =
                RowDataToRowMutationSerializer.extractRowKeyAsString(
                        record, rowKeyIndex, rowKeyTypeRoot);
        RowMutationEntry entry = RowMutationEntry.create(rowKey);

        if (flatMode) {
            // Flat mode: project out row key, serialize entire projected row as one blob
            ProjectedRowData projected = ProjectedRowData.from(flatProjection);
            projected.replaceRow(record);
            byte[] value = flatSerializer.serialize(projected);
            entry.setCell(
                    flatColumnFamily,
                    ByteString.copyFromUtf8(DEFAULT_QUALIFIER),
                    BigtableUtils.getTimestamp(context),
                    ByteString.copyFrom(value));
            return entry;
        }

        // Nested mode
        for (int i = 0; i < record.getArity(); i++) {
            if (i == rowKeyIndex) {
                continue;
            }
            if (record.isNullAt(i)) {
                continue;
            }

            String family = indexToFamily.get(i);
            int arity = indexToArity.get(i);
            RowData subRow = record.getRow(i, arity);
            SerializationSchema<RowData> serializer = familySerializers.get(family);

            QualifierConfig qc = qualifierConfigs.get(family);
            if (qc != null) {
                String qualifier = extractFieldAsString(subRow, qc.fieldIndex(), qc.fieldType());
                byte[] value = serializer.serialize(subRow);
                entry.setCell(
                        family,
                        ByteString.copyFromUtf8(qualifier),
                        BigtableUtils.getTimestamp(context),
                        ByteString.copyFrom(value));
            } else {
                byte[] value = serializer.serialize(subRow);
                entry.setCell(
                        family,
                        ByteString.copyFromUtf8(DEFAULT_QUALIFIER),
                        BigtableUtils.getTimestamp(context),
                        ByteString.copyFrom(value));
            }
        }
        return entry;
    }

    private RowMutationEntry serializeDelete(RowData record) {
        String rowKey =
                RowDataToRowMutationSerializer.extractRowKeyAsString(
                        record, rowKeyIndex, rowKeyTypeRoot);
        RowMutationEntry entry = RowMutationEntry.create(rowKey);

        if (flatMode) {
            entry.deleteFamily(flatColumnFamily);
            return entry;
        }

        // Nested mode: delete each column family
        for (Map.Entry<Integer, String> e : indexToFamily.entrySet()) {
            String family = e.getValue();
            QualifierConfig qc = qualifierConfigs.get(family);
            if (qc != null) {
                // Qualifier-keyed: delete only the specific cell identified by qualifier value
                int fieldIndex = e.getKey();
                if (!record.isNullAt(fieldIndex)) {
                    int arity = indexToArity.get(fieldIndex);
                    RowData subRow = record.getRow(fieldIndex, arity);
                    String qualifier =
                            extractFieldAsString(subRow, qc.fieldIndex(), qc.fieldType());
                    entry.deleteCells(family, ByteString.copyFromUtf8(qualifier));
                }
            } else {
                // No qualifier: delete entire column family
                entry.deleteFamily(family);
            }
        }
        return entry;
    }

    private static String extractFieldAsString(RowData row, int fieldIndex, LogicalType type) {
        if (type.is(LogicalTypeFamily.CHARACTER_STRING)) {
            return row.getString(fieldIndex).toString();
        } else if (type.is(LogicalTypeFamily.INTEGER_NUMERIC)) {
            switch (type.getTypeRoot()) {
                case TINYINT:
                    return String.valueOf(row.getByte(fieldIndex));
                case SMALLINT:
                    return String.valueOf(row.getShort(fieldIndex));
                case INTEGER:
                    return String.valueOf(row.getInt(fieldIndex));
                case BIGINT:
                    return String.valueOf(row.getLong(fieldIndex));
                default:
                    throw new IllegalArgumentException(
                            "Unsupported qualifier field type: " + type.getTypeRoot());
            }
        } else {
            throw new IllegalArgumentException(
                    "Qualifier field must be a string or integer type, got: " + type);
        }
    }
}
