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
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.types.RowKind;

import com.google.bigtable.v2.Mutation;
import com.google.bigtable.v2.Mutation.SetCell;
import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import com.google.protobuf.ByteString;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/** Tests for {@link FormatAwareRowMutationSerializer}. */
public class FormatAwareRowMutationSerializerTest {

    private static final long TIMESTAMP = 1704067200000L;

    private SinkWriter.Context createContext() {
        return new SinkWriter.Context() {
            @Override
            public long currentWatermark() {
                return 0;
            }

            @Override
            public Long timestamp() {
                return TIMESTAMP;
            }
        };
    }

    @Test
    public void testSerializeWithoutQualifierField() {
        DataType schema =
                DataTypes.ROW(
                        DataTypes.FIELD("rowKey", DataTypes.STRING().notNull()),
                        DataTypes.FIELD(
                                "cf1",
                                DataTypes.ROW(
                                        DataTypes.FIELD("shop_id", DataTypes.BIGINT()),
                                        DataTypes.FIELD("title", DataTypes.STRING()))));

        byte[] expectedBytes = "serialized-cf1".getBytes();
        SerializationSchema<RowData> mockSerializer = rowData -> expectedBytes;

        Map<String, SerializationSchema<RowData>> familySerializers = new HashMap<>();
        familySerializers.put("cf1", mockSerializer);

        FormatAwareRowMutationSerializer serializer =
                new FormatAwareRowMutationSerializer(
                        schema, "rowKey", familySerializers, Collections.emptyMap(), false);

        GenericRowData cf1Row = new GenericRowData(2);
        cf1Row.setField(0, 42L);
        cf1Row.setField(1, StringData.fromString("T-Shirt"));

        GenericRowData record = new GenericRowData(2);
        record.setField(0, StringData.fromString("row1"));
        record.setField(1, cf1Row);

        RowMutationEntry entry = serializer.serialize(record, createContext());

        assertEquals(1, entry.toProto().getMutationsCount());
        SetCell cell = entry.toProto().getMutations(0).getSetCell();
        assertEquals("cf1", cell.getFamilyName());
        assertEquals(ByteString.copyFromUtf8("payload"), cell.getColumnQualifier());
        assertEquals(ByteString.copyFrom(expectedBytes), cell.getValue());
    }

    @Test
    public void testSerializeWithQualifierField() {
        DataType schema =
                DataTypes.ROW(
                        DataTypes.FIELD("rowKey", DataTypes.STRING().notNull()),
                        DataTypes.FIELD(
                                "publications",
                                DataTypes.ROW(
                                        DataTypes.FIELD("id", DataTypes.BIGINT()),
                                        DataTypes.FIELD("channel", DataTypes.STRING()))));

        byte[] expectedBytes = "serialized-pub".getBytes();
        SerializationSchema<RowData> mockSerializer = rowData -> expectedBytes;

        Map<String, SerializationSchema<RowData>> familySerializers = new HashMap<>();
        familySerializers.put("publications", mockSerializer);

        Map<String, QualifierConfig> qualifierConfigs = new HashMap<>();
        qualifierConfigs.put("publications", new QualifierConfig(0, new BigIntType()));

        FormatAwareRowMutationSerializer serializer =
                new FormatAwareRowMutationSerializer(
                        schema, "rowKey", familySerializers, qualifierConfigs, false);

        GenericRowData pubRow = new GenericRowData(2);
        pubRow.setField(0, 5001L);
        pubRow.setField(1, StringData.fromString("online_store"));

        GenericRowData record = new GenericRowData(2);
        record.setField(0, StringData.fromString("row1"));
        record.setField(1, pubRow);

        RowMutationEntry entry = serializer.serialize(record, createContext());

        assertEquals(1, entry.toProto().getMutationsCount());
        SetCell cell = entry.toProto().getMutations(0).getSetCell();
        assertEquals("publications", cell.getFamilyName());
        assertEquals(ByteString.copyFromUtf8("5001"), cell.getColumnQualifier());
        assertEquals(ByteString.copyFrom(expectedBytes), cell.getValue());
    }

    @Test
    public void testSerializeSkipsNullFamilies() {
        DataType schema =
                DataTypes.ROW(
                        DataTypes.FIELD("rowKey", DataTypes.STRING().notNull()),
                        DataTypes.FIELD(
                                "cf1", DataTypes.ROW(DataTypes.FIELD("f1", DataTypes.STRING()))),
                        DataTypes.FIELD(
                                "cf2", DataTypes.ROW(DataTypes.FIELD("f2", DataTypes.STRING()))));

        byte[] bytes1 = "cf1-bytes".getBytes();
        byte[] bytes2 = "cf2-bytes".getBytes();

        Map<String, SerializationSchema<RowData>> familySerializers = new HashMap<>();
        familySerializers.put("cf1", rowData -> bytes1);
        familySerializers.put("cf2", rowData -> bytes2);

        FormatAwareRowMutationSerializer serializer =
                new FormatAwareRowMutationSerializer(
                        schema, "rowKey", familySerializers, Collections.emptyMap(), false);

        GenericRowData cf1Row = new GenericRowData(1);
        cf1Row.setField(0, StringData.fromString("value1"));

        GenericRowData record = new GenericRowData(3);
        record.setField(0, StringData.fromString("row1"));
        record.setField(1, cf1Row);
        record.setField(2, null);

        RowMutationEntry entry = serializer.serialize(record, createContext());

        assertEquals(1, entry.toProto().getMutationsCount());
        assertEquals("cf1", entry.toProto().getMutations(0).getSetCell().getFamilyName());
    }

    @Test
    public void testSerializeMultipleFamiliesMixed() {
        DataType schema =
                DataTypes.ROW(
                        DataTypes.FIELD("rowKey", DataTypes.STRING().notNull()),
                        DataTypes.FIELD(
                                "product",
                                DataTypes.ROW(
                                        DataTypes.FIELD("shop_id", DataTypes.BIGINT()),
                                        DataTypes.FIELD("title", DataTypes.STRING()))),
                        DataTypes.FIELD(
                                "publications",
                                DataTypes.ROW(
                                        DataTypes.FIELD("id", DataTypes.BIGINT()),
                                        DataTypes.FIELD("channel", DataTypes.STRING()))));

        byte[] productBytes = "product-bytes".getBytes();
        byte[] pubBytes = "pub-bytes".getBytes();

        Map<String, SerializationSchema<RowData>> familySerializers = new HashMap<>();
        familySerializers.put("product", rowData -> productBytes);
        familySerializers.put("publications", rowData -> pubBytes);

        Map<String, QualifierConfig> qualifierConfigs = new HashMap<>();
        qualifierConfigs.put("publications", new QualifierConfig(0, new BigIntType()));

        FormatAwareRowMutationSerializer serializer =
                new FormatAwareRowMutationSerializer(
                        schema, "rowKey", familySerializers, qualifierConfigs, false);

        GenericRowData productRow = new GenericRowData(2);
        productRow.setField(0, 7L);
        productRow.setField(1, StringData.fromString("T-Shirt"));

        GenericRowData pubRow = new GenericRowData(2);
        pubRow.setField(0, 5001L);
        pubRow.setField(1, StringData.fromString("online_store"));

        GenericRowData record = new GenericRowData(3);
        record.setField(0, StringData.fromString("1001"));
        record.setField(1, productRow);
        record.setField(2, pubRow);

        RowMutationEntry entry = serializer.serialize(record, createContext());

        assertEquals(2, entry.toProto().getMutationsCount());

        SetCell productCell = entry.toProto().getMutations(0).getSetCell();
        assertEquals("product", productCell.getFamilyName());
        assertEquals(ByteString.copyFromUtf8("payload"), productCell.getColumnQualifier());
        assertEquals(ByteString.copyFrom(productBytes), productCell.getValue());

        SetCell pubCell = entry.toProto().getMutations(1).getSetCell();
        assertEquals("publications", pubCell.getFamilyName());
        assertEquals(ByteString.copyFromUtf8("5001"), pubCell.getColumnQualifier());
        assertEquals(ByteString.copyFrom(pubBytes), pubCell.getValue());
    }

    @Test
    public void testSerializeFlatModeWithFormat() {
        // Schema: rowKey STRING, shop_id BIGINT, title STRING
        DataType schema =
                DataTypes.ROW(
                        DataTypes.FIELD("rowKey", DataTypes.STRING().notNull()),
                        DataTypes.FIELD("shop_id", DataTypes.BIGINT()),
                        DataTypes.FIELD("title", DataTypes.STRING()));

        byte[] expectedBytes = "flat-bytes".getBytes();
        // This mock serializer receives a projected RowData (without rowKey)
        SerializationSchema<RowData> mockSerializer = rowData -> expectedBytes;

        FormatAwareRowMutationSerializer serializer =
                FormatAwareRowMutationSerializer.forFlatMode(
                        schema, "rowKey", "product", mockSerializer, false, null);

        GenericRowData record = new GenericRowData(3);
        record.setField(0, StringData.fromString("row1"));
        record.setField(1, 42L);
        record.setField(2, StringData.fromString("T-Shirt"));

        RowMutationEntry entry = serializer.serialize(record, createContext());

        assertEquals(1, entry.toProto().getMutationsCount());
        SetCell cell = entry.toProto().getMutations(0).getSetCell();
        assertEquals("product", cell.getFamilyName());
        assertEquals(ByteString.copyFromUtf8("payload"), cell.getColumnQualifier());
        assertEquals(ByteString.copyFrom(expectedBytes), cell.getValue());
    }

    @Test
    public void testSerializeFlatModeWithRowKeyNotAtIndexZero() {
        // Row key is the last field, not the first
        DataType schema =
                DataTypes.ROW(
                        DataTypes.FIELD("shop_id", DataTypes.BIGINT()),
                        DataTypes.FIELD("title", DataTypes.STRING()),
                        DataTypes.FIELD("rowKey", DataTypes.STRING().notNull()));

        byte[] expectedBytes = "projected-bytes".getBytes();
        SerializationSchema<RowData> mockSerializer = rowData -> expectedBytes;

        FormatAwareRowMutationSerializer serializer =
                FormatAwareRowMutationSerializer.forFlatMode(
                        schema, "rowKey", "product", mockSerializer, false, null);

        GenericRowData record = new GenericRowData(3);
        record.setField(0, 42L);
        record.setField(1, StringData.fromString("T-Shirt"));
        record.setField(2, StringData.fromString("row1"));

        RowMutationEntry entry = serializer.serialize(record, createContext());

        assertEquals(1, entry.toProto().getMutationsCount());
        SetCell cell = entry.toProto().getMutations(0).getSetCell();
        assertEquals("product", cell.getFamilyName());
        assertEquals(ByteString.copyFromUtf8("payload"), cell.getColumnQualifier());
        assertEquals(ByteString.copyFrom(expectedBytes), cell.getValue());
    }

    @Test
    public void testSerializeNestedModeWithRowKeyNotAtIndexZero() {
        // Row key is in the middle of the schema
        DataType schema =
                DataTypes.ROW(
                        DataTypes.FIELD(
                                "cf1",
                                DataTypes.ROW(
                                        DataTypes.FIELD("shop_id", DataTypes.BIGINT()),
                                        DataTypes.FIELD("title", DataTypes.STRING()))),
                        DataTypes.FIELD("rowKey", DataTypes.STRING().notNull()),
                        DataTypes.FIELD(
                                "cf2",
                                DataTypes.ROW(
                                        DataTypes.FIELD("city", DataTypes.STRING()))));

        byte[] cf1Bytes = "cf1-bytes".getBytes();
        byte[] cf2Bytes = "cf2-bytes".getBytes();

        Map<String, SerializationSchema<RowData>> familySerializers = new HashMap<>();
        familySerializers.put("cf1", rowData -> cf1Bytes);
        familySerializers.put("cf2", rowData -> cf2Bytes);

        FormatAwareRowMutationSerializer serializer =
                new FormatAwareRowMutationSerializer(
                        schema, "rowKey", familySerializers, Collections.emptyMap(), false);

        GenericRowData cf1Row = new GenericRowData(2);
        cf1Row.setField(0, 42L);
        cf1Row.setField(1, StringData.fromString("T-Shirt"));

        GenericRowData cf2Row = new GenericRowData(1);
        cf2Row.setField(0, StringData.fromString("NYC"));

        GenericRowData record = new GenericRowData(3);
        record.setField(0, cf1Row);
        record.setField(1, StringData.fromString("row1"));
        record.setField(2, cf2Row);

        RowMutationEntry entry = serializer.serialize(record, createContext());

        assertEquals(2, entry.toProto().getMutationsCount());

        java.util.Set<String> families = new java.util.HashSet<>();
        for (int i = 0; i < entry.toProto().getMutationsCount(); i++) {
            families.add(entry.toProto().getMutations(i).getSetCell().getFamilyName());
        }
        assertTrue(families.contains("cf1"));
        assertTrue(families.contains("cf2"));
    }

    @Test
    public void testSerializeWithStringQualifierField() {
        DataType schema =
                DataTypes.ROW(
                        DataTypes.FIELD("rowKey", DataTypes.STRING().notNull()),
                        DataTypes.FIELD(
                                "events",
                                DataTypes.ROW(
                                        DataTypes.FIELD("event_name", DataTypes.STRING()),
                                        DataTypes.FIELD("payload", DataTypes.STRING()))));

        byte[] bytes = "event-bytes".getBytes();

        Map<String, SerializationSchema<RowData>> familySerializers = new HashMap<>();
        familySerializers.put("events", rowData -> bytes);

        Map<String, QualifierConfig> qualifierConfigs = new HashMap<>();
        qualifierConfigs.put("events", new QualifierConfig(0, DataTypes.STRING().getLogicalType()));

        FormatAwareRowMutationSerializer serializer =
                new FormatAwareRowMutationSerializer(
                        schema, "rowKey", familySerializers, qualifierConfigs, false);

        GenericRowData eventRow = new GenericRowData(2);
        eventRow.setField(0, StringData.fromString("click"));
        eventRow.setField(1, StringData.fromString("{}"));

        GenericRowData record = new GenericRowData(2);
        record.setField(0, StringData.fromString("user1"));
        record.setField(1, eventRow);

        RowMutationEntry entry = serializer.serialize(record, createContext());

        SetCell cell = entry.toProto().getMutations(0).getSetCell();
        assertEquals("events", cell.getFamilyName());
        assertEquals(ByteString.copyFromUtf8("click"), cell.getColumnQualifier());
    }

    @Test
    public void testUpdateBeforeReturnsNullInUpsertMode() {
        DataType schema =
                DataTypes.ROW(
                        DataTypes.FIELD("rowKey", DataTypes.STRING().notNull()),
                        DataTypes.FIELD(
                                "cf1",
                                DataTypes.ROW(
                                        DataTypes.FIELD("shop_id", DataTypes.BIGINT()),
                                        DataTypes.FIELD("title", DataTypes.STRING()))));

        Map<String, SerializationSchema<RowData>> familySerializers = new HashMap<>();
        familySerializers.put("cf1", rowData -> "bytes".getBytes());

        FormatAwareRowMutationSerializer serializer =
                new FormatAwareRowMutationSerializer(
                        schema, "rowKey", familySerializers, Collections.emptyMap(), true);

        GenericRowData cf1Row = new GenericRowData(2);
        cf1Row.setField(0, 42L);
        cf1Row.setField(1, StringData.fromString("T-Shirt"));

        GenericRowData record = new GenericRowData(2);
        record.setField(0, StringData.fromString("row1"));
        record.setField(1, cf1Row);
        record.setRowKind(RowKind.UPDATE_BEFORE);

        assertNull(serializer.serialize(record, createContext()));
    }

    @Test
    public void testInsertStillWorksInUpsertMode() {
        DataType schema =
                DataTypes.ROW(
                        DataTypes.FIELD("rowKey", DataTypes.STRING().notNull()),
                        DataTypes.FIELD(
                                "cf1",
                                DataTypes.ROW(
                                        DataTypes.FIELD("shop_id", DataTypes.BIGINT()),
                                        DataTypes.FIELD("title", DataTypes.STRING()))));

        byte[] expectedBytes = "serialized".getBytes();
        Map<String, SerializationSchema<RowData>> familySerializers = new HashMap<>();
        familySerializers.put("cf1", rowData -> expectedBytes);

        FormatAwareRowMutationSerializer serializer =
                new FormatAwareRowMutationSerializer(
                        schema, "rowKey", familySerializers, Collections.emptyMap(), true);

        GenericRowData cf1Row = new GenericRowData(2);
        cf1Row.setField(0, 42L);
        cf1Row.setField(1, StringData.fromString("T-Shirt"));

        GenericRowData record = new GenericRowData(2);
        record.setField(0, StringData.fromString("row1"));
        record.setField(1, cf1Row);
        record.setRowKind(RowKind.INSERT);

        RowMutationEntry entry = serializer.serialize(record, createContext());
        assertEquals(1, entry.toProto().getMutationsCount());
        assertEquals("cf1", entry.toProto().getMutations(0).getSetCell().getFamilyName());
    }

    @Test
    public void testDeleteNestedModeWithoutQualifierField() {
        DataType schema =
                DataTypes.ROW(
                        DataTypes.FIELD("rowKey", DataTypes.STRING().notNull()),
                        DataTypes.FIELD(
                                "cf1", DataTypes.ROW(DataTypes.FIELD("f1", DataTypes.STRING()))),
                        DataTypes.FIELD(
                                "cf2", DataTypes.ROW(DataTypes.FIELD("f2", DataTypes.STRING()))));

        Map<String, SerializationSchema<RowData>> familySerializers = new HashMap<>();
        familySerializers.put("cf1", rowData -> "bytes".getBytes());
        familySerializers.put("cf2", rowData -> "bytes".getBytes());

        FormatAwareRowMutationSerializer serializer =
                new FormatAwareRowMutationSerializer(
                        schema, "rowKey", familySerializers, Collections.emptyMap(), true);

        GenericRowData record = new GenericRowData(3);
        record.setField(0, StringData.fromString("row1"));
        record.setField(1, null);
        record.setField(2, null);
        record.setRowKind(RowKind.DELETE);

        RowMutationEntry entry = serializer.serialize(record, createContext());

        // Should produce deleteFamily mutations for cf1 and cf2
        assertEquals(2, entry.toProto().getMutationsCount());

        // Collect family names from mutations (order may vary due to HashMap)
        java.util.Set<String> deletedFamilies = new java.util.HashSet<>();
        for (int i = 0; i < entry.toProto().getMutationsCount(); i++) {
            assertTrue(entry.toProto().getMutations(i).hasDeleteFromFamily());
            deletedFamilies.add(
                    entry.toProto().getMutations(i).getDeleteFromFamily().getFamilyName());
        }
        assertTrue(deletedFamilies.contains("cf1"));
        assertTrue(deletedFamilies.contains("cf2"));
    }

    @Test
    public void testDeleteWithQualifierFieldDeletesSpecificCell() {
        DataType schema =
                DataTypes.ROW(
                        DataTypes.FIELD("rowKey", DataTypes.STRING().notNull()),
                        DataTypes.FIELD(
                                "product",
                                DataTypes.ROW(
                                        DataTypes.FIELD("shop_id", DataTypes.BIGINT()),
                                        DataTypes.FIELD("title", DataTypes.STRING()))),
                        DataTypes.FIELD(
                                "publications",
                                DataTypes.ROW(
                                        DataTypes.FIELD("id", DataTypes.BIGINT()),
                                        DataTypes.FIELD("channel", DataTypes.STRING()))));

        Map<String, SerializationSchema<RowData>> familySerializers = new HashMap<>();
        familySerializers.put("product", rowData -> "bytes".getBytes());
        familySerializers.put("publications", rowData -> "bytes".getBytes());

        Map<String, QualifierConfig> qualifierConfigs = new HashMap<>();
        qualifierConfigs.put("publications", new QualifierConfig(0, new BigIntType()));

        FormatAwareRowMutationSerializer serializer =
                new FormatAwareRowMutationSerializer(
                        schema, "rowKey", familySerializers, qualifierConfigs, true);

        // DELETE: product is null, publications has id=5001
        GenericRowData pubRow = new GenericRowData(2);
        pubRow.setField(0, 5001L);
        pubRow.setField(1, StringData.fromString("online_store"));

        GenericRowData record = new GenericRowData(3);
        record.setField(0, StringData.fromString("1001"));
        record.setField(1, null);
        record.setField(2, pubRow);
        record.setRowKind(RowKind.DELETE);

        RowMutationEntry entry = serializer.serialize(record, createContext());

        // product: deleteFamily (no qualifier config)
        // publications: deleteCells (has qualifier config, sub-row is non-null)
        assertEquals(2, entry.toProto().getMutationsCount());

        // Find each mutation by type since HashMap ordering is non-deterministic
        Mutation productMutation = null;
        Mutation pubMutation = null;
        for (int i = 0; i < entry.toProto().getMutationsCount(); i++) {
            Mutation m = entry.toProto().getMutations(i);
            if (m.hasDeleteFromFamily()) {
                productMutation = m;
            } else if (m.hasDeleteFromColumn()) {
                pubMutation = m;
            }
        }

        assertNotNull(productMutation);
        assertEquals("product", productMutation.getDeleteFromFamily().getFamilyName());

        assertNotNull(pubMutation);
        assertEquals("publications", pubMutation.getDeleteFromColumn().getFamilyName());
        assertEquals(
                ByteString.copyFromUtf8("5001"),
                pubMutation.getDeleteFromColumn().getColumnQualifier());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDeleteNestedModeNullSubRowWithQualifierFieldThrows() {
        DataType schema =
                DataTypes.ROW(
                        DataTypes.FIELD("rowKey", DataTypes.STRING().notNull()),
                        DataTypes.FIELD(
                                "product",
                                DataTypes.ROW(
                                        DataTypes.FIELD("id", DataTypes.BIGINT()),
                                        DataTypes.FIELD("title", DataTypes.STRING()))));

        Map<String, SerializationSchema<RowData>> familySerializers = new HashMap<>();
        familySerializers.put("product", rowData -> "bytes".getBytes());

        Map<String, QualifierConfig> qualifierConfigs = new HashMap<>();
        qualifierConfigs.put("product", new QualifierConfig(0, new BigIntType()));

        FormatAwareRowMutationSerializer serializer =
                new FormatAwareRowMutationSerializer(
                        schema, "rowKey", familySerializers, qualifierConfigs, true);

        GenericRowData record = new GenericRowData(2);
        record.setField(0, StringData.fromString("row1"));
        record.setField(1, null); // null sub-row with qualifier-field configured
        record.setRowKind(RowKind.DELETE);

        serializer.serialize(record, createContext());
    }

    @Test
    public void testDeleteFlatMode() {
        DataType schema =
                DataTypes.ROW(
                        DataTypes.FIELD("rowKey", DataTypes.STRING().notNull()),
                        DataTypes.FIELD("shop_id", DataTypes.BIGINT()),
                        DataTypes.FIELD("title", DataTypes.STRING()));

        FormatAwareRowMutationSerializer serializer =
                FormatAwareRowMutationSerializer.forFlatMode(
                        schema, "rowKey", "product", rowData -> "bytes".getBytes(), true, null);

        GenericRowData record = new GenericRowData(3);
        record.setField(0, StringData.fromString("row1"));
        record.setField(1, 42L);
        record.setField(2, StringData.fromString("T-Shirt"));
        record.setRowKind(RowKind.DELETE);

        RowMutationEntry entry = serializer.serialize(record, createContext());

        assertEquals(1, entry.toProto().getMutationsCount());
        assertTrue(entry.toProto().getMutations(0).hasDeleteFromFamily());
        assertEquals(
                "product", entry.toProto().getMutations(0).getDeleteFromFamily().getFamilyName());
    }

    @Test
    public void testNonUpsertModeIgnoresRowKind() {
        DataType schema =
                DataTypes.ROW(
                        DataTypes.FIELD("rowKey", DataTypes.STRING().notNull()),
                        DataTypes.FIELD(
                                "cf1", DataTypes.ROW(DataTypes.FIELD("f1", DataTypes.STRING()))));

        byte[] expectedBytes = "bytes".getBytes();
        Map<String, SerializationSchema<RowData>> familySerializers = new HashMap<>();
        familySerializers.put("cf1", rowData -> expectedBytes);

        // upsertMode = false
        FormatAwareRowMutationSerializer serializer =
                new FormatAwareRowMutationSerializer(
                        schema, "rowKey", familySerializers, Collections.emptyMap(), false);

        GenericRowData cf1Row = new GenericRowData(1);
        cf1Row.setField(0, StringData.fromString("value"));

        GenericRowData record = new GenericRowData(2);
        record.setField(0, StringData.fromString("row1"));
        record.setField(1, cf1Row);
        record.setRowKind(RowKind.DELETE); // should be ignored in non-upsert mode

        RowMutationEntry entry = serializer.serialize(record, createContext());

        // Should produce a normal setCell, not a delete
        assertNotNull(entry);
        assertEquals(1, entry.toProto().getMutationsCount());
        assertTrue(entry.toProto().getMutations(0).hasSetCell());
    }

    @Test
    public void testSerializeFlatModeWithQualifierField() {
        // Schema: rowKey STRING, product_id STRING, name STRING, price BIGINT
        DataType schema =
                DataTypes.ROW(
                        DataTypes.FIELD("rowKey", DataTypes.STRING().notNull()),
                        DataTypes.FIELD("product_id", DataTypes.STRING()),
                        DataTypes.FIELD("name", DataTypes.STRING()),
                        DataTypes.FIELD("price", DataTypes.BIGINT()));

        byte[] expectedBytes = "flat-qualifier-bytes".getBytes();
        SerializationSchema<RowData> mockSerializer = rowData -> expectedBytes;

        // product_id is at adjusted index 0 (after row key exclusion)
        QualifierConfig qualifierConfig =
                new QualifierConfig(0, DataTypes.STRING().getLogicalType());

        FormatAwareRowMutationSerializer serializer =
                FormatAwareRowMutationSerializer.forFlatMode(
                        schema, "rowKey", "products", mockSerializer, false,
                        qualifierConfig);

        GenericRowData record = new GenericRowData(4);
        record.setField(0, StringData.fromString("shop1"));
        record.setField(1, StringData.fromString("prod-42"));
        record.setField(2, StringData.fromString("T-Shirt"));
        record.setField(3, 2999L);

        RowMutationEntry entry = serializer.serialize(record, createContext());

        assertEquals(1, entry.toProto().getMutationsCount());
        SetCell cell = entry.toProto().getMutations(0).getSetCell();
        assertEquals("products", cell.getFamilyName());
        assertEquals(ByteString.copyFromUtf8("prod-42"), cell.getColumnQualifier());
        assertEquals(ByteString.copyFrom(expectedBytes), cell.getValue());
    }

    @Test
    public void testDeleteFlatModeWithQualifierField() {
        DataType schema =
                DataTypes.ROW(
                        DataTypes.FIELD("rowKey", DataTypes.STRING().notNull()),
                        DataTypes.FIELD("product_id", DataTypes.STRING()),
                        DataTypes.FIELD("name", DataTypes.STRING()),
                        DataTypes.FIELD("price", DataTypes.BIGINT()));

        QualifierConfig qualifierConfig =
                new QualifierConfig(0, DataTypes.STRING().getLogicalType());

        FormatAwareRowMutationSerializer serializer =
                FormatAwareRowMutationSerializer.forFlatMode(
                        schema, "rowKey", "products",
                        rowData -> "bytes".getBytes(), true, qualifierConfig);

        GenericRowData record = new GenericRowData(4);
        record.setField(0, StringData.fromString("shop1"));
        record.setField(1, StringData.fromString("prod-42"));
        record.setField(2, StringData.fromString("T-Shirt"));
        record.setField(3, 2999L);
        record.setRowKind(RowKind.DELETE);

        RowMutationEntry entry = serializer.serialize(record, createContext());

        assertEquals(1, entry.toProto().getMutationsCount());
        assertTrue(entry.toProto().getMutations(0).hasDeleteFromColumn());
        assertEquals(
                "products",
                entry.toProto().getMutations(0).getDeleteFromColumn().getFamilyName());
        assertEquals(
                ByteString.copyFromUtf8("prod-42"),
                entry.toProto().getMutations(0).getDeleteFromColumn().getColumnQualifier());
    }

    @Test
    public void testSerializeFlatModeWithIntegerQualifierField() {
        DataType schema =
                DataTypes.ROW(
                        DataTypes.FIELD("rowKey", DataTypes.STRING().notNull()),
                        DataTypes.FIELD("product_id", DataTypes.BIGINT()),
                        DataTypes.FIELD("name", DataTypes.STRING()));

        byte[] expectedBytes = "int-qualifier-bytes".getBytes();
        SerializationSchema<RowData> mockSerializer = rowData -> expectedBytes;

        QualifierConfig qualifierConfig =
                new QualifierConfig(0, new BigIntType());

        FormatAwareRowMutationSerializer serializer =
                FormatAwareRowMutationSerializer.forFlatMode(
                        schema, "rowKey", "products", mockSerializer, false,
                        qualifierConfig);

        GenericRowData record = new GenericRowData(3);
        record.setField(0, StringData.fromString("shop1"));
        record.setField(1, 42L);
        record.setField(2, StringData.fromString("T-Shirt"));

        RowMutationEntry entry = serializer.serialize(record, createContext());

        assertEquals(1, entry.toProto().getMutationsCount());
        SetCell cell = entry.toProto().getMutations(0).getSetCell();
        assertEquals("products", cell.getFamilyName());
        assertEquals(ByteString.copyFromUtf8("42"), cell.getColumnQualifier());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNullQualifierFieldThrowsInNestedMode() {
        DataType schema =
                DataTypes.ROW(
                        DataTypes.FIELD("rowKey", DataTypes.STRING().notNull()),
                        DataTypes.FIELD(
                                "events",
                                DataTypes.ROW(
                                        DataTypes.FIELD("event_name", DataTypes.STRING()),
                                        DataTypes.FIELD("payload", DataTypes.STRING()))));

        Map<String, SerializationSchema<RowData>> familySerializers = new HashMap<>();
        familySerializers.put("events", rowData -> "bytes".getBytes());

        Map<String, QualifierConfig> qualifierConfigs = new HashMap<>();
        qualifierConfigs.put("events", new QualifierConfig(0, DataTypes.STRING().getLogicalType()));

        FormatAwareRowMutationSerializer serializer =
                new FormatAwareRowMutationSerializer(
                        schema, "rowKey", familySerializers, qualifierConfigs, false);

        GenericRowData eventRow = new GenericRowData(2);
        eventRow.setField(0, null); // null qualifier field
        eventRow.setField(1, StringData.fromString("{}"));

        GenericRowData record = new GenericRowData(2);
        record.setField(0, StringData.fromString("user1"));
        record.setField(1, eventRow);

        serializer.serialize(record, createContext());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNullQualifierFieldThrowsInFlatMode() {
        DataType schema =
                DataTypes.ROW(
                        DataTypes.FIELD("rowKey", DataTypes.STRING().notNull()),
                        DataTypes.FIELD("product_id", DataTypes.STRING()),
                        DataTypes.FIELD("name", DataTypes.STRING()));

        QualifierConfig qualifierConfig =
                new QualifierConfig(0, DataTypes.STRING().getLogicalType());

        FormatAwareRowMutationSerializer serializer =
                FormatAwareRowMutationSerializer.forFlatMode(
                        schema, "rowKey", "products",
                        rowData -> "bytes".getBytes(), false, qualifierConfig);

        GenericRowData record = new GenericRowData(3);
        record.setField(0, StringData.fromString("shop1"));
        record.setField(1, null); // null qualifier field
        record.setField(2, StringData.fromString("T-Shirt"));

        serializer.serialize(record, createContext());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidRowKeyFieldThrowsInFlatMode() {
        DataType schema =
                DataTypes.ROW(
                        DataTypes.FIELD("rowKey", DataTypes.STRING().notNull()),
                        DataTypes.FIELD("name", DataTypes.STRING()));

        FormatAwareRowMutationSerializer.forFlatMode(
                schema, "nonExistentKey", "product",
                rowData -> "bytes".getBytes(), false, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidRowKeyFieldThrowsInNestedMode() {
        DataType schema =
                DataTypes.ROW(
                        DataTypes.FIELD("rowKey", DataTypes.STRING().notNull()),
                        DataTypes.FIELD(
                                "cf1", DataTypes.ROW(DataTypes.FIELD("f1", DataTypes.STRING()))));

        Map<String, SerializationSchema<RowData>> familySerializers = new HashMap<>();
        familySerializers.put("cf1", rowData -> "bytes".getBytes());

        new FormatAwareRowMutationSerializer(
                schema, "nonExistentKey", familySerializers, Collections.emptyMap(), false);
    }

    // --- Flat mode constructor null checks ---

    @Test(expected = NullPointerException.class)
    public void testFlatModeNullSchemaThrows() {
        FormatAwareRowMutationSerializer.forFlatMode(
                null, "rowKey", "cf1", rowData -> "bytes".getBytes(), false, null);
    }

    @Test(expected = NullPointerException.class)
    public void testFlatModeNullRowKeyFieldThrows() {
        DataType schema =
                DataTypes.ROW(
                        DataTypes.FIELD("rowKey", DataTypes.STRING().notNull()),
                        DataTypes.FIELD("name", DataTypes.STRING()));

        FormatAwareRowMutationSerializer.forFlatMode(
                schema, null, "cf1", rowData -> "bytes".getBytes(), false, null);
    }

    @Test(expected = NullPointerException.class)
    public void testFlatModeNullColumnFamilyThrows() {
        DataType schema =
                DataTypes.ROW(
                        DataTypes.FIELD("rowKey", DataTypes.STRING().notNull()),
                        DataTypes.FIELD("name", DataTypes.STRING()));

        FormatAwareRowMutationSerializer.forFlatMode(
                schema, "rowKey", null, rowData -> "bytes".getBytes(), false, null);
    }

    @Test(expected = NullPointerException.class)
    public void testFlatModeNullSerializerThrows() {
        DataType schema =
                DataTypes.ROW(
                        DataTypes.FIELD("rowKey", DataTypes.STRING().notNull()),
                        DataTypes.FIELD("name", DataTypes.STRING()));

        FormatAwareRowMutationSerializer.forFlatMode(
                schema, "rowKey", "cf1", null, false, null);
    }

    // --- Nested mode constructor null checks ---

    @Test(expected = NullPointerException.class)
    public void testNestedModeNullSchemaThrows() {
        new FormatAwareRowMutationSerializer(
                null, "rowKey", new HashMap<>(), Collections.emptyMap(), false);
    }

    @Test(expected = NullPointerException.class)
    public void testNestedModeNullRowKeyFieldThrows() {
        DataType schema =
                DataTypes.ROW(
                        DataTypes.FIELD("rowKey", DataTypes.STRING().notNull()),
                        DataTypes.FIELD(
                                "cf1", DataTypes.ROW(DataTypes.FIELD("f1", DataTypes.STRING()))));

        new FormatAwareRowMutationSerializer(
                schema, null, new HashMap<>(), Collections.emptyMap(), false);
    }

    @Test(expected = NullPointerException.class)
    public void testNestedModeNullFamilySerializersThrows() {
        DataType schema =
                DataTypes.ROW(
                        DataTypes.FIELD("rowKey", DataTypes.STRING().notNull()),
                        DataTypes.FIELD(
                                "cf1", DataTypes.ROW(DataTypes.FIELD("f1", DataTypes.STRING()))));

        new FormatAwareRowMutationSerializer(
                schema, "rowKey", null, Collections.emptyMap(), false);
    }

    @Test(expected = NullPointerException.class)
    public void testNestedModeNullQualifierConfigsThrows() {
        DataType schema =
                DataTypes.ROW(
                        DataTypes.FIELD("rowKey", DataTypes.STRING().notNull()),
                        DataTypes.FIELD(
                                "cf1", DataTypes.ROW(DataTypes.FIELD("f1", DataTypes.STRING()))));

        Map<String, SerializationSchema<RowData>> familySerializers = new HashMap<>();
        familySerializers.put("cf1", rowData -> "bytes".getBytes());

        new FormatAwareRowMutationSerializer(
                schema, "rowKey", familySerializers, null, false);
    }

    @Test
    public void testOpenDelegatesToFlatSerializer() throws Exception {
        DataType schema =
                DataTypes.ROW(
                        DataTypes.FIELD("rowKey", DataTypes.STRING().notNull()),
                        DataTypes.FIELD("name", DataTypes.STRING()));

        AtomicBoolean opened = new AtomicBoolean(false);
        SerializationSchema<RowData> trackingSerializer =
                new SerializationSchema<RowData>() {
                    @Override
                    public void open(InitializationContext context) {
                        opened.set(true);
                    }

                    @Override
                    public byte[] serialize(RowData element) {
                        return new byte[0];
                    }
                };

        FormatAwareRowMutationSerializer serializer =
                FormatAwareRowMutationSerializer.forFlatMode(
                        schema, "rowKey", "cf1", trackingSerializer, false, null);

        serializer.open(null);

        assertTrue("open() should delegate to flat serializer", opened.get());
    }

    @Test
    public void testOpenDelegatesToAllFamilySerializers() throws Exception {
        DataType schema =
                DataTypes.ROW(
                        DataTypes.FIELD("rowKey", DataTypes.STRING().notNull()),
                        DataTypes.FIELD(
                                "cf1", DataTypes.ROW(DataTypes.FIELD("f1", DataTypes.STRING()))),
                        DataTypes.FIELD(
                                "cf2", DataTypes.ROW(DataTypes.FIELD("f2", DataTypes.STRING()))));

        AtomicBoolean cf1Opened = new AtomicBoolean(false);
        AtomicBoolean cf2Opened = new AtomicBoolean(false);

        SerializationSchema<RowData> cf1Serializer =
                new SerializationSchema<RowData>() {
                    @Override
                    public void open(InitializationContext context) {
                        cf1Opened.set(true);
                    }

                    @Override
                    public byte[] serialize(RowData element) {
                        return new byte[0];
                    }
                };

        SerializationSchema<RowData> cf2Serializer =
                new SerializationSchema<RowData>() {
                    @Override
                    public void open(InitializationContext context) {
                        cf2Opened.set(true);
                    }

                    @Override
                    public byte[] serialize(RowData element) {
                        return new byte[0];
                    }
                };

        Map<String, SerializationSchema<RowData>> familySerializers = new HashMap<>();
        familySerializers.put("cf1", cf1Serializer);
        familySerializers.put("cf2", cf2Serializer);

        FormatAwareRowMutationSerializer serializer =
                new FormatAwareRowMutationSerializer(
                        schema, "rowKey", familySerializers, Collections.emptyMap(), false);

        serializer.open(null);

        assertTrue("open() should delegate to cf1 serializer", cf1Opened.get());
        assertTrue("open() should delegate to cf2 serializer", cf2Opened.get());
    }
}
