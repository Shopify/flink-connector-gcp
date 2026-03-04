# Include Row Key in Flat Mode Protobuf Value — Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Include the row key field in the serialized protobuf cell value in flat mode, so consumers reading Bigtable cells get the full message (including the key field).

**Architecture:** Remove the row key projection in `FormatAwareRowMutationSerializer` flat mode path and in `BigtableDynamicTableSink`. The protobuf serializer receives the full schema and the full row data. The row key is still separately used as the Bigtable row key via `BigtableRowKeyFormat`.

**Tech Stack:** Java, Flink Table API, Bigtable client, JUnit 4

---

### Task 1: Write failing test proving the row key is missing

**Files:**
- Modify: `connectors/bigtable/flink-connector-gcp-bigtable/src/test/java/com/google/flink/connector/gcp/bigtable/serializers/FormatAwareRowMutationSerializerTest.java`

**Step 1: Add test that verifies the serializer passes the full row (including row key) to the format serializer**

```java
@Test
public void testFlatModeIncludesRowKeyInSerializedValue() {
    DataType schema =
            DataTypes.ROW(
                    DataTypes.FIELD("product_id", DataTypes.BIGINT().notNull()),
                    DataTypes.FIELD("shop_id", DataTypes.BIGINT()),
                    DataTypes.FIELD("title", DataTypes.STRING()));

    BigtableRowKeyFormat keyFormat = new ToStringRowKeyFormat(0, new BigIntType());

    // Mock serializer that captures the RowData it receives
    final RowData[] captured = new RowData[1];
    SerializationSchema<RowData> capturingSerializer =
            rowData -> {
                captured[0] = rowData;
                return "bytes".getBytes();
            };

    FormatAwareRowMutationSerializer serializer =
            FormatAwareRowMutationSerializer.forFlatMode(
                    schema, "product_id", keyFormat, "product", capturingSerializer, false);

    GenericRowData record = new GenericRowData(3);
    record.setField(0, 12345L);
    record.setField(1, 67890L);
    record.setField(2, StringData.fromString("T-Shirt"));

    serializer.serialize(record, createContext());

    assertNotNull("Serializer should have been called", captured[0]);
    // The serialized RowData should have ALL 3 fields (including product_id)
    assertEquals(3, captured[0].getArity());
    assertEquals(12345L, captured[0].getLong(0));
    assertEquals(67890L, captured[0].getLong(1));
    assertEquals("T-Shirt", captured[0].getString(2).toString());
}
```

**Step 2: Run test to verify it fails**

Run: `cd connectors/bigtable/flink-connector-gcp-bigtable && ../../../mvnw test -Dtest=FormatAwareRowMutationSerializerTest#testFlatModeIncludesRowKeyInSerializedValue -DfailIfNoTests=false`
Expected: FAIL — `captured[0].getArity()` returns 2 (projected row without product_id)

---

### Task 2: Fix FormatAwareRowMutationSerializer to stop projecting out the row key

**Files:**
- Modify: `connectors/bigtable/flink-connector-gcp-bigtable/src/main/java/com/google/flink/connector/gcp/bigtable/serializers/FormatAwareRowMutationSerializer.java`

**Step 1: In the flat-mode constructor, remove the projection logic**

Remove the `flatProjection` field and the projection-building loop. The constructor should only resolve the `rowKeyIndex` (still needed for `rowKeyFormat.format()`).

Replace lines 111-127 (the projection logic in the flat-mode constructor):

```java
// Before (remove):
int resolvedRowKeyIndex = -1;
List<Integer> projection = new ArrayList<>();
int index = 0;
for (Field field : DataType.getFields(physicalDataType)) {
    if (field.getName().equals(rowKeyField)) {
        resolvedRowKeyIndex = index;
    } else {
        projection.add(index);
    }
    index++;
}
...
this.rowKeyIndex = resolvedRowKeyIndex;
this.flatProjection = projection.stream().mapToInt(Integer::intValue).toArray();

// After:
int resolvedRowKeyIndex = -1;
int index = 0;
for (Field field : DataType.getFields(physicalDataType)) {
    if (field.getName().equals(rowKeyField)) {
        resolvedRowKeyIndex = index;
        break;
    }
    index++;
}
checkNotNull(
        resolvedRowKeyIndex >= 0 ? resolvedRowKeyIndex : null,
        String.format("Row key field '%s' not found in schema", rowKeyField));
this.rowKeyIndex = resolvedRowKeyIndex;
this.flatProjection = null;
```

**Step 2: In `serialize()`, remove ProjectedRowData usage in flat mode**

Replace lines 198-208:

```java
// Before (remove):
if (flatMode) {
    ProjectedRowData projected = ProjectedRowData.from(flatProjection);
    projected.replaceRow(record);
    byte[] value = flatSerializer.serialize(projected);
    ...
}

// After:
if (flatMode) {
    byte[] value = flatSerializer.serialize(record);
    entry.setCell(
            flatColumnFamily,
            ByteString.copyFromUtf8(DEFAULT_QUALIFIER),
            BigtableUtils.getTimestamp(context),
            ByteString.copyFrom(value));
    return entry;
}
```

**Step 3: Remove unused imports**

Remove: `import org.apache.flink.table.data.utils.ProjectedRowData;`
Remove: `import java.util.ArrayList;` and `import java.util.List;` (if no longer used)

**Step 4: Run test to verify it passes**

Run: `cd connectors/bigtable/flink-connector-gcp-bigtable && ../../../mvnw test -Dtest=FormatAwareRowMutationSerializerTest#testFlatModeIncludesRowKeyInSerializedValue -DfailIfNoTests=false`
Expected: PASS

---

### Task 3: Fix BigtableDynamicTableSink to pass full schema to format serializer

**Files:**
- Modify: `connectors/bigtable/flink-connector-gcp-bigtable/src/main/java/com/google/flink/connector/gcp/bigtable/table/BigtableDynamicTableSink.java`

**Step 1: Remove projectWithoutRowKey call and pass full schema**

In `getSinkRuntimeProvider()`, replace the flat mode block (lines 185-197):

```java
// Before (remove):
String columnFamily = connectorOptions.get(BigtableConnectorOptions.COLUMN_FAMILY);
DataType projectedType = projectWithoutRowKey(physicalSchema, rowKeyField);
SerializationSchema<RowData> formatSerializer =
        valueEncodingFormat.createRuntimeEncoder(context, projectedType);

// After:
String columnFamily = connectorOptions.get(BigtableConnectorOptions.COLUMN_FAMILY);
SerializationSchema<RowData> formatSerializer =
        valueEncodingFormat.createRuntimeEncoder(context, physicalSchema);
```

**Step 2: Remove the `projectWithoutRowKey()` method entirely** (lines 319-327)

**Step 3: Remove unused import** `import java.util.ArrayList;` if no longer used elsewhere. Keep it if `buildFamilySerializers` or `parseQualifierConfigs` still needs it.

---

### Task 4: Run full test suite and fix any broken tests

**Step 1: Run all serializer tests**

Run: `cd connectors/bigtable/flink-connector-gcp-bigtable && ../../../mvnw test -Dtest=FormatAwareRowMutationSerializerTest -DfailIfNoTests=false`
Expected: The existing `testSerializeFlatModeWithFormat` test should still pass since it uses a mock serializer that ignores the input RowData and returns fixed bytes.

**Step 2: Run all bigtable tests**

Run: `cd connectors/bigtable/flink-connector-gcp-bigtable && ../../../mvnw test -DfailIfNoTests=false`
Expected: All tests PASS

**Step 3: Run spotless**

Run: `cd connectors/bigtable/flink-connector-gcp-bigtable && ../../../mvnw spotless:apply`

---

### Task 5: Commit

```bash
git add connectors/bigtable/flink-connector-gcp-bigtable/src/main/java/com/google/flink/connector/gcp/bigtable/serializers/FormatAwareRowMutationSerializer.java
git add connectors/bigtable/flink-connector-gcp-bigtable/src/main/java/com/google/flink/connector/gcp/bigtable/table/BigtableDynamicTableSink.java
git add connectors/bigtable/flink-connector-gcp-bigtable/src/test/java/com/google/flink/connector/gcp/bigtable/serializers/FormatAwareRowMutationSerializerTest.java
git commit -m "fix: include row key field in flat mode protobuf cell value

The FormatAwareRowMutationSerializer was projecting out the row key field
before serializing in flat mode. This caused the protobuf message to be
missing the key field (e.g. product_id=0 instead of the actual value).

Remove the ProjectedRowData projection and pass the full schema and full
row to the format serializer so all fields including the row key are
serialized into the cell value."
```
