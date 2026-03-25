# Flink Bigtable Connector

This is a connector for Bigtable Sink for Apache Flink.

## Features

*   **Table API and DataStream API Sink:** Write data to Bigtable using the Flink Table API or Datastream API.
*   **Custom Serializers:** Use built-in or custom serializers to convert your data into Bigtable mutations.

## Usage

### Prerequisites

*   **Google Cloud Project:** A Google Cloud project with billing enabled.
*   **Bigtable Instance:** A Bigtable instance in your project.
*   **Bigtable Table:** A Bigtable table in your instance.

### Examples

See the `flink-examples-gcp-bigtable` module for example pipelines demonstrating different use cases of the Flink Bigtable Connector.

### Installation

The connector is available on [Maven repository](https://mvnrepository.com/artifact/com.google.flink.connector.gcp/flink-connector-gcp-bigtable). **The latest version is: `0.3.2`**.

```
<dependency>
    <groupId>com.google.flink.connector.gcp</groupId>
    <artifactId>flink-connector-gcp-bigtable</artifactId>
    <version>VERSION_HERE</version>
</dependency>
```

You can also install the connector directly after cloning the repository and import it from your local Maven Repository:

```
cd connectors/bigtable
mvn clean install -DskipTest
```

## Serializers

This connector comes with four built-in serializers to convert data types into Bigtable `RowMutationEntry` objects:

*   **`GenericRecordToRowMutationSerializer`**: For AVRO `GenericRecord` objects.
*   **`RowDataToRowMutationSerializer`**: For Flink `RowData` objects.
*   **`FunctionRowMutationSerializer`**: For custom serialization logic using a provided function.
*   **`FormatAwareRowMutationSerializer`**: For format-agnostic serialization using Flink's format SPI. Used automatically when `value.format` is set in the Table API.

You can create your own custom serializer inheriting from `BaseRowMutationSerializer`.

See [supported types](#supported-types) for serializers `GenericRecordToRowMutationSerializer` and `RowDataToRowMutationSerializer`.

### Column Family and Nested Rows Modes

`GenericRecordToRowMutationSerializer` and `RowDataToRowMutationSerializer` support two modes:

*   **Column Family Mode**: All fields are written to a single specified column family. Nested fields are not supported.
*   **Nested Rows Mode**: Each top-level field represents a separate column family, with its value being another `GenericRecord` or `RowData` containing the columns for that family. Only single nested rows are supported.

These two mode are incompatible with each other and you must choose one of them.

**Column Family Mode:**

```
GenericRecordToRowMutationSerializer.builder()
    .withRowKeyField("key")
    .withColumnFamily("my-column-family")
    // other settings
    .build();
```

In this mode, all fields will be written to Column Family `my-column-family`.

**Nested Rows Mode:**

```
GenericRecordToRowMutationSerializer.builder()
    .withRowKeyField("key")
    .withNestedRowsMode()
    // other settings
    .build();
```

In this mode, all fields except `RowKeyField` must be Rows. The Row name represents the Column Family. For example:

*   `key` (String)
*   `family1` (contains fields like `name` (String), `age` (Integer))
*   `family2` (contains fields like `city` (String), `zipCode` (Integer)) 

Column Family `family1` would have columns `name, age` and Column Family `family2` would have columns `city, zipCode`.

You can find more information about Column Families in the [official Bigtable documentation](https://cloud.google.com/bigtable/docs/schema-design#column-families).

### Supported Types

Both `RECORD` and `ROW` are not supported unless it's to define a Column Family in `Nested Rows Mode`. Double nested rows are not supported. For those and other unsupported types, you can use `BYTES`.

#### `GenericRecordToRowMutationSerializer`

The [Avro Types](https://avro.apache.org/docs/1.12.0/api/java/org/apache/avro/Schema.Type.html) supported are:

* `STRING` 
* `BYTES`  
* `INT`  
* `LONG` 
* `FLOAT`  
* `DOUBLE`  
* `BOOLEAN`

#### `RowDataToRowMutationSerializer`

The [DataTypes](https://nightlies.apache.org/flink/flink-docs-release-1.19/api/java/index.html?org/apache/flink/table/api/DataTypes.html) supported are:

* `STRING`
* `CHAR`
* `VARCHAR`
* `BOOLEAN`
* `TINYINT`
* `SMALLINT`
* `INT`
* `BIGINT`
* `DOUBLE`
* `FLOAT`
* `BYTES`
* `BINARY`
* `TIMESTAMP`
* `TIMESTAMP_WITH_TIME_ZONE`
* `TIMESTAMP_WITH_LOCAL_TIME_ZONE`
* `INTERVAL(YEAR, MONTH)` 
* `INTERVAL(DAY, SECOND)`
* `TIME`
* `DATE`
* `DECIMAL` 

The maximum precision for time-based types is 6.

### Format-Agnostic Mode (Table API)

When using the Table API, you can use any Flink-compatible format (JSON, Protobuf, Avro, etc.) for encoding cell values by setting the `value.format` option. This delegates serialization to Flink's `SerializationFormatFactory` SPI, so any format on the classpath works automatically.

**Flat Mode** (single column family):

```
CREATE TABLE bigtable_sink (
  row_key STRING NOT NULL,
  name STRING,
  age INT,
  PRIMARY KEY (row_key) NOT ENFORCED
) WITH (
  'connector' = 'bigtable',
  'project' = 'my-project',
  'instance' = 'my-instance',
  'table' = 'my-table',
  'column-family' = 'cf1',
  'value.format' = 'protobuf'
);
```

**Nested Rows Mode** (multiple column families):

```
CREATE TABLE bigtable_sink (
  row_key STRING NOT NULL,
  product ROW<shop_id BIGINT, title STRING>,
  PRIMARY KEY (row_key) NOT ENFORCED
) WITH (
  'connector' = 'bigtable',
  'project' = 'my-project',
  'instance' = 'my-instance',
  'table' = 'my-table',
  'use-nested-rows-mode' = 'true',
  'value.format' = 'protobuf',
  'product.qualifier-field' = 'shop_id'
);
```

When `qualifier-field` is set, the specified field's value becomes the column qualifier and the full sub-row (including the qualifier field) is serialized as the cell value. When `qualifier-field` is not set, cells are stored under a default `payload` qualifier.

#### Delete Behavior (upsert/all changelog modes)

When a `DELETE` event is received, the delete scope depends on whether a `qualifier-field` is configured. This applies to both `FormatAwareRowMutationSerializer` and the built-in `RowDataToRowMutationSerializer` — both use the same delete semantics.

**Without `qualifier-field`** — deletes the entire column family:

```
-- Given: row_key='user1' with cells cf1:payload, cf1:other_col
-- DELETE event for row_key='user1'
-- Result: all cells in cf1 for row_key='user1' are deleted (deleteFamily)
```

> **Note:** Without a `qualifier-field`, the connector does not have enough information to target a specific qualifier, so it deletes the entire column family. This means the `payload` cell and any other cells in the same family (including those written by other systems) will be deleted.

**With `qualifier-field`** — deletes only the specific cell identified by the qualifier value:

```
-- Given: row_key='user1' with cells cf1:42, cf1:99
-- DELETE event for row_key='user1', shop_id=42
-- Result: only cell cf1:42 is deleted (deleteCells), cf1:99 is untouched
```

In nested-rows mode, the same logic applies per column family: families with a `<family>.qualifier-field` delete only the specific cell, while families without one delete the entire family.

Without `value.format`, the connector uses its built-in byte serialization (see [Serializers](#serializers)).

## Table API

This connector provides support for Flink's Table API, enabling easy and efficient data writing to Bigtable tables within your Flink Table API pipelines. 

The connector is named `bigtable`.

### Row Key

The Bigtable Flink Connector uses the primary key defined in your Flink schema as the row key for writing data to Bigtable. It cannot be null and only one primary key can be used.

The supported row key types are: `VARCHAR`, `CHAR`, `BIGINT`, `INT`, `SMALLINT`, and `TINYINT`. Numeric types are zero-padded to 19 digits to preserve lexicographic sort order for non-negative values.

```
Schema schema =
        Schema.newBuilder()
                .column("my-key", DataTypes.STRING().notNull())
                .column("stringColumn", DataTypes.STRING())
                .column("intColumn", DataTypes.INT())
                .primaryKey("my-key")
                .build();
```

You can also use numeric primary keys:

```
Schema schema =
        Schema.newBuilder()
                .column("id", DataTypes.INT().notNull())
                .column("stringColumn", DataTypes.STRING())
                .primaryKey("id")
                .build();
```

#### Connector Options

The following connector options are available:

| Option | Description |
|---|---|
| `project` | (Required) The Google Cloud project ID. |
| `instance` | (Required) The Bigtable instance ID. |
| `table` | (Required) The Bigtable table ID. |
| `column-family` | The column family to write to (incompatible with `use-nested-rows-mode`). |
| `use-nested-rows-mode` | Whether to use nested rows as column families (incompatible with `column-family`). |
| `sink.parallelism` | The parallelism of the sink. |
| `flow-control` | Specifies the use of batch flow control for writing. Defaults to `false`. |
| `app-profile-id` | Specifies the App Profile ID used when writing. |
| `credentials-file` | Specifies the Google Cloud credentials file to use. |
| `credentials-key` | Specifies the Google Cloud credentials key to use. |
| `credentials-access-token` | Specifies the Google Cloud access token to use as credentials. |
| `changelog-mode` | Changelog mode for the sink: `insert-only` (default), `upsert`, or `all`. Modes other than `insert-only` require a PRIMARY KEY. |
| `batchSize` | The number of elements to group in a batch. |
| `value.format` | The format for encoding cell values (e.g., `json`, `protobuf`). When set, uses Flink's format SPI instead of built-in byte serialization. |
| `qualifier-field` | Field name to use as the Bigtable column qualifier. Requires `value.format` and `column-family`. When not set, cells are stored under a default `payload` qualifier. |
| `<family>.qualifier-field` | Per-family qualifier field for nested-rows mode. Requires `value.format` and `use-nested-rows-mode`. When not set, cells are stored under a default `payload` qualifier. |

Either `column-family` or `use-nested-rows-mode` is required. The `value.format` option is optional — when omitted, the connector uses its built-in byte serialization.

#### Changelog Modes

The `changelog-mode` option controls which types of change events the sink accepts:

| Mode | Events | Description |
|---|---|---|
| `insert-only` | `INSERT` | Default. Only accepts inserts — suitable for append-only workloads. |
| `upsert` | `INSERT`, `UPDATE_AFTER`, `DELETE` | Accepts inserts, updates, and deletes. `UPDATE_BEFORE` events are ignored. Requires a PRIMARY KEY. |
| `all` | `INSERT`, `UPDATE_BEFORE`, `UPDATE_AFTER`, `DELETE` | Accepts all changelog events. Requires a PRIMARY KEY. |

For `DELETE` events, see [Delete Behavior](#delete-behavior-upsertall-changelog-modes) for details on how deletes are applied to Bigtable.

## Exactly Once

Due to Bigtable's nature, the Sink offers Exactly Once out of the box. In order to get Exactly Once, every element needs to have a timestamp which will be used in the `RowMutationEntry`.

Bigtable storage format is a tuple of `(Key, CFamily, Column, Timestamp)` and `Value`. Bigtable is idempotent through timestamp. This means that if you write the same value multiple times with the same timestamp, the database doesn't change. If you write different values for the same rowkey, Bigtable will update its state with the value that has the latest timestamp (when querying at now).

| Incoming Mutation (in order)                                  | Bigtable State (after mutation, at ts NOW) |
| :------------------------------------------------------------ | :---------------------------------------- |
| RowKey, CF, C, timestamp1, my-value                           | my-value                                   |
| RowKey, CF, C, timestamp2, my-value-2                         | my-value2                                  |
| RowKey, CF, C, timestamp1, my-value                           | my-value2                                  |
| RowKey, CF, C, timestamp3, my-value                           | my-value                                   |

This means that, as long as the timestamp doesn't change with retries (i.e., it is idempotent with retries) and the rest of the pipeline satisfies Exactly Once, the Bigtable Sink would work as Exactly once.

You can see an example of idempotent timestamps in the `flink-examples-gcp-bigtable` module.
