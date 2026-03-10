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

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import java.util.Objects;

/**
 * Row key format that converts a single primary key field to its string representation using {@code
 * String.valueOf()}. This is the default format, preserving backward compatibility.
 */
public class ToStringRowKeyFormat implements BigtableRowKeyFormat {

    private static final long serialVersionUID = 1L;

    private final int keyFieldIndex;
    private final LogicalTypeRoot typeRoot;

    public ToStringRowKeyFormat(int keyFieldIndex, LogicalType keyFieldType) {
        this.keyFieldIndex = keyFieldIndex;
        this.typeRoot = keyFieldType.getTypeRoot();
    }

    @Override
    public String format(RowData record) {
        switch (typeRoot) {
            case VARCHAR:
            case CHAR:
                return record.getString(keyFieldIndex).toString();
            case BIGINT:
                return String.valueOf(record.getLong(keyFieldIndex));
            case INTEGER:
                return String.valueOf(record.getInt(keyFieldIndex));
            case SMALLINT:
                return String.valueOf(record.getShort(keyFieldIndex));
            case TINYINT:
                return String.valueOf(record.getByte(keyFieldIndex));
            default:
                throw new IllegalArgumentException(
                        "Unsupported row key type for 'to-string' format: " + typeRoot);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ToStringRowKeyFormat that = (ToStringRowKeyFormat) o;
        return keyFieldIndex == that.keyFieldIndex && typeRoot == that.typeRoot;
    }

    @Override
    public int hashCode() {
        return Objects.hash(keyFieldIndex, typeRoot);
    }
}
