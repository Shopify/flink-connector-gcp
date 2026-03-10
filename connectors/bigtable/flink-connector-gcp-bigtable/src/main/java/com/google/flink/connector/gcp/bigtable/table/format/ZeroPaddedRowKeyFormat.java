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

/**
 * Row key format that zero-pads a single numeric primary key field to a fixed length. Ensures
 * correct lexicographic ordering in Bigtable row scans.
 */
public class ZeroPaddedRowKeyFormat implements BigtableRowKeyFormat {

    private static final long serialVersionUID = 1L;

    private final int keyFieldIndex;
    private final LogicalTypeRoot typeRoot;
    private final String formatString;

    public ZeroPaddedRowKeyFormat(int keyFieldIndex, LogicalType keyFieldType, int padLength) {
        this.keyFieldIndex = keyFieldIndex;
        this.typeRoot = keyFieldType.getTypeRoot();
        this.formatString = "%0" + padLength + "d";
    }

    @Override
    public String format(RowData record) {
        long value;
        switch (typeRoot) {
            case BIGINT:
                value = record.getLong(keyFieldIndex);
                break;
            case INTEGER:
                value = record.getInt(keyFieldIndex);
                break;
            case SMALLINT:
                value = record.getShort(keyFieldIndex);
                break;
            case TINYINT:
                value = record.getByte(keyFieldIndex);
                break;
            default:
                throw new IllegalArgumentException(
                        "Unsupported row key type for 'zero-padded' format: " + typeRoot);
        }
        return String.format(formatString, value);
    }
}
