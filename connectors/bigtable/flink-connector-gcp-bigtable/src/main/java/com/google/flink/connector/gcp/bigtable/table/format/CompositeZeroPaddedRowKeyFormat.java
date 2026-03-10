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
 * Row key format that concatenates multiple primary key fields with a separator. Numeric fields can
 * be optionally zero-padded to a fixed length for correct lexicographic ordering.
 */
public class CompositeZeroPaddedRowKeyFormat implements BigtableRowKeyFormat {

    private static final long serialVersionUID = 1L;

    private final int[] keyFieldIndices;
    private final LogicalTypeRoot[] typeRoots;
    private final int[] padLengths;
    private final String separator;

    /**
     * @param keyFieldIndices indices of the PK fields in the RowData
     * @param keyFieldTypes logical types of the PK fields
     * @param padLengths per-field pad length (0 means no padding)
     * @param separator delimiter between key parts
     */
    public CompositeZeroPaddedRowKeyFormat(
            int[] keyFieldIndices,
            LogicalType[] keyFieldTypes,
            int[] padLengths,
            String separator) {
        this.keyFieldIndices = keyFieldIndices;
        this.typeRoots = new LogicalTypeRoot[keyFieldTypes.length];
        for (int i = 0; i < keyFieldTypes.length; i++) {
            this.typeRoots[i] = keyFieldTypes[i].getTypeRoot();
        }
        this.padLengths = padLengths;
        this.separator = separator;
    }

    @Override
    public String format(RowData record) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < keyFieldIndices.length; i++) {
            if (i > 0) {
                sb.append(separator);
            }
            int index = keyFieldIndices[i];
            LogicalTypeRoot typeRoot = typeRoots[i];
            int padLength = padLengths[i];

            if (typeRoot == LogicalTypeRoot.VARCHAR || typeRoot == LogicalTypeRoot.CHAR) {
                sb.append(record.getString(index).toString());
            } else {
                long value = extractNumericValue(record, index, typeRoot);
                if (padLength > 0) {
                    sb.append(String.format("%0" + padLength + "d", value));
                } else {
                    sb.append(value);
                }
            }
        }
        return sb.toString();
    }

    private static long extractNumericValue(RowData record, int index, LogicalTypeRoot typeRoot) {
        switch (typeRoot) {
            case BIGINT:
                return record.getLong(index);
            case INTEGER:
                return record.getInt(index);
            case SMALLINT:
                return record.getShort(index);
            case TINYINT:
                return record.getByte(index);
            default:
                throw new IllegalArgumentException(
                        "Unsupported row key type for 'composite-zero-padded' format: "
                                + typeRoot);
        }
    }
}
