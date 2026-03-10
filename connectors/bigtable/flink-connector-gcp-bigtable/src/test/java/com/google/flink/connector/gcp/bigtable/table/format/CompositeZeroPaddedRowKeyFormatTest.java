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

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.VarCharType;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class CompositeZeroPaddedRowKeyFormatTest {

    @Test
    public void testTwoBigintFieldsWithPadding() {
        // Schema: shop_id BIGINT (index 0), product_id BIGINT (index 1), name STRING (index 2)
        CompositeZeroPaddedRowKeyFormat format =
                new CompositeZeroPaddedRowKeyFormat(
                        new int[] {0, 1},
                        new LogicalType[] {new BigIntType(), new BigIntType()},
                        new int[] {10, 10},
                        "#");
        GenericRowData row = GenericRowData.of(12345L, 42L, StringData.fromString("product"));
        assertEquals("0000012345#0000000042", format.format(row));
    }

    @Test
    public void testMixedTypesWithPadding() {
        // Schema: shop_id BIGINT (index 0), category STRING (index 1), product_id INT (index 2)
        CompositeZeroPaddedRowKeyFormat format =
                new CompositeZeroPaddedRowKeyFormat(
                        new int[] {0, 1, 2},
                        new LogicalType[] {new BigIntType(), new VarCharType(), new IntType()},
                        new int[] {10, 0, 8},
                        "#");
        GenericRowData row =
                GenericRowData.of(12345L, StringData.fromString("electronics"), 42);
        assertEquals("0000012345#electronics#00000042", format.format(row));
    }

    @Test
    public void testStringFieldNoPadding() {
        // pad-length 0 means no padding for string fields
        CompositeZeroPaddedRowKeyFormat format =
                new CompositeZeroPaddedRowKeyFormat(
                        new int[] {0, 1},
                        new LogicalType[] {new VarCharType(), new VarCharType()},
                        new int[] {0, 0},
                        "#");
        GenericRowData row =
                GenericRowData.of(
                        StringData.fromString("abc"), StringData.fromString("def"));
        assertEquals("abc#def", format.format(row));
    }

    @Test
    public void testCustomSeparator() {
        CompositeZeroPaddedRowKeyFormat format =
                new CompositeZeroPaddedRowKeyFormat(
                        new int[] {0, 1},
                        new LogicalType[] {new BigIntType(), new BigIntType()},
                        new int[] {5, 5},
                        "|");
        GenericRowData row = GenericRowData.of(1L, 2L);
        assertEquals("00001|00002", format.format(row));
    }

    @Test
    public void testSingleFieldComposite() {
        CompositeZeroPaddedRowKeyFormat format =
                new CompositeZeroPaddedRowKeyFormat(
                        new int[] {0},
                        new LogicalType[] {new BigIntType()},
                        new int[] {10},
                        "#");
        GenericRowData row = GenericRowData.of(12345L, StringData.fromString("data"));
        assertEquals("0000012345", format.format(row));
    }

    @Test
    public void testNumericFieldWithoutPadding() {
        // pad-length 0 means no padding, just String.valueOf()
        CompositeZeroPaddedRowKeyFormat format =
                new CompositeZeroPaddedRowKeyFormat(
                        new int[] {0, 1},
                        new LogicalType[] {new BigIntType(), new BigIntType()},
                        new int[] {0, 10},
                        "#");
        GenericRowData row = GenericRowData.of(12345L, 42L);
        assertEquals("12345#0000000042", format.format(row));
    }
}
