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
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TinyIntType;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ZeroPaddedRowKeyFormatTest {

    @Test
    public void testBigintZeroPadded() {
        ZeroPaddedRowKeyFormat format = new ZeroPaddedRowKeyFormat(0, new BigIntType(), 10);
        GenericRowData row = GenericRowData.of(12345L, StringData.fromString("data"));
        assertEquals("0000012345", format.format(row));
    }

    @Test
    public void testIntZeroPadded() {
        ZeroPaddedRowKeyFormat format = new ZeroPaddedRowKeyFormat(0, new IntType(), 8);
        GenericRowData row = GenericRowData.of(42, StringData.fromString("data"));
        assertEquals("00000042", format.format(row));
    }

    @Test
    public void testSmallintZeroPadded() {
        ZeroPaddedRowKeyFormat format = new ZeroPaddedRowKeyFormat(0, new SmallIntType(), 5);
        GenericRowData row = GenericRowData.of((short) 7);
        assertEquals("00007", format.format(row));
    }

    @Test
    public void testTinyintZeroPadded() {
        ZeroPaddedRowKeyFormat format = new ZeroPaddedRowKeyFormat(0, new TinyIntType(), 3);
        GenericRowData row = GenericRowData.of((byte) 9);
        assertEquals("009", format.format(row));
    }

    @Test
    public void testValueExactlyPadLength() {
        ZeroPaddedRowKeyFormat format = new ZeroPaddedRowKeyFormat(0, new BigIntType(), 5);
        GenericRowData row = GenericRowData.of(12345L);
        assertEquals("12345", format.format(row));
    }

    @Test
    public void testValueExceedsPadLength() {
        ZeroPaddedRowKeyFormat format = new ZeroPaddedRowKeyFormat(0, new BigIntType(), 3);
        GenericRowData row = GenericRowData.of(12345L);
        // String.format("%03d", 12345) produces "12345" (no truncation)
        assertEquals("12345", format.format(row));
    }
}
