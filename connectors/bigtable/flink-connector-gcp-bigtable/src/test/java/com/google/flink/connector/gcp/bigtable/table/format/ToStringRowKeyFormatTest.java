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
import org.apache.flink.table.types.logical.VarCharType;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ToStringRowKeyFormatTest {

    @Test
    public void testFormatStringField() {
        ToStringRowKeyFormat format = new ToStringRowKeyFormat(0, new VarCharType());
        GenericRowData row = GenericRowData.of(StringData.fromString("mykey"), 42);
        assertEquals("mykey", format.format(row));
    }

    @Test
    public void testFormatBigintField() {
        ToStringRowKeyFormat format = new ToStringRowKeyFormat(0, new BigIntType());
        GenericRowData row = GenericRowData.of(12345L, StringData.fromString("data"));
        assertEquals("12345", format.format(row));
    }

    @Test
    public void testFormatIntField() {
        ToStringRowKeyFormat format = new ToStringRowKeyFormat(1, new IntType());
        GenericRowData row = GenericRowData.of(StringData.fromString("data"), 999);
        assertEquals("999", format.format(row));
    }

    @Test
    public void testFormatSmallintField() {
        ToStringRowKeyFormat format = new ToStringRowKeyFormat(0, new SmallIntType());
        GenericRowData row = GenericRowData.of((short) 42);
        assertEquals("42", format.format(row));
    }

    @Test
    public void testFormatTinyintField() {
        ToStringRowKeyFormat format = new ToStringRowKeyFormat(0, new TinyIntType());
        GenericRowData row = GenericRowData.of((byte) 7);
        assertEquals("7", format.format(row));
    }
}
