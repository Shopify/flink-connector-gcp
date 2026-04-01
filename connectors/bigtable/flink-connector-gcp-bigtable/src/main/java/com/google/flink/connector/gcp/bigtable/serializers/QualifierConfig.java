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

import org.apache.flink.table.types.logical.LogicalType;

import java.io.Serializable;
import java.util.Objects;

/**
 * Configuration for a qualifier-keyed column family. Holds the index and logical type of the field
 * within the column family's sub-row that serves as the Bigtable column qualifier.
 */
public final class QualifierConfig implements Serializable {

    private final int fieldIndex;
    private final LogicalType fieldType;

    public QualifierConfig(int fieldIndex, LogicalType fieldType) {
        this.fieldIndex = fieldIndex;
        this.fieldType = fieldType;
    }

    public int fieldIndex() {
        return fieldIndex;
    }

    public LogicalType fieldType() {
        return fieldType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        QualifierConfig that = (QualifierConfig) o;
        return fieldIndex == that.fieldIndex && Objects.equals(fieldType, that.fieldType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fieldIndex, fieldType);
    }

    @Override
    public String toString() {
        return "QualifierConfig[fieldIndex=" + fieldIndex + ", fieldType=" + fieldType + "]";
    }
}
