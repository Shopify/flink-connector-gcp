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

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.factories.Factory;

import java.util.Map;

/**
 * Factory SPI for row key formats. Discovered via Flink's ServiceLoader mechanism using the
 * {@code key.format} option, analogous to Kafka's {@code key.format}.
 *
 * <p>Implementations must be registered in
 * {@code META-INF/services/org.apache.flink.table.factories.Factory}.
 */
public interface BigtableRowKeyFormatFactory extends Factory {

    /**
     * Creates a {@link BigtableRowKeyFormat} from the given configuration and the full resolved
     * table schema.
     *
     * <p>The {@code schema} parameter provides access to the complete table schema, including
     * column names, types, and primary key constraints. Implementations should determine which
     * fields to use for row key construction by reading field name(s) from {@code rawTableOptions}
     * (e.g. a format-specific option) and resolving the corresponding indices and types from
     * {@code schema}. When no explicit field option is set, implementations should fall back to the
     * PRIMARY KEY columns declared in the schema.
     *
     * @param tableOptions the full table configuration (all connector options)
     * @param rawTableOptions the raw string-to-string table options map, for reading dynamic
     *     per-field options like {@code key.composite-zero-padded.pad-length.<field>}
     * @param schema the full resolved table schema, including columns and primary key constraints
     * @return a configured {@link BigtableRowKeyFormat}
     */
    BigtableRowKeyFormat createRowKeyFormat(
            ReadableConfig tableOptions,
            Map<String, String> rawTableOptions,
            ResolvedSchema schema);
}
