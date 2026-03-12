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

package com.google.flink.connector.gcp.bigtable.writer;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;

import com.google.api.core.ApiFuture;
import com.google.api.gax.batching.Batcher;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import com.google.cloud.bigtable.data.v2.models.TableId;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Flushable writer that sends records to Bigtable during "flush"/
 *
 * <p>Method "collect" adds elements to the {@link Batcher}, which gets flushed during "flush".
 *
 * <p>At closing, all records are flushed and clients are closed.
 */
public class BigtableFlushableWriter {
    String table;
    BigtableDataClient client;
    Batcher<RowMutationEntry, Void> batcher;

    List<ApiFuture<Void>> batchFutures = new ArrayList<>();

    private long totalRecordsBuffer = 0L;
    private Counter numRecordsOutCounter;
    private Counter numOutEntryFailuresCounter;
    private Counter numBatchFailuresCounter;
    private Histogram numEntriesPerFlush;
    private final long flushMaxRecords;
    private Counter numAutoFlushesCounter;

    private static final Integer HISTOGRAM_WINDOW_SIZE = 100;

    public BigtableFlushableWriter(
            BigtableDataClient client, WriterInitContext sinkInitContext, String table) {
        this(client, sinkInitContext, table, 0L);
    }

    public BigtableFlushableWriter(
            BigtableDataClient client,
            WriterInitContext sinkInitContext,
            String table,
            long flushMaxRecords) {
        checkNotNull(client);
        checkNotNull(sinkInitContext);
        checkNotNull(table);

        this.client = client;
        this.table = table;
        this.batcher = client.newBulkMutationBatcher(TableId.of(table));
        this.flushMaxRecords = flushMaxRecords;

        // Instantiate Metrics
        this.numRecordsOutCounter =
                sinkInitContext.metricGroup().getIOMetricGroup().getNumRecordsOutCounter();
        this.numOutEntryFailuresCounter =
                sinkInitContext.metricGroup().counter("numOutEntryFailuresCounter");
        this.numBatchFailuresCounter =
                sinkInitContext.metricGroup().counter("numBatchFailuresCounter");
        this.numAutoFlushesCounter = sinkInitContext.metricGroup().counter("numAutoFlushesCounter");
        this.numEntriesPerFlush =
                sinkInitContext
                        .metricGroup()
                        .histogram(
                                "numEntriesPerFlush",
                                new DescriptiveStatisticsHistogram(HISTOGRAM_WINDOW_SIZE));
    }

    /** Adds RowMuationEntry to Batcher. */
    public void collect(RowMutationEntry entry) throws InterruptedException {
        ApiFuture<Void> future = batcher.add(entry);
        batchFutures.add(future);
        totalRecordsBuffer++;

        if (shouldAutoFlush()) {
            numAutoFlushesCounter.inc();
            flush();
        }
    }

    private boolean shouldAutoFlush() {
        return flushMaxRecords > 0 && totalRecordsBuffer >= flushMaxRecords;
    }

    /** Sends mutations to Bigtable. */
    public void flush() throws InterruptedException {
        batcher.flush();

        // Check individual futures for failures
        List<Throwable> entryFailures = new ArrayList<>();
        for (ApiFuture<Void> future : batchFutures) {
            try {
                future.get();
            } catch (CancellationException | ExecutionException entryException) {
                this.numOutEntryFailuresCounter.inc();
                entryFailures.add(entryException);
            }
        }

        if (!entryFailures.isEmpty()) {
            this.numBatchFailuresCounter.inc();
            // Clear state before throwing — batcher is still alive
            batchFutures.clear();
            totalRecordsBuffer = 0L;
            RuntimeException ex =
                    new RuntimeException(
                            "Batch flush had " + entryFailures.size() + " entry failures",
                            entryFailures.get(0));
            entryFailures.subList(1, entryFailures.size()).forEach(ex::addSuppressed);
            throw ex;
        }

        // Update metrics
        this.numEntriesPerFlush.update(totalRecordsBuffer);
        this.numRecordsOutCounter.inc(totalRecordsBuffer);

        // Clear state (no batcher recreation needed)
        batchFutures.clear();
        totalRecordsBuffer = 0L;
    }

    /** Send outstanding mutations and closes clients. */
    public void close() throws InterruptedException {
        flush();
        batcher.close();
        client.close();
    }

    @VisibleForTesting
    Counter getNumRecordsOutCounter() {
        return numRecordsOutCounter;
    }

    @VisibleForTesting
    Counter getNumOutEntryFailuresCounter() {
        return numOutEntryFailuresCounter;
    }

    @VisibleForTesting
    Counter getNumBatchFailuresCounter() {
        return numBatchFailuresCounter;
    }

    @VisibleForTesting
    Histogram getNumEntriesPerFlush() {
        return numEntriesPerFlush;
    }

    @VisibleForTesting
    Counter getNumAutoFlushesCounter() {
        return numAutoFlushesCounter;
    }
}
