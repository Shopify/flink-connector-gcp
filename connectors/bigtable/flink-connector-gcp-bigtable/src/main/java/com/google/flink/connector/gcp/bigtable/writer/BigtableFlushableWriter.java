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
import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;
import org.apache.flink.util.InstantiationUtil;

import com.google.api.core.ApiFuture;
import com.google.api.gax.batching.Batcher;
import com.google.api.gax.batching.BatchingException;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import com.google.cloud.bigtable.data.v2.models.TableId;
import com.google.flink.connector.gcp.bigtable.utils.ErrorMessages;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

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

    private Long totalRecordsBuffer = 0L;
    private Long totalBytesBuffer = 0L;
    private Counter numRecordsOutCounter;
    private Counter numBytesOutCounter;
    private Counter numOutEntryFailuresCounter;
    private Counter numBatchFailuresCounter;
    private Histogram numEntriesPerFlush;
    private final long flushMaxRecords;
    private final long flushMaxBytes;
    private final long flushInterval;
    @Nullable private ProcessingTimeService processingTimeService;
    private Counter numAutoFlushesCounter;

    private static final Logger logger = LoggerFactory.getLogger(BigtableFlushableWriter.class);

    private static final Integer BATCHER_CLOSE_TIMEOUT_SECONDS = 60;
    private static final Integer HISTOGRAM_WINDOW_SIZE = 100;

    public BigtableFlushableWriter(
            BigtableDataClient client, WriterInitContext sinkInitContext, String table) {
        this(client, sinkInitContext, table, 0L, 0L, 0L, null);
    }

    public BigtableFlushableWriter(
            BigtableDataClient client,
            WriterInitContext sinkInitContext,
            String table,
            long flushMaxRecords,
            long flushMaxBytes) {
        this(client, sinkInitContext, table, flushMaxRecords, flushMaxBytes, 0L, null);
    }

    public BigtableFlushableWriter(
            BigtableDataClient client,
            WriterInitContext sinkInitContext,
            String table,
            long flushMaxRecords,
            long flushMaxBytes,
            long flushInterval,
            @Nullable ProcessingTimeService processingTimeService) {
        checkNotNull(client);
        checkNotNull(sinkInitContext);
        checkNotNull(table);

        this.client = client;
        this.table = table;
        this.batcher = client.newBulkMutationBatcher(TableId.of(table));
        this.flushMaxRecords = flushMaxRecords;
        this.flushMaxBytes = flushMaxBytes;
        this.flushInterval = flushInterval;
        this.processingTimeService = processingTimeService;

        // Instantiate Metrics
        this.numRecordsOutCounter =
                sinkInitContext.metricGroup().getIOMetricGroup().getNumRecordsOutCounter();
        this.numBytesOutCounter =
                sinkInitContext.metricGroup().getIOMetricGroup().getNumBytesOutCounter();
        this.numOutEntryFailuresCounter =
                sinkInitContext.metricGroup().counter("numOutEntryFailuresCounter");
        this.numBatchFailuresCounter =
                sinkInitContext.metricGroup().counter("numBatchFailuresCounter");
        this.numAutoFlushesCounter =
                sinkInitContext.metricGroup().counter("numAutoFlushesCounter");
        this.numEntriesPerFlush =
                sinkInitContext
                        .metricGroup()
                        .histogram(
                                "numEntriesPerFlush",
                                new DescriptiveStatisticsHistogram(HISTOGRAM_WINDOW_SIZE));

        if (processingTimeService != null && flushInterval > 0) {
            scheduleNextFlush();
        }
    }

    /** Adds RowMuationEntry to Batcher. */
    public void collect(RowMutationEntry entry) throws InterruptedException {
        ApiFuture<Void> future = batcher.add(entry);
        batchFutures.add(future);
        totalRecordsBuffer++;
        totalBytesBuffer += getEntryBytesSize(entry);

        if (shouldAutoFlush()) {
            numAutoFlushesCounter.inc();
            flush();
        }
    }

    private boolean shouldAutoFlush() {
        if (flushMaxRecords > 0 && totalRecordsBuffer >= flushMaxRecords) {
            return true;
        }
        if (flushMaxBytes > 0 && totalBytesBuffer >= flushMaxBytes) {
            return true;
        }
        return false;
    }

    /** Called by timer to trigger a time-based flush. */
    public void onTimer() throws InterruptedException {
        if (totalRecordsBuffer > 0) {
            numAutoFlushesCounter.inc();
            flush();
        }
        scheduleNextFlush();
    }

    private void scheduleNextFlush() {
        if (processingTimeService != null && flushInterval > 0) {
            processingTimeService.registerTimer(
                    processingTimeService.getCurrentProcessingTime() + flushInterval,
                    timestamp -> {
                        try {
                            onTimer();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new RuntimeException(e);
                        }
                    });
        }
    }

    /** Sends mutations to Bigtable. */
    public void flush() throws InterruptedException {
        try {
            batcher.close(Duration.ofSeconds(BATCHER_CLOSE_TIMEOUT_SECONDS));
            // Update metrics
            this.numEntriesPerFlush.update(totalRecordsBuffer);
            this.numRecordsOutCounter.inc(totalRecordsBuffer);
            this.numBytesOutCounter.inc(totalBytesBuffer);
            // Recreate client and clear metrics
            batcher = client.newBulkMutationBatcher(TableId.of(this.table));
            batchFutures.clear();
            totalRecordsBuffer = 0L;
            totalBytesBuffer = 0L;
        } catch (BatchingException batchingException) {
            this.numBatchFailuresCounter.inc();
            for (ApiFuture<Void> future : batchFutures) {
                try {
                    future.get();
                } catch (CancellationException
                        | ExecutionException
                        | InterruptedException entryException) {
                    this.numOutEntryFailuresCounter.inc();
                }
            }
            throw new RuntimeException(batchingException.getMessage());
        } catch (TimeoutException timeoutException) {
            throw new RuntimeException(timeoutException.getMessage());
        }
    }

    /** Send outstanding mutations and closes clients. */
    public void close() throws InterruptedException {
        flush();
        batcher.close();
        client.close();
    }

    /** Calculate total bytes per entry. */
    @VisibleForTesting
    static int getEntryBytesSize(RowMutationEntry entry) {
        try {
            return InstantiationUtil.serializeObject(entry).length;
        } catch (IOException e) {
            logger.warn(ErrorMessages.METRICS_ENTRY_SERIALIZATION_WARNING + e.getMessage());
            return 0;
        }
    }

    @VisibleForTesting
    Counter getNumRecordsOutCounter() {
        return numRecordsOutCounter;
    }

    @VisibleForTesting
    Counter getNumBytesOutCounter() {
        return numBytesOutCounter;
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
