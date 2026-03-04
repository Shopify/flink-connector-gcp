# Fix Batcher Flush Data Loss Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace destructive `batcher.close()` + recreate pattern in `flush()` with non-destructive `batcher.flush()` to prevent data loss in auto-flush scenarios.

**Architecture:** The `flush()` method currently calls `batcher.close(Duration)` which is a terminal operation, then recreates the batcher. With auto-flush triggering every N records, this close+recreate cycle is both expensive and fragile. The fix uses the Batcher's native `flush()` method (sync send + wait, keeps batcher alive) and checks futures individually for failures. `batcher.close()` is only called in the writer's `close()` method where it belongs.

**Tech Stack:** Java, GAX Batcher API (2.59.1), Google Cloud Bigtable client, Apache Flink Sink API

---

## Key API Reference (GAX `Batcher` 2.59.1)

- `flush()` — Synchronously sends pending elements and waits for all outstanding batches. Does NOT close batcher. Does NOT throw `BatchingException`.
- `close()` — Flushes + closes batcher permanently. Throws `BatchingException` if any entries failed.
- `close(Duration)` — Same as `close()` with timeout. Throws `TimeoutException` on timeout.

Individual entry failures are always reflected in the `ApiFuture` returned by `add()`.

---

### Task 1: Write failing test for non-destructive flush behavior

**Files:**
- Modify: `connectors/bigtable/flink-connector-gcp-bigtable/src/test/java/com/google/flink/connector/gcp/bigtable/writer/BigtableWriterTest.java`

**Step 1: Write a test that verifies records survive multiple flush cycles without batcher recreation**

This test writes records, flushes, writes more records, flushes again — all using the same batcher instance. It verifies all records arrive in Bigtable. This test should pass with the fix but also passes with current code (since close+recreate works functionally). The key test is the next one.

```java
@Test
public void testMultipleFlushCyclesPreserveData() throws InterruptedException {
    BigtableFlushableWriter writer =
            new BigtableFlushableWriter(client, mockContext, TestingUtils.TABLE, 0);

    // First batch: 5 records
    for (int i = 0; i < 5; i++) {
        RowMutationEntry entry = RowMutationEntry.create("batch1-key" + i);
        entry.setCell(
                TestingUtils.COLUMN_FAMILY, "column", TestingUtils.TIMESTAMP, "value" + i);
        writer.collect(entry);
    }
    writer.flush();
    assertEquals(5, writer.getNumRecordsOutCounter().getCount());

    // Second batch: 5 more records
    for (int i = 0; i < 5; i++) {
        RowMutationEntry entry = RowMutationEntry.create("batch2-key" + i);
        entry.setCell(
                TestingUtils.COLUMN_FAMILY, "column", TestingUtils.TIMESTAMP, "value" + i);
        writer.collect(entry);
    }
    writer.flush();
    assertEquals(10, writer.getNumRecordsOutCounter().getCount());

    // Verify all records exist in Bigtable
    for (int i = 0; i < 5; i++) {
        assertNotNull(client.readRow(TableId.of(TestingUtils.TABLE), "batch1-key" + i));
        assertNotNull(client.readRow(TableId.of(TestingUtils.TABLE), "batch2-key" + i));
    }

    writer.close();
}
```

**Step 2: Write a test that verifies auto-flush with interleaved checkpoint flushes preserves all data**

This is the critical test — it reproduces the real-world scenario where auto-flush and checkpoint flush interact.

```java
@Test
public void testAutoFlushWithCheckpointFlushPreservesAllData() throws InterruptedException {
    // Auto-flush every 3 records
    BigtableFlushableWriter writer =
            new BigtableFlushableWriter(client, mockContext, TestingUtils.TABLE, 3);

    // Write 7 records: auto-flush at 3 and 6, leaves 1 buffered
    for (int i = 0; i < 7; i++) {
        RowMutationEntry entry = RowMutationEntry.create("key" + i);
        entry.setCell(
                TestingUtils.COLUMN_FAMILY, "column", TestingUtils.TIMESTAMP, "value" + i);
        writer.collect(entry);
    }

    // 2 auto-flushes happened (at record 3 and 6), counter = 6
    assertEquals(2, writer.getNumAutoFlushesCounter().getCount());
    assertEquals(6, writer.getNumRecordsOutCounter().getCount());

    // Checkpoint flush sends the remaining 1 record
    writer.flush();
    assertEquals(7, writer.getNumRecordsOutCounter().getCount());

    // Verify ALL 7 records exist in Bigtable
    for (int i = 0; i < 7; i++) {
        Row row = client.readRow(TableId.of(TestingUtils.TABLE), "key" + i);
        assertNotNull("Record key" + i + " should exist in Bigtable", row);
    }

    writer.close();
}
```

**Step 3: Run tests to verify they pass (baseline)**

Run: `cd connectors/bigtable && mvn test -pl flink-connector-gcp-bigtable -Dtest=BigtableWriterTest -DfailIfNoTests=false -q`

Expected: All tests PASS (current close+recreate pattern works functionally, these tests establish the correctness baseline)

**Step 4: Commit**

```bash
git add connectors/bigtable/flink-connector-gcp-bigtable/src/test/java/com/google/flink/connector/gcp/bigtable/writer/BigtableWriterTest.java
git commit -m "test: add data preservation tests for flush lifecycle"
```

---

### Task 2: Replace `batcher.close()` with `batcher.flush()` in the flush method

**Files:**
- Modify: `connectors/bigtable/flink-connector-gcp-bigtable/src/main/java/com/google/flink/connector/gcp/bigtable/writer/BigtableFlushableWriter.java`

**Step 1: Rewrite the `flush()` method**

Replace the current `flush()` implementation (lines 120-145) with:

```java
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
        throw new RuntimeException(
                "Batch flush had " + entryFailures.size() + " entry failures");
    }

    // Update metrics
    this.numEntriesPerFlush.update(totalRecordsBuffer);
    this.numRecordsOutCounter.inc(totalRecordsBuffer);

    // Clear state (no batcher recreation needed)
    batchFutures.clear();
    totalRecordsBuffer = 0L;
}
```

Key changes:
- `batcher.close(Duration)` → `batcher.flush()` (non-destructive, keeps batcher alive)
- Removed `batcher = client.newBulkMutationBatcher(...)` (no recreation needed)
- Explicit future checking replaces `BatchingException` catch
- State cleanup happens in both success and error paths
- Removed `catch (TimeoutException)` — `batcher.flush()` doesn't throw it (RPC-level timeouts are handled by the client config and reflected in individual futures)

**Step 2: Remove unused imports**

Remove these imports that are no longer needed:
- `java.time.Duration` (no longer passing Duration to close)
- `com.google.api.gax.batching.BatchingException` (no longer catching it)
- `java.util.concurrent.TimeoutException` (no longer catching it)

Also remove the now-unused constant:
- `BATCHER_CLOSE_TIMEOUT_SECONDS`

**Step 3: Run tests**

Run: `cd connectors/bigtable && mvn test -pl flink-connector-gcp-bigtable -Dtest=BigtableWriterTest -DfailIfNoTests=false -q`

Expected: ALL tests PASS, including the new tests from Task 1

**Step 4: Commit**

```bash
git add connectors/bigtable/flink-connector-gcp-bigtable/src/main/java/com/google/flink/connector/gcp/bigtable/writer/BigtableFlushableWriter.java
git commit -m "fix: use non-destructive batcher.flush() instead of close+recreate

batcher.close() is a terminal operation that destroys the batcher.
Using it in flush() required recreating the batcher every time, which
is expensive and fragile — especially with auto-flush triggering every
N records. Switch to batcher.flush() which sends pending entries and
waits for completion without closing the batcher."
```

---

### Task 3: Fix the `close()` method

**Files:**
- Modify: `connectors/bigtable/flink-connector-gcp-bigtable/src/main/java/com/google/flink/connector/gcp/bigtable/writer/BigtableFlushableWriter.java`

**Step 1: Simplify `close()` method**

The current `close()` calls `flush()` (which previously closed+recreated the batcher), then calls `batcher.close()` again on the new empty batcher. Now that `flush()` keeps the batcher alive, `close()` becomes cleaner:

Replace lines 148-152:

```java
/** Send outstanding mutations and closes clients. */
public void close() throws InterruptedException {
    flush();
    batcher.close();
    client.close();
}
```

This is the same code, but the semantics are now correct:
- `flush()` sends remaining entries (batcher stays open)
- `batcher.close()` closes the batcher permanently (the ONLY place close is called)
- `client.close()` closes the client

No code change needed — the method is already correct with the new `flush()` semantics. Just verify it works.

**Step 2: Run tests**

Run: `cd connectors/bigtable && mvn test -pl flink-connector-gcp-bigtable -Dtest=BigtableWriterTest -DfailIfNoTests=false -q`

Expected: ALL tests PASS

**Step 3: Commit (only if close() needed changes)**

Skip commit if no changes were needed.

---

### Task 4: Run full test suite and verify

**Files:** None (verification only)

**Step 1: Run all Bigtable connector tests**

Run: `cd connectors/bigtable && mvn test -pl flink-connector-gcp-bigtable -DfailIfNoTests=false -q`

Expected: ALL tests PASS

**Step 2: Run checkstyle/formatting if applicable**

Run: `cd connectors/bigtable && mvn spotless:check -pl flink-connector-gcp-bigtable -q 2>/dev/null || echo "No spotless plugin"` and `cd connectors/bigtable && mvn checkstyle:check -pl flink-connector-gcp-bigtable -q 2>/dev/null || echo "No checkstyle plugin"`

Fix any formatting issues if they arise.

**Step 3: Commit any formatting fixes**

```bash
git commit -am "style: fix formatting"
```
