# Plan: Optimizing `audb.publish()` for Multi-Worker Processing

## Current State Analysis

### Overview of the Publish Flow

The `publish()` function in `audb/core/publish.py:582-932` processes files in two phases:

1. **Find Phase**: Discover changed files and calculate MD5 checksums
2. **Upload Phase**: Zip and upload files to the backend

### Current Threading Implementation

| Function | Phase | Threading | Location |
|----------|-------|-----------|----------|
| `_find_attachments()` | Find | **Sequential** | publish.py:77-162 |
| `_find_tables()` | Find | **Sequential** | publish.py:275-309 |
| `_find_media()` | Find | **Parallel** (`audeer.run_tasks`) | publish.py:165-272 |
| `_put_attachments()` | Upload | **Parallel** (`audeer.run_tasks`) | publish.py:405-428 |
| `_put_tables()` | Upload | **Parallel** (`audeer.run_tasks`) | publish.py:551-579 |
| `_put_media()` | Upload | **Parallel** (`audeer.run_tasks`) | publish.py:431-548 |

### Current Execution Flow (publish.py:889-916)

```
Sequential execution:
├─ _find_attachments()     # Sequential loop with progress_bar
├─ _put_attachments()      # Parallel with run_tasks
├─ _find_tables()          # Sequential loop with progress_bar
├─ _put_tables()           # Parallel with run_tasks
├─ _find_media()           # Parallel with run_tasks
├─ _put_media()            # Parallel with run_tasks
├─ upload_dependencies()   # Sequential
└─ put_file(header)        # Sequential
```

### Bottlenecks Identified

1. **`_find_attachments()` is sequential** (lines 135-160)
   - Uses `audeer.progress_bar()` for iteration
   - Calls `utils.md5()` sequentially for each attachment
   - For databases with many attachments, this becomes a bottleneck

2. **`_find_tables()` is sequential** (lines 299-307)
   - Uses `audeer.progress_bar()` for iteration
   - Calls `utils.md5()` sequentially for each table
   - For databases with many tables, this becomes a bottleneck

3. **No pipelining between find and upload**
   - All files in a category must be "found" before any uploading starts
   - CPU-bound work (MD5 calculation) and I/O-bound work (uploading) cannot overlap

---

## Proposed Optimizations

### Phase 1: Parallelize Sequential Find Functions (Low Risk, High Impact)

**Goal**: Make `_find_attachments()` and `_find_tables()` use parallel processing like `_find_media()` already does.

#### Changes to `_find_attachments()` (lines 77-162)

Current code (sequential):
```python
for attachment_id in audeer.progress_bar(
    list(db.attachments),
    desc="Find attachments",
    disable=not verbose,
):
    # ... validate and calculate MD5
```

Proposed change (parallel):
```python
# Thread-safe container for results
attachment_ids = []

def job(attachment_id: str):
    path = db.attachments[attachment_id].path
    if not os.path.exists(audeer.path(db_root, path)):
        if path not in deps:
            db.attachments[attachment_id].files  # Raises FileNotFoundError
    else:
        checksum = utils.md5(audeer.path(db_root, path))
        if path not in deps or checksum != deps.checksum(path):
            return (attachment_id, path, checksum)  # Return result instead of modifying shared state
    return None

results = audeer.run_tasks(
    job,
    params=[([attachment_id], {}) for attachment_id in db.attachments],
    num_workers=num_workers,
    progress_bar=verbose,
    task_description="Find attachments",
    maximum_refresh_time=define.MAXIMUM_REFRESH_TIME,
)

# Process results sequentially (safe modification of deps)
for result in results:
    if result is not None:
        attachment_id, path, checksum = result
        deps._add_attachment(
            file=path,
            version=version,
            archive=attachment_id,
            checksum=checksum,
        )
        attachment_ids.append(attachment_id)
```

**Key Changes**:
- Move validation loop (lines 95-131) before the parallel section (this must remain sequential as it validates paths)
- Parallel MD5 computation with `audeer.run_tasks()`
- Return results from parallel jobs instead of modifying `deps` in parallel
- Update `deps` sequentially after parallel section

#### Changes to `_find_tables()` (lines 275-309)

Current code (sequential):
```python
for table, file in audeer.progress_bar(
    zip(table_ids, table_files),
    desc="Find tables",
    disable=not verbose,
):
    checksum = utils.md5(os.path.join(db_root, file))
    if file not in deps or checksum != deps.checksum(file):
        deps._add_meta(file, version, checksum)
        tables.append(table)
```

Proposed change (parallel):
```python
def job(table: str, file: str):
    checksum = utils.md5(os.path.join(db_root, file))
    if file not in deps or checksum != deps.checksum(file):
        return (table, file, checksum)
    return None

results = audeer.run_tasks(
    job,
    params=[([table, file], {}) for table, file in zip(table_ids, table_files)],
    num_workers=num_workers,
    progress_bar=verbose,
    task_description="Find tables",
    maximum_refresh_time=define.MAXIMUM_REFRESH_TIME,
)

# Process results sequentially
tables = []
for result in results:
    if result is not None:
        table, file, checksum = result
        deps._add_meta(file, version, checksum)
        tables.append(table)
```

#### Function Signature Changes

Both functions need `num_workers` parameter added:

```python
def _find_attachments(
    db: audformat.Database,
    db_root: str,
    version: str,
    deps: Dependencies,
    num_workers: int,  # NEW
    verbose: bool,
) -> list[str]:
```

```python
def _find_tables(
    db: audformat.Database,
    db_root: str,
    version: str,
    deps: Dependencies,
    num_workers: int,  # NEW
    verbose: bool,
) -> list[str]:
```

#### Estimated Impact

- For databases with many tables or attachments: **significant speedup**
- For databases with few tables/attachments: **negligible difference**
- Risk: **Low** - follows existing pattern used in `_find_media()`

---

### Phase 2: Pipeline Find and Upload Operations (Medium Risk, High Impact)

**Goal**: Allow uploading to start as soon as files are found, overlapping CPU work (MD5) with I/O work (upload).

#### Concept

Instead of:
```
[Find all files] → [Upload all files]
```

Use a producer-consumer pattern:
```
[Find file₁] → [Upload file₁]
[Find file₂] → [Upload file₂]
...continuing in parallel...
```

#### Implementation Approach

Use a thread-safe queue to connect find and upload workers:

```python
import queue
import threading

def _process_media_pipelined(
    db: audformat.Database,
    db_root: str,
    db_root_files: set[str],
    version: str,
    deps: Dependencies,
    archives: Mapping[str, str],
    backend_interface,
    num_workers: int,
    verbose: bool,
):
    """Combined find and upload with pipelining."""

    # Queue for archives ready to upload
    upload_queue = queue.Queue()

    # Sentinel to signal completion
    DONE = object()

    # Track archives that need uploading
    archives_to_upload = set()

    def find_worker():
        """Producer: find files and queue archives for upload."""
        # ... existing _find_media logic ...
        # Instead of returning archives, queue them:
        for archive in archives_to_upload:
            upload_queue.put(archive)
        upload_queue.put(DONE)

    def upload_worker():
        """Consumer: upload archives as they become available."""
        while True:
            archive = upload_queue.get()
            if archive is DONE:
                break
            # ... existing _put_media logic for single archive ...

    # Start workers
    find_thread = threading.Thread(target=find_worker)
    upload_threads = [
        threading.Thread(target=upload_worker)
        for _ in range(num_workers)
    ]

    find_thread.start()
    for t in upload_threads:
        t.start()

    find_thread.join()
    for _ in upload_threads:
        upload_queue.put(DONE)  # Signal all upload workers to stop
    for t in upload_threads:
        t.join()
```

#### Complexity Considerations

1. **Thread Safety**: The `deps` object is modified by both find and upload operations
   - Solution: Use a lock or accumulate changes for later batch application

2. **Error Handling**: An error in one thread shouldn't crash others silently
   - Solution: Use `concurrent.futures.ThreadPoolExecutor` with proper exception handling

3. **Progress Reporting**: Need to aggregate progress from multiple workers
   - Solution: Use thread-safe counters or callbacks

#### Estimated Impact

- For large databases with slow network: **significant speedup** (30-50% reduction in wall-clock time)
- Complexity: **Higher** - requires careful thread management
- Risk: **Medium** - more moving parts, harder to debug

---

### Phase 3: Parallel Independent Find Operations (Higher Risk)

**Goal**: Run `_find_attachments()`, `_find_tables()`, and `_find_media()` concurrently.

#### Why This is Tricky

All three functions modify the shared `deps` object:
- `deps._add_attachment()`
- `deps._add_meta()`
- `deps._add_media()`
- `deps._drop()`

These operations are NOT thread-safe.

#### Possible Approaches

**Option A: Make deps operations thread-safe**
- Add locking to all deps methods
- Pro: Allows full parallelism
- Con: Lock contention may reduce benefits; significant refactoring

**Option B: Return results, apply sequentially**
- Each find function returns (additions, removals) tuples
- Main thread applies all changes sequentially after parallel completion
- Pro: No shared mutable state during parallel execution
- Con: Requires restructuring of find functions

**Option C: Separate deps per type, merge later**
- Each find function works with its own deps subset
- Merge at the end
- Pro: No contention
- Con: deps structure doesn't easily support this

#### Estimated Impact

- Theoretical speedup: Up to 3x if all three find operations take similar time
- Practical speedup: Lower, as media files usually dominate
- Risk: **Higher** - thread safety concerns, complex merging logic

---

## Recommendation

### Immediate (Phase 1)

**Implement parallel processing for `_find_attachments()` and `_find_tables()`**

- Low risk, consistent with existing patterns
- Will benefit databases with many tables/attachments
- Easy to test and verify

### Future Consideration (Phase 2)

**Implement pipelining for media processing**

- Worthwhile for very large databases
- Consider only if benchmarks show significant time spent waiting between find and upload phases
- Requires careful thread safety handling

### Not Recommended (Phase 3)

**Parallel independent find operations**

- Complexity outweighs benefits
- Media files typically dominate processing time
- Thread safety around deps is non-trivial

---

## Implementation Checklist

### Phase 1 Tasks

- [ ] Add `num_workers` parameter to `_find_attachments()` signature
- [ ] Refactor `_find_attachments()` to use `audeer.run_tasks()` for MD5 calculation
- [ ] Keep validation loop sequential (before parallel section)
- [ ] Add `num_workers` parameter to `_find_tables()` signature
- [ ] Refactor `_find_tables()` to use `audeer.run_tasks()` for MD5 calculation
- [ ] Update calls in `publish()` to pass `num_workers` to new signatures
- [ ] Add tests for parallel execution
- [ ] Benchmark with various database sizes

### Testing Strategy

1. **Unit tests**: Verify same results with `num_workers=1` and `num_workers=4`
2. **Integration tests**: Full publish with various database configurations
3. **Benchmarks**: Measure wall-clock time with 1, 2, 4, 8 workers

---

## Appendix: Code Locations

| File | Lines | Description |
|------|-------|-------------|
| `audb/core/publish.py` | 77-162 | `_find_attachments()` |
| `audb/core/publish.py` | 165-272 | `_find_media()` (already parallel) |
| `audb/core/publish.py` | 275-309 | `_find_tables()` |
| `audb/core/publish.py` | 405-428 | `_put_attachments()` (already parallel) |
| `audb/core/publish.py` | 431-548 | `_put_media()` (already parallel) |
| `audb/core/publish.py` | 551-579 | `_put_tables()` (already parallel) |
| `audb/core/publish.py` | 582-932 | Main `publish()` function |
| `audb/core/publish.py` | 889-916 | Sequential execution of find/put steps |
| `audb/core/utils.py` | 40-66 | `md5()` function |
