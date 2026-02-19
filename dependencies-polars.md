# Migration Plan: audb.Dependencies to Polars

This document outlines the plan to migrate `audb.Dependencies` from pandas to Polars for improved performance and future-proofing for out-of-memory datasets.

## Current Implementation Analysis

### Architecture Overview

**File:** `audb/core/dependencies.py` (863 lines)

The `Dependencies` class manages metadata for all database files (media, tables, attachments) using a pandas DataFrame with PyArrow-backed dtypes.

### Data Structure

| Column | Type | Description |
|--------|------|-------------|
| file (index) | string | Relative file path (row identifier) |
| archive | string[pyarrow] | UUID of archive containing the file |
| bit_depth | int32[pyarrow] | Bit depth for media files |
| channels | int32[pyarrow] | Number of audio channels |
| checksum | string[pyarrow] | MD5 checksum |
| duration | float64[pyarrow] | Duration in seconds |
| format | string[pyarrow] | File format (csv, wav, parquet, etc.) |
| removed | int32[pyarrow] | Boolean flag (1=removed, 0=active) |
| sampling_rate | int32[pyarrow] | Sampling rate in Hz |
| type | int32[pyarrow] | File type enum {0: meta, 1: media, 2: attachment} |
| version | string[pyarrow] | Version string |

**Typical Size:** 10,000 to 1,000,000+ rows

### Identified Performance Bottlenecks

1. **Filter operations** (`.media`, `.tables`, `.archives`)
   - Current: O(n) linear scan with boolean masks
   - Impact: ~5-20ms for 1M rows

2. **Batch inserts** via `pd.concat()`
   - Current: Creates new DataFrame, concatenates, resets dtypes
   - Impact: ~100-500ms for 10k files

3. **Drop operations**
   - Current: `self._df[~self._df.index.isin(files)]`
   - Impact: ~100-200ms for large file lists

4. **Single-threaded execution**
   - No parallelization of operations

5. **Memory usage**
   - Entire DataFrame must fit in memory
   - No lazy evaluation support

---

## Migration Goals

1. **Performance:** Speed up filter operations, random access, and batch mutations
2. **Scalability:** Support datasets larger than memory via lazy evaluation
3. **API Compatibility:** Preserve existing public API where possible
4. **Format Compatibility:** Keep Parquet as primary format

---

## Why Polars?

| Feature | Pandas (current) | Polars (proposed) |
|---------|-----------------|-------------------|
| Lazy evaluation | No | Yes (LazyFrame) |
| Parallel execution | No | Yes (automatic) |
| Memory efficiency | Moderate | High (zero-copy) |
| String handling | Object dtype overhead | Native string type |
| Parquet support | Via PyArrow | Native (faster) |
| Filter performance | Boolean masks | Expression-based (faster) |
| Index concept | Yes | No (use column instead) |

---

## Implementation Plan

### Phase 1: Preparation

#### 1.1 Add Polars Dependency

**File:** `pyproject.toml`

```toml
dependencies = [
    ...
    "polars>=1.0.0",
]
```

#### 1.2 Create Polars Schema Definition

**File:** `audb/core/define.py`

```python
import polars as pl

DEPENDENCY_SCHEMA_POLARS = {
    "file": pl.String,
    "archive": pl.String,
    "bit_depth": pl.Int32,
    "channels": pl.Int32,
    "checksum": pl.String,
    "duration": pl.Float64,
    "format": pl.String,
    "removed": pl.Int32,
    "sampling_rate": pl.Int32,
    "type": pl.Int32,
    "version": pl.String,
}
```

---

### Phase 2: Core Data Structure Migration

#### 2.1 Replace Internal DataFrame

**Current:**
```python
def __init__(self):
    self._df = pd.DataFrame(columns=define.DEPENDENCY_TABLE)
```

**New:**
```python
def __init__(self):
    self._df = pl.DataFrame(schema=define.DEPENDENCY_SCHEMA_POLARS)
```

#### 2.2 Remove Index Dependency

Polars does not have an index concept. The "file" column becomes a regular column.

**Impact:**
- All `self._df.index` references become `self._df["file"]`
- All `self._df.loc[file]` become `self._df.filter(pl.col("file") == file)`
- All `self._df.at[file, col]` become filtered row access

---

### Phase 3: Method Migration

#### 3.1 Dunder Methods

| Method | Current (pandas) | New (polars) |
|--------|-----------------|--------------|
| `__contains__` | `file in self._df.index` | `file in self._df["file"]` |
| `__getitem__` | `self._df.loc[file].tolist()` | `self._df.filter(pl.col("file") == file).row(0)` |
| `__len__` | `len(self._df)` | `self._df.height` |
| `__str__` | `str(self._df)` | `str(self._df)` |
| `__eq__` | `self._df.equals(other._df)` | `self._df.equals(other._df)` |

**Detailed Implementation:**

```python
def __contains__(self, file: str) -> bool:
    """Check if file exists in dependencies."""
    return file in self._df["file"]

def __getitem__(self, file: str) -> list:
    """Get row values for a file."""
    row = self._df.filter(pl.col("file") == file)
    if row.height == 0:
        raise KeyError(file)
    # Return values in column order (excluding "file")
    return list(row.select(define.DEPENDENCY_TABLE).row(0))

def __len__(self) -> int:
    """Return number of files."""
    return self._df.height
```

#### 3.2 Property Methods

| Property | Current | New |
|----------|---------|-----|
| `archives` | `sorted(self._df.archive.unique().tolist())` | `self._df["archive"].unique().sort().to_list()` |
| `files` | `self._df.index.tolist()` | `self._df["file"].to_list()` |
| `media` | `self._df[mask].index.tolist()` | `self._df.filter(pl.col("type") == 1)["file"].to_list()` |
| `tables` | `self._df[mask].index.tolist()` | `self._df.filter(pl.col("type") == 0)["file"].to_list()` |
| `attachments` | `self._df[mask].index.tolist()` | `self._df.filter(pl.col("type") == 2)["file"].to_list()` |

**Detailed Implementation:**

```python
@property
def archives(self) -> list:
    """List of unique archives, sorted."""
    return self._df["archive"].unique().sort().to_list()

@property
def files(self) -> list:
    """List of all files."""
    return self._df["file"].to_list()

@property
def media(self) -> list:
    """List of media files."""
    return (
        self._df
        .filter(pl.col("type") == define.DEPENDENCY_TYPE["media"])
        ["file"]
        .to_list()
    )

@property
def removed_media(self) -> list:
    """List of removed media files."""
    return (
        self._df
        .filter(
            (pl.col("type") == define.DEPENDENCY_TYPE["media"]) &
            (pl.col("removed") == 1)
        )
        ["file"]
        .to_list()
    )
```

#### 3.3 Column Accessor Methods

**Current pattern:**
```python
def archive(self, file: str) -> str:
    return self._column_loc("archive", file)

def _column_loc(self, column: str, file: str, dtype=None):
    value = self._df.at[file, column]
    if dtype is not None:
        value = dtype(value)
    return value
```

**New pattern:**
```python
def archive(self, file: str) -> str:
    return self._column_value("archive", file)

def _column_value(self, column: str, file: str) -> typing.Any:
    """Get a single column value for a file."""
    result = self._df.filter(pl.col("file") == file).select(column)
    if result.height == 0:
        raise KeyError(file)
    return result.item()
```

**Optimized batch access (new method):**
```python
def _column_values(self, column: str, files: list[str]) -> dict:
    """Get column values for multiple files efficiently."""
    result = (
        self._df
        .filter(pl.col("file").is_in(files))
        .select(["file", column])
    )
    return dict(zip(result["file"].to_list(), result[column].to_list()))
```

#### 3.4 Mutation Methods

**`_add_media` - Current:**
```python
def _add_media(self, values: list):
    df = pd.DataFrame.from_records(values, columns=...)
    self._df = pd.concat([self._df, df])
    self._df = self._set_dtypes(self._df)
```

**`_add_media` - New:**
```python
def _add_media(self, values: list):
    new_rows = pl.DataFrame(
        values,
        schema=define.DEPENDENCY_SCHEMA_POLARS,
        orient="row",
    )
    self._df = pl.concat([self._df, new_rows])
```

**`_drop` - Current:**
```python
def _drop(self, files: list):
    # Note: df.drop() is slow
    self._df = self._df[~self._df.index.isin(files)]
```

**`_drop` - New:**
```python
def _drop(self, files: list):
    self._df = self._df.filter(~pl.col("file").is_in(files))
```

**`_remove` - Current:**
```python
def _remove(self, file: str):
    self._df.at[file, "removed"] = 1
```

**`_remove` - New:**
```python
def _remove(self, file: str):
    self._df = self._df.with_columns(
        pl.when(pl.col("file") == file)
        .then(1)
        .otherwise(pl.col("removed"))
        .alias("removed")
    )
```

---

### Phase 4: I/O Migration

#### 4.1 Load Method

**Current:**
```python
def load(self, path: str):
    if path.endswith("csv"):
        table = csv.read_csv(path, ...)
        self._df = self._table_to_dataframe(table)
    elif path.endswith("pkl"):
        self._df = pd.read_pickle(path)
    elif path.endswith("parquet"):
        table = parquet.read_table(path)
        self._df = self._table_to_dataframe(table)
```

**New:**
```python
def load(self, path: str):
    if path.endswith("csv"):
        self._df = pl.read_csv(
            path,
            schema=define.DEPENDENCY_SCHEMA_POLARS,
            has_header=True,
        )
    elif path.endswith("pkl"):
        # Legacy support: load pandas pickle, convert to polars
        pdf = pd.read_pickle(path)
        pdf = pdf.reset_index(names=["file"])
        self._df = pl.from_pandas(pdf)
    elif path.endswith("parquet"):
        self._df = pl.read_parquet(path)
```

#### 4.2 Save Method

**Current:**
```python
def save(self, path: str):
    if path.endswith("csv"):
        table = self._dataframe_to_table(self._df)
        csv.write_csv(table, path)
    elif path.endswith("pkl"):
        self._df.to_pickle(path, protocol=4)
    elif path.endswith("parquet"):
        table = self._dataframe_to_table(self._df, file_column=True)
        parquet.write_table(table, path)
```

**New:**
```python
def save(self, path: str):
    if path.endswith("csv"):
        self._df.write_csv(path)
    elif path.endswith("pkl"):
        # For cache: use Polars native serialization
        self._df.write_ipc(path.replace(".pkl", ".arrow"))
    elif path.endswith("parquet"):
        self._df.write_parquet(path)
```

#### 4.3 Cache Format Change

**Recommendation:** Replace pickle (`.pkl`) with Arrow IPC (`.arrow`) for cache files.

- Arrow IPC is Polars' native format
- Faster than pickle
- Memory-mappable

**Migration path:**
1. Support reading old `.pkl` files (convert pandas to polars)
2. Write new cache as `.arrow`
3. Deprecate `.pkl` in future version

---

### Phase 5: Backward Compatibility

#### 5.1 Deprecate `__call__` Method

The `deps()` method returns the internal DataFrame, which is used by external code.

**Current:**
```python
def __call__(self) -> pd.DataFrame:
    return self._df
```

**New (with deprecation):**
```python
def __call__(self) -> pl.DataFrame:
    warnings.warn(
        "Calling Dependencies() to get the internal DataFrame is deprecated. "
        "Use Dependencies.to_polars() or Dependencies.to_pandas() instead.",
        DeprecationWarning,
    )
    return self._df

def to_polars(self) -> pl.DataFrame:
    """Return the dependencies as a Polars DataFrame."""
    return self._df

def to_pandas(self) -> pd.DataFrame:
    """Return the dependencies as a pandas DataFrame."""
    return self._df.to_pandas()
```

#### 5.2 Maintain Return Types

All public methods should return the same types as before:
- `files`, `media`, `tables`, etc. return `list[str]`
- `archive()`, `version()`, etc. return `str`
- `duration()` returns `float`
- `channels()`, `bit_depth()`, etc. return `int`

---

### Phase 6: Lazy Evaluation Support (Future)

For very large datasets that don't fit in memory:

```python
class Dependencies:
    def __init__(self):
        self._df: pl.DataFrame | None = None
        self._lazy: pl.LazyFrame | None = None

    def load_lazy(self, path: str):
        """Load dependencies lazily for large datasets."""
        self._lazy = pl.scan_parquet(path)
        self._df = None

    def _ensure_collected(self):
        """Collect lazy frame if needed."""
        if self._df is None and self._lazy is not None:
            self._df = self._lazy.collect()

    @property
    def media(self) -> list:
        if self._lazy is not None:
            # Execute only the filter, not full collection
            return (
                self._lazy
                .filter(pl.col("type") == define.DEPENDENCY_TYPE["media"])
                .select("file")
                .collect()
                ["file"]
                .to_list()
            )
        return self._df.filter(...)["file"].to_list()
```

---

## Testing Strategy

### Unit Tests

Update `tests/test_dependencies.py`:

1. **Test all public methods** return correct types and values
2. **Test I/O roundtrip** for all formats (CSV, Parquet, legacy pickle)
3. **Test edge cases:** empty dependencies, single file, large datasets
4. **Test backward compatibility:** loading old pickle files

### Benchmark Validation

Run existing benchmarks to validate performance improvements:

```bash
python benchmarks/benchmark-dependencies-methods.py
```

**Expected Improvements:**

| Operation | Current (pandas) | Expected (polars) | Improvement |
|-----------|-----------------|-------------------|-------------|
| Filter (media) | 5-20ms | 1-5ms | 3-5x |
| Batch insert (10k) | 100-500ms | 20-100ms | 5x |
| Drop (10k files) | 100-200ms | 20-50ms | 4x |
| Load parquet (1M) | 5-15ms | 2-5ms | 2-3x |
| Save parquet (1M) | 10-20ms | 5-10ms | 2x |

---

## Migration Checklist

### Phase 1: Preparation
- [ ] Add polars to dependencies in `pyproject.toml`
- [ ] Add Polars schema definition to `define.py`
- [ ] Create feature branch

### Phase 2: Core Migration
- [ ] Replace `self._df` initialization
- [ ] Update `__init__` method
- [ ] Remove PyArrow schema (no longer needed)

### Phase 3: Method Updates
- [ ] Update `__contains__`
- [ ] Update `__getitem__`
- [ ] Update `__len__`
- [ ] Update `__str__`
- [ ] Update `__eq__`
- [ ] Update all properties (archives, files, media, tables, etc.)
- [ ] Update all column accessors (archive, duration, etc.)
- [ ] Update `_column_loc` → `_column_value`
- [ ] Update `_add_media`
- [ ] Update `_add_meta`
- [ ] Update `_add_attachment`
- [ ] Update `_update_media`
- [ ] Update `_update_media_version`
- [ ] Update `_remove`
- [ ] Update `_drop`

### Phase 4: I/O
- [ ] Update `load` for Parquet
- [ ] Update `load` for CSV
- [ ] Update `load` for pickle (legacy support)
- [ ] Update `save` for Parquet
- [ ] Update `save` for CSV
- [ ] Decide on cache format (Arrow IPC vs pickle)
- [ ] Remove `_dataframe_to_table` and `_table_to_dataframe`
- [ ] Remove `_set_dtypes` (Polars handles types natively)

### Phase 5: Compatibility
- [ ] Add deprecation warning to `__call__`
- [ ] Add `to_polars()` method
- [ ] Add `to_pandas()` method
- [ ] Update docstrings

### Phase 6: Testing
- [ ] Update all unit tests
- [ ] Run benchmark suite
- [ ] Test with real databases
- [ ] Test backward compatibility with old cache files

### Phase 7: Documentation
- [ ] Update API documentation
- [ ] Add migration guide for users of `deps()`
- [ ] Update changelog

---

## Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| API breakage for `deps()` users | High | Medium | Deprecation warning + `to_pandas()` |
| Performance regression in edge cases | Low | Medium | Comprehensive benchmarking |
| Pickle cache incompatibility | Medium | Low | Support reading old format |
| Polars version compatibility | Low | Low | Pin minimum version, test updates |

---

## Timeline Estimate

| Phase | Effort |
|-------|--------|
| Phase 1: Preparation | Small |
| Phase 2: Core Migration | Medium |
| Phase 3: Method Updates | Large |
| Phase 4: I/O | Medium |
| Phase 5: Compatibility | Small |
| Phase 6: Testing | Medium |
| Phase 7: Documentation | Small |

---

## Open Questions

1. **Cache format:** Should we switch from pickle to Arrow IPC for cache files?
   - Pro: Faster, memory-mappable, native Polars format
   - Con: Breaking change for existing caches

2. **Lazy evaluation:** Should we implement lazy loading in this migration or defer to a future release?
   - Pro: Enables out-of-memory datasets now
   - Con: Adds complexity, may not be needed immediately

3. **`__call__` deprecation timeline:** How long should the deprecation period be?
   - Suggestion: 2 minor versions with warning, remove in next major version

4. **Minimum Polars version:** What version should we require?
   - Suggestion: `polars>=1.0.0` for stable API
