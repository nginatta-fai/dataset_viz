from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import duckdb
import pyarrow as pa
from datasets import Dataset, DatasetDict, load_from_disk
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# Enforce offline behaviour; huggingface hub calls will fail fast.
os.environ.setdefault("HF_DATASETS_OFFLINE", "1")

# Default root can be overridden per request; not relied on exclusively.
DEFAULT_ROOT = Path(
    os.environ.get(
        "DATASETS_DIR",
        Path(__file__).resolve().parent.parent.parent / "datasets",
    )
).resolve()
DEFAULT_LIMIT = int(os.environ.get("QUERY_DEFAULT_LIMIT", "1000"))

app = FastAPI(title="Dataset Viz Backend", version="0.1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://127.0.0.1:5173", "http://localhost:5173"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


class QueryRequest(BaseModel):
    sql: str
    split: Optional[str] = None
    limit: Optional[int] = None


class QueryResponse(BaseModel):
    columns: List[str]
    rows: List[Dict[str, Any]]
    row_count: int
    truncated: bool
    elapsed_ms: float


def _resolve_root(root: Optional[str]) -> Path:
    base = Path(root).expanduser().resolve() if root else DEFAULT_ROOT
    if not base.exists():
        raise HTTPException(status_code=404, detail=f"Root path does not exist: {base}")
    if not base.is_dir():
        raise HTTPException(status_code=400, detail=f"Root path is not a directory: {base}")
    return base


def _available_datasets(root: Path) -> List[str]:
    return sorted([p.name for p in root.iterdir() if p.is_dir() and not p.name.startswith(".")])


def _detect_format(path: Path) -> str:
    """
    Return "hf" for load_from_disk-style datasets, "parquet" for parquet files,
    "arrow" for IPC files, else raise.
    """
    # A true `save_to_disk` dataset always contains state.json. Cached datasets
    # from `load_dataset` often have only dataset_info.json alongside Arrow
    # shards and are *not* load_from_disk compatible. Require state.json to
    # classify as Hugging Face; otherwise fall through so Arrow/Parquet paths
    # get picked up and handled as flat files.
    if (path / "state.json").exists():
        return "hf"
    if list(path.glob("*.parquet")):
        return "parquet"
    if list(path.glob("*.arrow")):
        return "arrow"
    raise HTTPException(
        status_code=400,
        detail="Unsupported dataset format. Expecting a Hugging Face 'load_from_disk' folder or parquet/arrow files.",
    )


def _load_hf_dataset(path: Path) -> DatasetDict | Dataset:
    try:
        return load_from_disk(str(path))
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail="Dataset not found locally") from exc
    except Exception as exc:  # pragma: no cover - narrow env
        raise HTTPException(status_code=400, detail=f"Failed to load dataset: {exc}") from exc


def _select_split(ds: DatasetDict | Dataset, split: Optional[str]) -> Dataset:
    if isinstance(ds, DatasetDict):
        chosen = split or next(iter(ds.keys()))
        if chosen not in ds:
            raise HTTPException(status_code=404, detail=f"Split '{chosen}' not present")
        return ds[chosen]
    return ds


def _hf_split_to_arrow(split: Dataset) -> pa.Table:
    # Access the underlying pyarrow table without materialising to pandas.
    if hasattr(split, "_data") and hasattr(split._data, "to_table"):
        return split._data.to_table()
    # Fallback: Dataset may expose .data attribute with .to_table()
    if hasattr(split, "data") and hasattr(split.data, "to_table"):
        return split.data.to_table()
    raise HTTPException(status_code=500, detail="Could not obtain Arrow table from dataset")


def _file_dataset_to_arrow(path: Path, fmt: str, limit: Optional[int] = None) -> pa.Table:
    if fmt == "parquet":
        pattern = str(path / "*.parquet")
        sql = f"SELECT * FROM read_parquet('{pattern}')"
        if limit:
            sql += f" LIMIT {int(limit)}"
        return duckdb.query(sql).arrow()

    import pyarrow.ipc as ipc

    files = sorted([f for f in path.glob("*.arrow") if not f.name.startswith("cache-")])
    if not files:
        # Fall back to all .arrow files if we only had cache shards.
        files = sorted(path.glob("*.arrow"))
    if not files:
        raise HTTPException(status_code=400, detail="No .arrow files found in dataset directory")

    def _arrow_kind(file: Path) -> str:
        with file.open("rb") as fh:
            magic = fh.read(6)
        if magic.startswith(b"ARROW1") or magic.startswith(b"ARROW") or magic.startswith(b"FEA1"):
            return "file"
        if magic.startswith(b"\xff\xff\xff\xff"):
            return "stream"
        try:
            with ipc.open_file(file):
                return "file"
        except Exception:
            return "stream"

    kind = _arrow_kind(files[0])

    # Return empty table with correct schema without materialising data.
    if limit == 0:
        if kind == "file":
            with ipc.open_file(files[0]) as reader:
                return pa.Table.from_batches([], schema=reader.schema)
        with ipc.open_stream(files[0]) as reader:
            return pa.Table.from_batches([], schema=reader.schema)

    remaining = int(limit) if limit is not None else None
    tables: list[pa.Table] = []

    for file in files:
        if kind == "file":
            with ipc.open_file(file) as reader:
                if remaining is None:
                    tables.append(reader.read_all())
                    continue
                batches = []
                for i in range(reader.num_record_batches):
                    batch = reader.get_batch(i)
                    if remaining < batch.num_rows:
                        batch = batch.slice(0, remaining)
                    batches.append(batch)
                    remaining -= batch.num_rows
                    if remaining <= 0:
                        break
                if batches:
                    tables.append(pa.Table.from_batches(batches))
        else:
            with ipc.open_stream(file) as reader:
                if remaining is None:
                    tables.append(reader.read_all())
                    continue

                batches = []
                while remaining > 0:
                    batch = reader.read_next_batch()
                    if batch is None:
                        break
                    if remaining < batch.num_rows:
                        batch = batch.slice(0, remaining)
                    batches.append(batch)
                    remaining -= batch.num_rows
                    if remaining <= 0:
                        break
                if batches:
                    tables.append(pa.Table.from_batches(batches))

        if remaining is not None and remaining <= 0:
            break

    if not tables:
        # No rows requested / no data, return empty with schema from first file.
        if kind == "file":
            with ipc.open_file(files[0]) as reader:
                return pa.Table.from_batches([], schema=reader.schema)
        with ipc.open_stream(files[0]) as reader:
            return pa.Table.from_batches([], schema=reader.schema)
    return pa.concat_tables(tables)


def _get_arrow_table(root: Path, name: str, split: Optional[str], limit: Optional[int] = None) -> Tuple[pa.Table, bool]:
    path = root / name
    if not path.exists():
        raise HTTPException(status_code=404, detail="Dataset not found locally")

    fmt = _detect_format(path)
    if fmt == "hf":
        ds = _load_hf_dataset(path)
        split_ds = _select_split(ds, split)
        table = _hf_split_to_arrow(split_ds)
        truncated = False
        if limit:
            table = table.slice(0, limit)
            truncated = table.num_rows >= limit
        return table, truncated

    # Parquet / Arrow files treated as a single split named "default".
    table = _file_dataset_to_arrow(path, fmt, limit)
    truncated = bool(limit and table.num_rows >= limit)
    return table, truncated


def _append_limit(sql: str, limit: int) -> Tuple[str, bool]:
    lowered = sql.lower()
    if " limit " in lowered or lowered.strip().endswith("limit") or lowered.strip().endswith("limit;"):
        return sql, False
    trimmed = sql.strip().rstrip(";")
    return f"{trimmed} LIMIT {limit}", True


@app.get("/health")
def health() -> Dict[str, str]:
    return {"status": "ok"}


@app.get("/datasets")
def list_datasets(root: Optional[str] = Query(None, description="Dataset root directory")) -> Dict[str, List[str]]:
    resolved_root = _resolve_root(root)
    return {"datasets": _available_datasets(resolved_root)}


@app.get("/datasets/{name}/splits")
def list_splits(
    name: str,
    root: Optional[str] = Query(None, description="Dataset root directory"),
) -> Dict[str, List[str]]:
    resolved_root = _resolve_root(root)
    path = resolved_root / name
    if not path.exists():
        raise HTTPException(status_code=404, detail="Dataset not found locally")
    fmt = _detect_format(path)
    if fmt == "hf":
        ds = _load_hf_dataset(path)
        if isinstance(ds, DatasetDict):
            return {"splits": list(ds.keys())}
        return {"splits": ["default"]}
    # file-based datasets have a single split
    return {"splits": ["default"]}


@app.get("/datasets/{name}/schema")
def schema(
    name: str,
    split: Optional[str] = Query(None),
    root: Optional[str] = Query(None, description="Dataset root directory"),
) -> Dict[str, Any]:
    resolved_root = _resolve_root(root)
    path = resolved_root / name
    if not path.exists():
        raise HTTPException(status_code=404, detail="Dataset not found locally")
    fmt = _detect_format(path)

    if fmt == "hf":
        ds = _load_hf_dataset(path)
        split_ds = _select_split(ds, split)
        # features is a FeatureType mapping; convert to string for the UI.
        columns = []
        for k, v in split_ds.features.items():
            dtype = getattr(v, "dtype", None) or getattr(v, "feature", None) or v
            columns.append({"name": k, "dtype": str(dtype)})
        return {"columns": columns, "approx_rows": len(split_ds)}

    # file datasets: read zero rows to inspect schema
    table = _file_dataset_to_arrow(path, fmt, limit=0)
    columns = [{"name": f.name, "dtype": str(f.type)} for f in table.schema]
    return {"columns": columns, "approx_rows": None}


@app.post("/datasets/{name}/query", response_model=QueryResponse)
def query_dataset(
    name: str,
    payload: QueryRequest,
    root: Optional[str] = Query(None, description="Dataset root directory"),
) -> QueryResponse:
    if not payload.sql.strip():
        raise HTTPException(status_code=400, detail="SQL is empty")

    limit_to_apply = payload.limit or DEFAULT_LIMIT
    sql, appended = _append_limit(payload.sql, limit_to_apply)

    resolved_root = _resolve_root(root)
    table, truncated_by_reader = _get_arrow_table(resolved_root, name, payload.split, None)

    con = duckdb.connect(database=":memory:")
    con.register("t", table)

    started = time.perf_counter()
    try:
        result = con.execute(sql).arrow()
    except Exception as exc:  # pragma: no cover - defensive
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    elapsed_ms = (time.perf_counter() - started) * 1000

    # DuckDB 1.4+ returns a RecordBatchReader; earlier versions return a Table.
    if hasattr(result, "to_table"):
        result_table = result.to_table()
    elif hasattr(result, "read_all"):
        result_table = result.read_all()
    else:
        result_table = result  # assume pyarrow.Table

    rows = result_table.to_pylist()
    max_rows = payload.limit or (DEFAULT_LIMIT if appended else None)
    truncated = truncated_by_reader or (max_rows is not None and len(rows) >= max_rows)

    return QueryResponse(
        columns=result_table.schema.names,
        rows=rows,
        row_count=len(rows),
        truncated=truncated,
        elapsed_ms=elapsed_ms,
    )
