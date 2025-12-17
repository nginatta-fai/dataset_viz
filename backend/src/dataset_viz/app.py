from __future__ import annotations

import os
import time
from pathlib import Path
from functools import lru_cache
from queue import SimpleQueue
from typing import Any, Dict, List, Optional, Tuple

import duckdb
import pyarrow as pa
import pyarrow.dataset as pyds
from datasets import Dataset, DatasetDict, load_from_disk
from pyarrow.lib import ArrowInvalid
from fastapi import FastAPI, HTTPException, Query, Request, Response
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
MAX_LIMIT = int(os.environ.get("QUERY_MAX_LIMIT", "5000"))
DUCKDB_POOL_SIZE = int(os.environ.get("DUCKDB_POOL_SIZE", "4"))

app = FastAPI(title="Dataset Viz Backend", version="0.1.0")
app.add_middleware(
    CORSMiddleware,
    # Local-only tool; allow common localhost hosts/ports. Regex covers any localhost/127.0.0.1 port.
    allow_origins=["*"],
    allow_origin_regex=r".*",
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"],
    max_age=3600,
)


@app.options("/{path:path}")
def preflight(path: str, request: Request):
    origin = request.headers.get("origin", "*")
    req_headers = request.headers.get("access-control-request-headers", "*")
    headers = {
        "Access-Control-Allow-Origin": origin,
        "Access-Control-Allow-Methods": "GET,POST,PUT,PATCH,DELETE,OPTIONS",
        "Access-Control-Allow-Headers": req_headers,
        "Access-Control-Max-Age": "3600",
    }
    return Response(status_code=204, headers=headers)

_duckdb_pool: SimpleQueue[duckdb.DuckDBPyConnection] = SimpleQueue()
for _ in range(DUCKDB_POOL_SIZE):
    _duckdb_pool.put(duckdb.connect(database=":memory:"))


def _acquire_con() -> duckdb.DuckDBPyConnection:
    try:
        return _duckdb_pool.get_nowait()
    except Exception:
        return duckdb.connect(database=":memory:")


def _release_con(con: duckdb.DuckDBPyConnection) -> None:
    try:
        con.execute("DROP VIEW IF EXISTS t")
        con.execute("DROP TABLE IF EXISTS t")
    except Exception:
        pass
    try:
        _duckdb_pool.put_nowait(con)
    except Exception:
        con.close()


def _prepare_connection(con: duckdb.DuckDBPyConnection) -> None:
    """Clear any leftover relation names before registering a new dataset."""
    con.execute("DROP VIEW IF EXISTS t")
    con.execute("DROP TABLE IF EXISTS t")


class QueryRequest(BaseModel):
    sql: str
    split: Optional[str] = None
    limit: Optional[int] = None
    offset: Optional[int] = None


class QueryResponse(BaseModel):
    columns: List[str]
    data: List[List[Any]]
    row_count: int
    truncated: bool
    elapsed_ms: float


class CountResponse(BaseModel):
    rows: int


def _resolve_root(root: Optional[str]) -> Path:
    base = Path(root).expanduser().resolve() if root else DEFAULT_ROOT
    if not base.exists():
        raise HTTPException(status_code=404, detail=f"Root path does not exist: {base}")
    if not base.is_dir():
        raise HTTPException(status_code=400, detail=f"Root path is not a directory: {base}")
    return base


def _available_datasets(root: Path) -> List[str]:
    """Return dataset names for both subfolders and top-level flat files."""
    names: set[str] = set()
    for p in root.iterdir():
        if p.name.startswith("."):
            continue
        if p.is_dir():
            names.add(p.name)
        elif p.suffix in {".parquet", ".arrow"}:
            names.add(p.name)
    return sorted(names)


def _detect_format(path: Path) -> str:
    """
    Return "hf" for load_from_disk-style datasets, "parquet" for parquet files,
    "arrow" for IPC files, else raise.
    """
    if path.is_file():
        if path.suffix == ".parquet":
            return "parquet"
        if path.suffix == ".arrow":
            return "arrow"
    # A true `save_to_disk` dataset always contains state.json. Cached datasets
    # from `load_dataset` often have only dataset_info.json alongside Arrow
    # shards and are *not* load_from_disk compatible. Require state.json to
    # classify as Hugging Face; otherwise fall through so Arrow/Parquet paths
    # get picked up and handled as flat files.
    if (path / "state.json").exists():
        return "hf"
    if path.is_dir() and list(path.glob("*.parquet")):
        return "parquet"
    if path.is_dir() and list(path.glob("*.arrow")):
        return "arrow"
    raise HTTPException(
        status_code=400,
        detail="Unsupported dataset format. Expecting a Hugging Face 'load_from_disk' folder or parquet/arrow files.",
    )


@lru_cache(maxsize=8)
def _load_hf_dataset(path: Path) -> DatasetDict | Dataset:
    """LRU-cache HF dataset objects to avoid reloading for every call."""
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


def _hf_split_files(split_ds: Dataset) -> List[Path]:
    """Best-effort extraction of on-disk shard files for a HF Dataset split."""
    files: set[Path] = set()
    cache_files = getattr(split_ds, "cache_files", None)
    if cache_files:
        for entry in cache_files:
            if not isinstance(entry, dict):
                continue
            filename = entry.get("filename") or entry.get("file_name") or entry.get("path")
            if filename:
                p = Path(filename)
                if p.exists():
                    files.add(p)
    return sorted(files, key=lambda p: str(p))


def _hf_split_to_arrow_table(split_ds: Dataset) -> pa.Table:
    """Obtain a pyarrow.Table for a HF split without converting through Python objects."""
    for attr in ("_data", "data"):
        data = getattr(split_ds, attr, None)
        if data is None:
            continue
        if isinstance(data, pa.Table):
            return data
        to_table = getattr(data, "to_table", None)
        if callable(to_table):
            return to_table()

    to_table = getattr(split_ds, "to_table", None)
    if callable(to_table):
        return to_table()
    raise HTTPException(status_code=500, detail="Could not obtain Arrow table from dataset")


def _file_dataset_to_relation_sql(path: Path, fmt: str, split: Optional[str]) -> str:
    """Return a DuckDB SQL snippet that reads the dataset lazily."""
    if fmt == "parquet":
        if path.is_file():
            pattern = str(path)
        else:
            pattern = str(path / "*.parquet")
        return f"SELECT * FROM read_parquet('{pattern}')"
    # Arrow IPC handled via register fallback when read_ipc is unavailable
    return ""


def _get_hf_split_paths(path: Path, split: str | None) -> List[Path]:
    """Return Arrow/Parquet file paths for the HF split without loading into memory."""
    candidate_dirs: list[Path] = []
    if split:
        candidate_dirs.append(path / split)
    candidate_dirs.extend([p for p in path.iterdir() if p.is_dir() and p.name == (split or p.name)])
    files: list[Path] = []
    for d in candidate_dirs:
        files.extend(sorted(d.glob("*.arrow")))
        files.extend(sorted(d.glob("*.parquet")))
        if files:
            break
    if not files:
        raise HTTPException(status_code=404, detail=f"No files found for split '{split}'")
    return files


def _create_relation(con: duckdb.DuckDBPyConnection, root: Path, name: str, split: Optional[str]) -> Tuple[str, str]:
    """Register a lazy relation in DuckDB named view_name and return (view_name, fmt)."""
    path = (root / name).resolve()
    try:
        path.relative_to(root)
    except ValueError:
        raise HTTPException(status_code=400, detail="Dataset path escapes the configured root")
    if not path.exists():
        raise HTTPException(status_code=404, detail="Dataset not found locally")

    fmt = _detect_format(path)

    if fmt == "hf":
        ds = _load_hf_dataset(path)
        split_ds = _select_split(ds, split)
        # Prefer using underlying shard files when possible; `cache_files` is the most reliable.
        files = _hf_split_files(split_ds)
        if not files:
            try:
                effective_split = (
                    split
                    or next(iter(ds.keys()))
                    if isinstance(ds, DatasetDict)
                    else split
                )
                files = _get_hf_split_paths(path, effective_split)
            except Exception:
                files = []

        parquet_files = [f for f in files if f.suffix == ".parquet"]
        arrow_files = [f for f in files if f.suffix == ".arrow"]
        if parquet_files:
            pattern = "{" + ",".join(str(f) for f in parquet_files) + "}"
            view_sql = f"SELECT * FROM read_parquet('{pattern}')"
        elif arrow_files:
            try:
                dataset = pyds.dataset([str(f) for f in arrow_files], format="ipc")
                reader = dataset.to_reader()
                con.register("t", reader)
                return "t", fmt
            except Exception:
                con.register("t", _hf_split_to_arrow_table(split_ds))
                return "t", fmt
        else:
            # Last-resort fallback: register an Arrow table view (avoid Python object materialisation).
            con.register("t", _hf_split_to_arrow_table(split_ds))
            return "t", fmt
    else:
        view_sql = _file_dataset_to_relation_sql(path, fmt, split)
        if not view_sql and fmt == "arrow":
            files = sorted(path.glob("*.arrow")) if path.is_dir() else [path]
            if not files:
                raise HTTPException(status_code=400, detail="No .arrow files found in dataset directory")
            try:
                dataset = pyds.dataset([str(f) for f in files], format="ipc")
                reader = dataset.to_reader()
                con.register("t", reader)
                return "t", fmt
            except ArrowInvalid:
                # Arrow streams: fall back to loading into a single table.
                tables = []
                import pyarrow.ipc as ipc

                for f in files:
                    with f.open("rb") as fh:
                        magic = fh.read(6)
                    if magic.startswith(b"\xff\xff\xff\xff"):
                        with ipc.open_stream(f) as r:
                            tables.append(r.read_all())
                    else:
                        with ipc.open_file(f) as r:
                            tables.append(r.read_all())
                merged = pa.concat_tables(tables, promote=True) if len(tables) > 1 else tables[0]
                con.register("t", merged)
                return "t", fmt

    if view_sql:
        con.execute(f"CREATE OR REPLACE VIEW t AS {view_sql}")
        return "t", fmt
    return "t", fmt


def _wrap_with_pagination(sql: str, limit: int, offset: int) -> Tuple[str, bool]:
    """Always wrap the user SQL to apply our limit/offset without mutating the inner query."""
    trimmed = sql.strip().rstrip(";")
    wrapped = f"SELECT * FROM ({trimmed}) AS _q LIMIT {limit} OFFSET {offset}"
    return wrapped, True


def _file_schema(path: Path, fmt: str) -> List[Dict[str, str]]:
    con = _acquire_con()
    try:
        _prepare_connection(con)
        view_name, _ = _create_relation(con, path.parent, path.name, None)
        result = con.execute(f"SELECT * FROM {view_name} LIMIT 0").arrow()
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    finally:
        _release_con(con)

    if hasattr(result, "to_table"):
        table = result.to_table()
    elif hasattr(result, "read_all"):
        table = result.read_all()
    else:
        table = result  # assume pyarrow.Table
    return [{"name": f.name, "dtype": str(f.type)} for f in table.schema]


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
    path = (resolved_root / name).resolve()
    try:
        path.relative_to(resolved_root)
    except ValueError:
        raise HTTPException(status_code=400, detail="Dataset path escapes the configured root")
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
    path = (resolved_root / name).resolve()
    try:
        path.relative_to(resolved_root)
    except ValueError:
        raise HTTPException(status_code=400, detail="Dataset path escapes the configured root")
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
    columns = _file_schema(path, fmt)
    return {"columns": columns, "approx_rows": None}


@app.get("/datasets/{name}/count", response_model=CountResponse)
def count_dataset(
    name: str,
    split: Optional[str] = Query(None),
    root: Optional[str] = Query(None, description="Dataset root directory"),
) -> CountResponse:
    resolved_root = _resolve_root(root)
    con = _acquire_con()
    try:
        _prepare_connection(con)
        view_name, _ = _create_relation(con, resolved_root, name, split)
        started = time.perf_counter()
        row_count = con.execute(f"SELECT COUNT(*) FROM {view_name}").fetchone()[0]
        _ = (time.perf_counter() - started)  # reserved for future metrics
    except Exception as exc:  # pragma: no cover - defensive
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    finally:
        _release_con(con)
    return CountResponse(rows=int(row_count))


@app.post("/datasets/{name}/query", response_model=QueryResponse)
def query_dataset(
    name: str,
    payload: QueryRequest,
    root: Optional[str] = Query(None, description="Dataset root directory"),
) -> QueryResponse:
    if not payload.sql.strip():
        raise HTTPException(status_code=400, detail="SQL is empty")

    requested_limit = payload.limit if payload.limit is not None else DEFAULT_LIMIT
    if requested_limit < 1:
        raise HTTPException(status_code=400, detail="limit must be >= 1")
    limit_to_apply = min(requested_limit, MAX_LIMIT)

    offset = payload.offset or 0
    if offset < 0:
        raise HTTPException(status_code=400, detail="offset must be >= 0")
    fetch_limit = limit_to_apply + 1  # fetch one extra row to detect truncation
    sql, _ = _wrap_with_pagination(payload.sql, fetch_limit, offset)

    resolved_root = _resolve_root(root)
    con = _acquire_con()
    try:
        _prepare_connection(con)
        view_name, _ = _create_relation(con, resolved_root, name, payload.split)

        started = time.perf_counter()
        result = con.execute(sql).arrow()
        elapsed_ms = (time.perf_counter() - started) * 1000

        # DuckDB 1.4+ returns a RecordBatchReader; earlier versions return a Table.
        if hasattr(result, "to_table"):
            result_table = result.to_table()
        elif hasattr(result, "read_all"):
            result_table = result.read_all()
        else:
            result_table = result  # assume pyarrow.Table

        truncated = result_table.num_rows > limit_to_apply
        if truncated:
            result_table = result_table.slice(0, limit_to_apply)

        return QueryResponse(
            columns=result_table.schema.names,
            data=[col.to_pylist() for col in result_table.columns],
            row_count=result_table.num_rows,
            truncated=truncated,
            elapsed_ms=elapsed_ms,
        )
    except Exception as exc:  # pragma: no cover - defensive
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    finally:
        _release_con(con)
