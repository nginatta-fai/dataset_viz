# Dataset Visualizer (local only)

Run a small FastAPI + React app to inspect pre-downloaded Hugging Face datasets entirely offline.

## Prerequisites
- Python 3.11 with [`uv`](https://github.com/astral-sh/uv)
- Node.js 20 via `fnm`
- Datasets already on disk (no downloads at runtime)

## Layout
```
backend/    # FastAPI + DuckDB server
frontend/   # Vite + React UI
```

You are no longer tied to a fixed datasets folder. The UI asks for a dataset root path (e.g., `/home/user/data/hf`) and sends that to the backend on every request.
Note: browsers cannot disclose absolute filesystem paths via a folder picker for security. Enter/paste the path; if you need a true folder dialog, wrap the app in Tauri/Electron or run the backend CLI with `DATASETS_DIR` set.

Supported dataset formats:
- Hugging Face `load_from_disk` folders (contain `dataset_info.json` / `state.json`)
- Flat `.parquet` or `.arrow` files (treated as a single split named `default`)

## Backend
```bash
cd backend
uv sync
uv run uvicorn dataset_viz.app:app --host 127.0.0.1 --port 8000 --reload
```
Environment knobs:
- `DATASETS_DIR` default root if UI doesn't provide one (still offline only)
- `QUERY_DEFAULT_LIMIT` to change the auto-added LIMIT (default 1000)
- `QUERY_MAX_LIMIT` to cap per-request limit for UI responsiveness (default 5000)
`HF_DATASETS_OFFLINE` is forced to `1`.

## Frontend
```bash
cd frontend
fnm use 20
npm install
npm run dev
```
Vite serves on `http://127.0.0.1:5173` and talks to the backend at `http://127.0.0.1:8000`.

## Usage
1. Start backend and frontend as above.
2. In the UI, paste the folder path that contains your datasets (each dataset is a subdirectory or Parquet/Arrow files) and click “Load datasets”.
3. Pick dataset + split, inspect schema, edit SQL (table exposed as `t`), run, view table and charts.

Queries run through DuckDB in-process over Arrow tables for speed; default LIMIT is applied if absent to avoid huge payloads.
