import React, { useEffect, useMemo, useRef, useState } from "react";
import { api, ColumnInfo, QueryResponse } from "./api";
import { DatasetPicker } from "./components/DatasetPicker";
import { SqlEditor } from "./components/SqlEditor";
import { ResultTable } from "./components/ResultTable";

const DEFAULT_SQL = "SELECT * FROM t;";

function App() {
  const [datasets, setDatasets] = useState<string[]>([]);
  const [datasetRoot, setDatasetRoot] = useState<string>("");
  const [selectedDataset, setSelectedDataset] = useState<string | null>(null);
  const [splits, setSplits] = useState<string[]>([]);
  const [selectedSplit, setSelectedSplit] = useState<string | null>(null);
  const [schema, setSchema] = useState<ColumnInfo[]>([]);
  const [sql, setSql] = useState(DEFAULT_SQL);
  const [limit, setLimit] = useState<number>(500);
  const [offset, setOffset] = useState<number>(0);
  const [result, setResult] = useState<QueryResponse | null>(null);
  const [totalCount, setTotalCount] = useState<number | null>(null);
  const [loading, setLoading] = useState(false);
  const [countLoading, setCountLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [autoRunPending, setAutoRunPending] = useState(false);

  const queryAbortRef = useRef<AbortController | null>(null);
  const countAbortRef = useRef<AbortController | null>(null);
  const querySeq = useRef(0);
  const countSeq = useRef(0);

  useEffect(() => {
    queryAbortRef.current?.abort();
    queryAbortRef.current = null;
    countAbortRef.current?.abort();
    countAbortRef.current = null;
    setLoading(false);
    setCountLoading(false);
    setDatasets([]);
    setSelectedDataset(null);
    setSplits([]);
    setSelectedSplit(null);
    setSchema([]);
    setResult(null);
    setOffset(0);
    setTotalCount(null);
    setAutoRunPending(false);
  }, [datasetRoot]);

  const loadDatasets = () => {
    if (!datasetRoot.trim()) {
      setError("Choose a dataset folder path first.");
      return;
    }
    setError(null);
    api
      .listDatasets(datasetRoot)
      .then((res) => {
        setDatasets(res.datasets);
        const first = res.datasets[0] ?? null;
        setSelectedDataset(first);
        setAutoRunPending(Boolean(first));
      })
      .catch((err) => setError(err.message));
  };

  useEffect(() => {
    if (!selectedDataset || !datasetRoot) return;
    queryAbortRef.current?.abort();
    countAbortRef.current?.abort();
    setError(null);
    setResult(null);
    setOffset(0);
    setTotalCount(null);
    setSchema([]);
    setSplits([]);
    setSelectedSplit(null);
    api
      .listSplits(selectedDataset, datasetRoot)
      .then((res) => {
        setSplits(res.splits);
        const next = res.splits[0] ?? null;
        setSelectedSplit(next);
        setAutoRunPending(Boolean(next));
      })
      .catch((err) => {
        setSchema([]);
        setSplits([]);
        setSelectedSplit(null);
        setError(err.message);
      });
  }, [selectedDataset, datasetRoot]);

  useEffect(() => {
    if (!selectedDataset || !selectedSplit || !datasetRoot) return;
    setOffset(0);
    setTotalCount(null);
    api
      .schema(selectedDataset, datasetRoot, selectedSplit)
      .then((schemaRes) => {
        setSchema(schemaRes.columns);
        if (schemaRes.approx_rows !== null) setTotalCount(schemaRes.approx_rows);
      })
      .catch((err) => setError(err.message));
  }, [selectedSplit, selectedDataset, datasetRoot]);

  const runQuery = async (sqlOverride?: string, offsetOverride?: number) => {
    if (!selectedDataset || !datasetRoot) {
      setError("Pick a dataset and dataset folder first.");
      return;
    }
    const sqlToUse = sqlOverride ?? sql;
    const offsetToUse = offsetOverride ?? offset;
    const requestId = ++querySeq.current;

    queryAbortRef.current?.abort();
    const controller = new AbortController();
    queryAbortRef.current = controller;

    setLoading(true);
    setError(null);
    try {
      const res = await api.query(selectedDataset, datasetRoot, {
        sql: sqlToUse,
        split: selectedSplit,
        limit,
        offset: offsetToUse
      }, controller.signal);
      if (requestId !== querySeq.current) return;
      setResult(res);
    } catch (err: unknown) {
      if (controller.signal.aborted) return;
      if (requestId !== querySeq.current) return;
      setResult(null);
      setError(err instanceof Error ? err.message : "Query failed");
    } finally {
      if (requestId === querySeq.current) setLoading(false);
    }
  };

  const fetchCount = async () => {
    if (!selectedDataset || !datasetRoot) {
      setError("Pick a dataset and dataset folder first.");
      return;
    }
    const requestId = ++countSeq.current;

    countAbortRef.current?.abort();
    const controller = new AbortController();
    countAbortRef.current = controller;

    setCountLoading(true);
    setError(null);
    try {
      const res = await api.count(selectedDataset, datasetRoot, selectedSplit, controller.signal);
      if (requestId !== countSeq.current) return;
      setTotalCount(res.rows);
    } catch (err: unknown) {
      if (controller.signal.aborted) return;
      if (requestId !== countSeq.current) return;
      setTotalCount(null);
      setError(err instanceof Error ? err.message : "Count failed");
    } finally {
      if (requestId === countSeq.current) setCountLoading(false);
    }
  };

  // Auto-run default query once when a dataset/split becomes available.
  useEffect(() => {
    if (autoRunPending && selectedDataset && selectedSplit && datasetRoot && !loading) {
      setSql(DEFAULT_SQL);
      setAutoRunPending(false);
      runQuery(DEFAULT_SQL);
    }
  }, [autoRunPending, selectedDataset, selectedSplit, datasetRoot, loading]);

  const info = useMemo(() => {
    if (!result) return null;
    const start = result.row_count > 0 ? offset + 1 : offset;
    const end = offset + result.row_count;
    let span: string | null = null;
    if (result.row_count > 0) {
      const base = `${start}-${end}`;
      if (totalCount !== null) span = `${base} of ${totalCount}`;
      else if (result.truncated) span = `${base}+`;
      else span = base;
    }
    return [
      span,
      `${result.row_count} rows`,
      `${result.elapsed_ms.toFixed(1)} ms`,
      result.truncated ? "truncated" : null
    ]
      .filter(Boolean)
      .join(" · ");
  }, [result, offset, totalCount]);

  const hasNext = useMemo(() => {
    if (!result) return false;
    if (totalCount !== null) return offset + result.row_count < totalCount;
    return result.truncated;
  }, [result, offset, totalCount]);

  const handleLimitChange = (value: number) => {
    setLimit(value);
    setOffset(0);
  };

  return (
    <div className="app">
      <div style={{ marginBottom: 16 }}>
        <h1 style={{ margin: 0 }}>Dataset Visualizer</h1>
      </div>

      <div className="panel" style={{ marginBottom: 12 }}>
        <div className="row" style={{ gap: 12, alignItems: "flex-end" }}>
          <div style={{ flex: 1 }}>
            <label>Dataset folder path</label>
            <input
              className="input"
              type="text"
              placeholder="/path/to/your/datasets"
              value={datasetRoot}
              onChange={(e) => setDatasetRoot(e.target.value)}
            />
          </div>
          <button className="btn align-input" onClick={loadDatasets} style={{ whiteSpace: "nowrap" }}>
            Load datasets
          </button>
        </div>
        <div style={{ fontSize: 12, color: "#94a3b8", marginTop: 6 }}>
          Paste a local path containing your datasets (each dataset is a subfolder). Browser file pickers cannot reveal
          absolute paths without a desktop wrapper.
        </div>
      </div>

      <div className="layout">
        <div className="side-column">
          <DatasetPicker
            datasets={datasets}
            selectedDataset={selectedDataset}
            onDatasetChange={(d) => setSelectedDataset(d)}
            splits={splits}
            selectedSplit={selectedSplit}
            onSplitChange={(s) => setSelectedSplit(s)}
            schema={schema}
          />
          <SqlEditor
            sql={sql}
            onChange={setSql}
            onRun={runQuery}
            limit={limit}
            onLimitChange={handleLimitChange}
            offset={offset}
            onOffsetChange={setOffset}
            disabled={!selectedDataset || loading}
          />
          {error && <div className="error">{error}</div>}
        </div>

        <div className="main-column">
          <div className="panel result-panel">
            <div className="row" style={{ justifyContent: "space-between" }}>
              <div className="result-meta">
                <span style={{ fontWeight: 700 }}>Results</span>
                {info && <span>{info}</span>}
                {totalCount !== null && <span>Total: {totalCount.toLocaleString()}</span>}
              </div>
              <div className="row" style={{ gap: 8 }}>
                <button
                  className="btn secondary"
                  onClick={fetchCount}
                  disabled={!selectedDataset || countLoading || loading}
                  style={{ minWidth: 110 }}
                >
                  {countLoading ? "Counting..." : "Count rows"}
                </button>
                <button
                  className="btn secondary"
                  onClick={() => {
                    const nextOffset = Math.max(0, offset - limit);
                    setOffset(nextOffset);
                    runQuery(undefined, nextOffset);
                  }}
                  disabled={!result || offset === 0 || loading}
                  style={{ minWidth: 90 }}
                >
                  Prev
                </button>
                <button
                  className="btn secondary"
                  onClick={() => {
                    const nextOffset = offset + limit;
                    setOffset(nextOffset);
                    runQuery(undefined, nextOffset);
                  }}
                  disabled={!result || !hasNext || loading}
                  style={{ minWidth: 90 }}
                >
                  Next
                </button>
              </div>
            </div>
            <div className="table-area">
              {result && <ResultTable columns={result.columns} data={result.data} rowCount={result.row_count} />}
              {!result && <div style={{ paddingTop: 8, color: "#94a3b8" }}>Run a query to see results</div>}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

export default App;
