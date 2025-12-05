import React, { useEffect, useMemo, useState } from "react";
import { api, ColumnInfo, QueryResponse } from "./api";
import { DatasetPicker } from "./components/DatasetPicker";
import { SqlEditor } from "./components/SqlEditor";
import { ResultTable } from "./components/ResultTable";
import { ChartView } from "./components/ChartView";

const DEFAULT_SQL = "SELECT * FROM t LIMIT 100;";

function App() {
  const [datasets, setDatasets] = useState<string[]>([]);
  const [datasetRoot, setDatasetRoot] = useState<string>("");
  const [selectedDataset, setSelectedDataset] = useState<string | null>(null);
  const [splits, setSplits] = useState<string[]>([]);
  const [selectedSplit, setSelectedSplit] = useState<string | null>(null);
  const [schema, setSchema] = useState<ColumnInfo[]>([]);
  const [sql, setSql] = useState(DEFAULT_SQL);
  const [limit, setLimit] = useState<number>(500);
  const [result, setResult] = useState<QueryResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [chartType, setChartType] = useState<"table" | "bar" | "line" | "scatter" | "hist">("table");
  const [xField, setXField] = useState<string | null>(null);
  const [yField, setYField] = useState<string | null>(null);

  useEffect(() => {
    setDatasets([]);
    setSelectedDataset(null);
    setSplits([]);
    setSelectedSplit(null);
    setSchema([]);
    setResult(null);
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
        setSelectedDataset(res.datasets[0] ?? null);
      })
      .catch((err) => setError(err.message));
  };

  useEffect(() => {
    if (!selectedDataset || !datasetRoot) return;
    setError(null);
    setResult(null);
    api
      .listSplits(selectedDataset, datasetRoot)
      .then((res) => {
        setSplits(res.splits);
        const next = res.splits[0] ?? null;
        setSelectedSplit(next);
        if (next) {
          return api.schema(selectedDataset, datasetRoot, next);
        }
        return { columns: [], approx_rows: null };
      })
      .then((schemaRes) => setSchema(schemaRes.columns))
      .catch((err) => setError(err.message));
  }, [selectedDataset, datasetRoot]);

  useEffect(() => {
    if (!selectedDataset || !selectedSplit || !datasetRoot) return;
    api
      .schema(selectedDataset, datasetRoot, selectedSplit)
      .then((schemaRes) => setSchema(schemaRes.columns))
      .catch((err) => setError(err.message));
  }, [selectedSplit, selectedDataset, datasetRoot]);

  useEffect(() => {
    if (schema.length) {
      setXField(schema[0]?.name ?? null);
      setYField(schema[1]?.name ?? null);
    }
  }, [schema]);

  const runQuery = async () => {
    if (!selectedDataset || !datasetRoot) {
      setError("Pick a dataset and dataset folder first.");
      return;
    }
    setLoading(true);
    setError(null);
    try {
      const res = await api.query(selectedDataset, datasetRoot, {
        sql,
        split: selectedSplit,
        limit
      });
      setResult(res);
    } catch (err: unknown) {
      setResult(null);
      setError(err instanceof Error ? err.message : "Query failed");
    } finally {
      setLoading(false);
    }
  };

  const info = useMemo(() => {
    if (!result) return null;
    return [
      `${result.row_count} rows`,
      `${result.elapsed_ms.toFixed(1)} ms`,
      result.truncated ? "truncated" : null
    ]
      .filter(Boolean)
      .join(" · ");
  }, [result]);

  return (
    <div className="app">
      <div style={{ marginBottom: 16 }}>
        <h1 style={{ margin: 0 }}>Dataset Visualizer</h1>
        <div style={{ color: "#475569" }}>Local-only, DuckDB powered</div>
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
            <div style={{ fontSize: 12, color: "#94a3b8", marginTop: 4 }}>
              Paste a local path containing your datasets (each dataset is a subfolder). Browser file pickers cannot
              reveal absolute paths without a desktop wrapper.
            </div>
          </div>
          <button className="btn" onClick={loadDatasets} style={{ whiteSpace: "nowrap" }}>
            Load datasets
          </button>
        </div>
      </div>

      <div className="layout">
        <div className="grid" style={{ gap: 12 }}>
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
            onLimitChange={setLimit}
            disabled={!selectedDataset || loading}
          />
          {error && <div className="error">{error}</div>}
        </div>

        <div className="grid" style={{ gap: 12 }}>
          <div className="panel">
            <div className="row" style={{ justifyContent: "space-between" }}>
              <div className="result-meta">
                <span style={{ fontWeight: 700 }}>Results</span>
                {info && <span>{info}</span>}
              </div>
              <button className="btn secondary" onClick={() => setChartType("table")}>
                Table
              </button>
            </div>
            {result && <ResultTable columns={result.columns} rows={result.rows} />}
            {!result && <div style={{ paddingTop: 8, color: "#94a3b8" }}>Run a query to see results</div>}
          </div>

          <ChartView
            data={result}
            type={chartType}
            onTypeChange={setChartType}
            xField={xField}
            yField={yField}
            onXChange={setXField}
            onYChange={setYField}
          />
        </div>
      </div>
    </div>
  );
}

export default App;
