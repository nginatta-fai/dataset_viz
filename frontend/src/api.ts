export type ColumnInfo = { name: string; dtype: string };

export type DatasetList = { datasets: string[] };
export type SplitsList = { splits: string[] };
export type SchemaResponse = { columns: ColumnInfo[]; approx_rows: number | null };

export type QueryRequest = {
  sql: string;
  split?: string | null;
  limit?: number | null;
};

export type QueryResponse = {
  columns: string[];
  rows: Record<string, unknown>[];
  row_count: number;
  truncated: boolean;
  elapsed_ms: number;
};

const BASE_URL = "http://127.0.0.1:8000";

const withRoot = (path: string, root?: string | null) =>
  root ? `${path}${path.includes("?") ? "&" : "?"}root=${encodeURIComponent(root)}` : path;

async function json<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`${BASE_URL}${path}`, {
    headers: { "Content-Type": "application/json" },
    ...init
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(text || res.statusText);
  }
  return res.json() as Promise<T>;
}

export const api = {
  listDatasets: (root: string) => json<DatasetList>(withRoot("/datasets", root)),
  listSplits: (dataset: string, root: string) =>
    json<SplitsList>(withRoot(`/datasets/${encodeURIComponent(dataset)}/splits`, root)),
  schema: (dataset: string, root: string, split?: string | null) =>
    json<SchemaResponse>(
      withRoot(
        `/datasets/${encodeURIComponent(dataset)}/schema${split ? `?split=${encodeURIComponent(split)}` : ""}`,
        root
      )
    ),
  query: (dataset: string, root: string, body: QueryRequest) =>
    json<QueryResponse>(withRoot(`/datasets/${encodeURIComponent(dataset)}/query`, root), {
      method: "POST",
      body: JSON.stringify(body)
    })
};
