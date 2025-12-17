export type ColumnInfo = { name: string; dtype: string };

export type DatasetList = { datasets: string[] };
export type SplitsList = { splits: string[] };
export type SchemaResponse = { columns: ColumnInfo[]; approx_rows: number | null };

export type QueryRequest = {
  sql: string;
  split?: string | null;
  limit?: number | null;
  offset?: number | null;
};

export type QueryResponse = {
  columns: string[];
  data: unknown[][];
  row_count: number;
  truncated: boolean;
  elapsed_ms: number;
};

export type CountResponse = { rows: number };

const BASE_URL = import.meta.env.VITE_BACKEND_URL || "http://127.0.0.1:8000";

const withRoot = (path: string, root?: string | null) =>
  root ? `${path}${path.includes("?") ? "&" : "?"}root=${encodeURIComponent(root)}` : path;

async function json<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`${BASE_URL}${path}`, {
    headers: { "Content-Type": "application/json" },
    ...init
  });
  if (!res.ok) {
    const text = await res.text();
    let message = text || res.statusText;
    try {
      const parsed: unknown = text ? JSON.parse(text) : null;
      if (parsed && typeof parsed === "object" && "detail" in parsed) {
        const detail = (parsed as { detail?: unknown }).detail;
        if (typeof detail === "string") message = detail;
        else message = JSON.stringify(detail);
      } else if (typeof parsed === "string") {
        message = parsed;
      }
    } catch {
      // non-JSON response; keep original text/status
    }
    throw new Error(message);
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
  count: (dataset: string, root: string, split?: string | null, signal?: AbortSignal) =>
    json<CountResponse>(
      withRoot(
        `/datasets/${encodeURIComponent(dataset)}/count${split ? `?split=${encodeURIComponent(split)}` : ""}`,
        root
      ),
      { signal }
    ),
  query: (dataset: string, root: string, body: QueryRequest, signal?: AbortSignal) =>
    json<QueryResponse>(withRoot(`/datasets/${encodeURIComponent(dataset)}/query`, root), {
      method: "POST",
      body: JSON.stringify(body),
      signal
    })
};
