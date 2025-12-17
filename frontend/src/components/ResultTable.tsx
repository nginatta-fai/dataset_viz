import React, { useEffect, useMemo, useRef, useState } from "react";

type Props = {
  columns: string[];
  data: unknown[][];
  rowCount: number;
};

const ROW_HEIGHT = 36;
const OVERSCAN = 8;
const DEFAULT_MAX_COLUMNS = 32;

type CellRef = { row: number; col: number } | null;

const summarizeValue = (value: unknown) => {
  if (value === null || value === undefined) return "∅";
  if (typeof value === "string") return value;
  if (typeof value === "number" || typeof value === "boolean" || typeof value === "bigint") return String(value);
  if (value instanceof Date) return value.toISOString();
  if (Array.isArray(value)) return `[${value.length} items]`;
  if (typeof value === "object") return "{…}";
  return String(value);
};

const formatFullValue = (value: unknown) => {
  if (value === null || value === undefined) return "∅";
  if (typeof value === "string") return value;
  if (typeof value === "number" || typeof value === "boolean" || typeof value === "bigint") return String(value);
  if (value instanceof Date) return value.toISOString();
  try {
    return JSON.stringify(value, null, 2);
  } catch {
    return String(value);
  }
};

export const ResultTable: React.FC<Props> = ({ columns, data, rowCount }) => {
  const wrapperRef = useRef<HTMLDivElement | null>(null);
  const scrollTopRef = useRef(0);
  const rafRef = useRef<number | null>(null);

  const [viewportHeight, setViewportHeight] = useState(360);
  const [scrollTop, setScrollTop] = useState(0);
  const [maxColumns, setMaxColumns] = useState(DEFAULT_MAX_COLUMNS);
  const [cellView, setCellView] = useState<CellRef>(null);

  const safeMaxColumns = useMemo(() => {
    if (!columns.length) return 0;
    const n = Number.isFinite(maxColumns) ? Math.floor(maxColumns) : DEFAULT_MAX_COLUMNS;
    return Math.max(1, Math.min(columns.length, n));
  }, [columns.length, maxColumns]);

  const visibleColumns = useMemo(() => columns.slice(0, safeMaxColumns), [columns, safeMaxColumns]);
  const visibleData = useMemo(() => data.slice(0, safeMaxColumns), [data, safeMaxColumns]);

  const inferredRows = visibleData[0]?.length ?? 0;
  const safeRowCount = Math.min(rowCount, inferredRows);

  const range = useMemo(() => {
    const start = Math.max(0, Math.floor(scrollTop / ROW_HEIGHT) - OVERSCAN);
    const end = Math.min(
      safeRowCount,
      Math.ceil((scrollTop + viewportHeight) / ROW_HEIGHT) + OVERSCAN
    );
    return { start, end };
  }, [safeRowCount, scrollTop, viewportHeight]);

  const topPad = range.start * ROW_HEIGHT;
  const bottomPad = (safeRowCount - range.end) * ROW_HEIGHT;

  useEffect(() => {
    const el = wrapperRef.current;
    if (!el) return;
    const update = () => setViewportHeight(el.clientHeight);
    update();
    if (typeof ResizeObserver === "undefined") {
      window.addEventListener("resize", update);
      return () => window.removeEventListener("resize", update);
    }
    const ro = new ResizeObserver(update);
    ro.observe(el);
    return () => ro.disconnect();
  }, []);

  useEffect(() => {
    if (!columns.length) return;
    setMaxColumns((prev) => Math.min(prev, columns.length));
  }, [columns.length]);

  useEffect(() => {
    const el = wrapperRef.current;
    if (!el) return;
    el.scrollTop = 0;
    scrollTopRef.current = 0;
    setScrollTop(0);
    setCellView(null);
  }, [columns, rowCount]);

  useEffect(() => {
    return () => {
      if (rafRef.current !== null) cancelAnimationFrame(rafRef.current);
    };
  }, []);

  const handleScroll = (e: React.UIEvent<HTMLDivElement>) => {
    scrollTopRef.current = e.currentTarget.scrollTop;
    if (rafRef.current !== null) return;
    rafRef.current = requestAnimationFrame(() => {
      rafRef.current = null;
      setScrollTop(scrollTopRef.current);
    });
  };

  const getValue = (row: number, col: number) => visibleData[col]?.[row];

  const cellText = useMemo(() => {
    if (!cellView) return null;
    return formatFullValue(getValue(cellView.row, cellView.col));
  }, [cellView, visibleData]);

  if (!safeRowCount) {
    return <div style={{ padding: 12, color: "#94a3b8" }}>No rows returned</div>;
  }

  return (
    <div className="table-shell">
      <div className="table-toolbar">
        <div style={{ fontSize: 12, color: "#475569" }}>
          Showing {visibleColumns.length.toLocaleString()} / {columns.length.toLocaleString()} columns ·{" "}
          {safeRowCount.toLocaleString()} rows
        </div>
        <div className="row" style={{ gap: 8 }}>
          <label style={{ fontSize: 12, fontWeight: 600, color: "#475569" }}>Max columns</label>
          <input
            className="input"
            type="number"
            min={1}
            max={columns.length}
            value={safeMaxColumns}
            onChange={(e) => setMaxColumns(Number(e.target.value))}
            style={{ width: 110, padding: "6px 10px" }}
          />
          <button
            className="btn secondary"
            onClick={() => setMaxColumns(columns.length)}
            disabled={safeMaxColumns >= columns.length}
            style={{ padding: "6px 10px", borderRadius: 8 }}
          >
            All
          </button>
        </div>
      </div>

      <div className="table-wrapper" ref={wrapperRef} onScroll={handleScroll}>
        <table className="result-table">
          <thead>
            <tr>
              {visibleColumns.map((c) => (
                <th key={c} title={c}>
                  {c}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {topPad > 0 && (
              <tr style={{ height: topPad }}>
                <td colSpan={visibleColumns.length} className="spacer-cell" />
              </tr>
            )}
            {Array.from({ length: range.end - range.start }, (_, i) => range.start + i).map((rowIdx) => (
              <tr key={rowIdx} style={{ height: ROW_HEIGHT }}>
                {visibleColumns.map((c, colIdx) => {
                  const value = getValue(rowIdx, colIdx);
                  const text = summarizeValue(value);
                  return (
                    <td
                      key={`${rowIdx}:${c}`}
                      onClick={() => setCellView({ row: rowIdx, col: colIdx })}
                      title={typeof value === "string" ? value : undefined}
                    >
                      {text}
                    </td>
                  );
                })}
              </tr>
            ))}
            {bottomPad > 0 && (
              <tr style={{ height: bottomPad }}>
                <td colSpan={visibleColumns.length} className="spacer-cell" />
              </tr>
            )}
          </tbody>
        </table>
      </div>

      {cellView && (
        <div
          className="modal-overlay"
          onClick={() => setCellView(null)}
          role="dialog"
          aria-modal="true"
        >
          <div className="modal" onClick={(e) => e.stopPropagation()}>
            <div className="row" style={{ justifyContent: "space-between", marginBottom: 10 }}>
              <div style={{ fontWeight: 700 }}>
                {visibleColumns[cellView.col] ?? "Value"} · row {cellView.row + 1}
              </div>
              <button className="btn secondary" onClick={() => setCellView(null)} style={{ padding: "6px 10px" }}>
                Close
              </button>
            </div>
            <pre className="modal-pre">{cellText ?? ""}</pre>
          </div>
        </div>
      )}
    </div>
  );
};
