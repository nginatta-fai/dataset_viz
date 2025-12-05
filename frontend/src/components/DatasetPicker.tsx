import React from "react";
import type { ColumnInfo } from "../api";

type Props = {
  datasets: string[];
  selectedDataset: string | null;
  onDatasetChange: (name: string) => void;
  splits: string[];
  selectedSplit: string | null;
  onSplitChange: (name: string) => void;
  schema: ColumnInfo[];
};

export const DatasetPicker: React.FC<Props> = ({
  datasets,
  selectedDataset,
  onDatasetChange,
  splits,
  selectedSplit,
  onSplitChange,
  schema
}) => {
  return (
    <div className="panel">
      <div className="grid" style={{ gap: 12 }}>
        <div>
          <label>Dataset</label>
          <select
            className="select"
            value={selectedDataset ?? ""}
            onChange={(e) => onDatasetChange(e.target.value)}
          >
            <option value="" disabled>
              Select dataset
            </option>
            {datasets.map((d) => (
              <option key={d} value={d}>
                {d}
              </option>
            ))}
          </select>
        </div>

        <div>
          <label>Split</label>
          <select
            className="select"
            value={selectedSplit ?? ""}
            onChange={(e) => onSplitChange(e.target.value)}
            disabled={!splits.length}
          >
            <option value="" disabled>
              Select split
            </option>
            {splits.map((s) => (
              <option key={s} value={s}>
                {s}
              </option>
            ))}
          </select>
        </div>

        <div>
          <label>Schema</label>
          <div className="schema-list">
            {!schema.length && <div style={{ color: "#94a3b8" }}>No columns loaded</div>}
            {schema.map((col) => (
              <div className="schema-item" key={col.name}>
                <span>{col.name}</span>
                <span style={{ color: "#475569" }}>{col.dtype}</span>
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
};
