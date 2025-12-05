import React from "react";

type Props = {
  sql: string;
  onChange: (v: string) => void;
  onRun: () => void;
  limit: number;
  onLimitChange: (n: number) => void;
  disabled?: boolean;
};

export const SqlEditor: React.FC<Props> = ({ sql, onChange, onRun, limit, onLimitChange, disabled }) => {
  return (
    <div className="panel">
      <div className="grid" style={{ gap: 12 }}>
        <label>SQL (table is exposed as <code>t</code>)</label>
        <textarea
          className="textarea"
          value={sql}
          onChange={(e) => onChange(e.target.value)}
          spellCheck={false}
          disabled={disabled}
        />
        <div className="row" style={{ justifyContent: "space-between" }}>
          <div className="row" style={{ gap: 8 }}>
            <label>Default limit</label>
            <input
              className="input"
              type="number"
              min={1}
              value={limit}
              onChange={(e) => onLimitChange(Number(e.target.value))}
              style={{ width: 120 }}
              disabled={disabled}
            />
          </div>
          <button className="btn" onClick={onRun} disabled={disabled}>
            Run Query
          </button>
        </div>
      </div>
    </div>
  );
};
