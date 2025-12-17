import React from "react";

type Props = {
  sql: string;
  onChange: (v: string) => void;
  onRun: () => void;
  disabled?: boolean;
};

export const SqlEditor: React.FC<Props> = ({
  sql,
  onChange,
  onRun,
  disabled
}) => {
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
        <button className="btn" onClick={() => onRun()} disabled={disabled} style={{ width: "100%" }}>
          Run Query
        </button>
      </div>
    </div>
  );
};
