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
        <div className="row" style={{ justifyContent: "flex-end" }}>
          <button className="btn" onClick={() => onRun()} disabled={disabled}>
            Run Query
          </button>
        </div>
      </div>
    </div>
  );
};
