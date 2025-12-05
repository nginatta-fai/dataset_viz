import React, { useMemo } from "react";
import ReactECharts from "echarts-for-react";
import type { QueryResponse } from "../api";

type ChartKind = "table" | "bar" | "line" | "scatter" | "hist";

type Props = {
  data: QueryResponse | null;
  type: ChartKind;
  onTypeChange: (t: ChartKind) => void;
  xField: string | null;
  yField: string | null;
  onXChange: (f: string) => void;
  onYChange: (f: string) => void;
};

export const ChartView: React.FC<Props> = ({
  data,
  type,
  onTypeChange,
  xField,
  yField,
  onXChange,
  onYChange
}) => {
  const columns = data?.columns ?? [];
  const rows = data?.rows ?? [];

  const option = useMemo(() => {
    if (!data || !rows.length) return null;

    if (type === "hist") {
      if (!xField) return null;
      const values = rows
        .map((r) => r[xField])
        .filter((v): v is number => typeof v === "number");
      if (!values.length) return null;
      values.sort((a, b) => a - b);
      const bins = 20;
      const min = values[0];
      const max = values[values.length - 1];
      const width = (max - min || 1) / bins;
      const counts = new Array(bins).fill(0);
      values.forEach((v) => {
        const idx = Math.min(bins - 1, Math.floor((v - min) / width));
        counts[idx] += 1;
      });
      const labels = counts.map((_, i) => `${(min + i * width).toFixed(2)}`);
      return {
        tooltip: {},
        xAxis: { type: "category", data: labels, axisLabel: { formatter: "{value}" } },
        yAxis: { type: "value" },
        series: [{ type: "bar", data: counts, name: xField }]
      };
    }

    if (!xField || !yField) return null;
    const xData = rows.map((r) => r[xField]);
    const yData = rows.map((r) => r[yField]);

    const seriesData =
      type === "scatter"
        ? xData.map((x, i) => [x as number, yData[i] as number])
        : yData;

    const common = {
      tooltip: { trigger: "axis" },
      xAxis: { type: type === "scatter" ? "value" : "category", data: type === "scatter" ? undefined : xData },
      yAxis: { type: "value" }
    };

    if (type === "bar") {
      return {
        ...common,
        series: [{ type: "bar", data: seriesData, name: yField }]
      };
    }
    if (type === "line") {
      return {
        ...common,
        series: [{ type: "line", data: seriesData, name: yField, smooth: true }]
      };
    }
    return {
      tooltip: {},
      xAxis: { type: "value", name: xField },
      yAxis: { type: "value", name: yField },
      series: [{ type: "scatter", data: seriesData, name: `${xField} vs ${yField}` }]
    };
  }, [data, rows, type, xField, yField]);

  return (
    <div className="panel">
      <div className="grid" style={{ gap: 12 }}>
        <div className="chart-controls">
          <label>
            Chart
            <select className="select" value={type} onChange={(e) => onTypeChange(e.target.value as ChartKind)}>
              <option value="table">Table only</option>
              <option value="bar">Bar</option>
              <option value="line">Line</option>
              <option value="scatter">Scatter</option>
              <option value="hist">Histogram</option>
            </select>
          </label>

          <label>
            X field
            <select
              className="select"
              value={xField ?? ""}
              onChange={(e) => onXChange(e.target.value)}
              disabled={!columns.length}
            >
              <option value="" disabled>
                Select
              </option>
              {columns.map((c) => (
                <option key={c} value={c}>
                  {c}
                </option>
              ))}
            </select>
          </label>

          {type !== "hist" && (
            <label>
              Y field
              <select
                className="select"
                value={yField ?? ""}
                onChange={(e) => onYChange(e.target.value)}
                disabled={!columns.length}
              >
                <option value="" disabled>
                  Select
                </option>
                {columns.map((c) => (
                  <option key={c} value={c}>
                    {c}
                  </option>
                ))}
              </select>
            </label>
          )}
        </div>

        {type === "table" && <div style={{ color: "#475569", fontSize: 13 }}>Charts disabled (table only).</div>}
        {type !== "table" && !option && (
          <div className="error">Not enough numeric data or missing field selection for this chart.</div>
        )}
        {type !== "table" && option && <ReactECharts option={option} notMerge style={{ height: 360 }} />}
      </div>
    </div>
  );
};
