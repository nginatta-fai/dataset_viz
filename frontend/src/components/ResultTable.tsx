import React, { useMemo } from "react";
import {
  ColumnDef,
  flexRender,
  getCoreRowModel,
  useReactTable
} from "@tanstack/react-table";

type Props = {
  columns: string[];
  rows: Record<string, unknown>[];
};

export const ResultTable: React.FC<Props> = ({ columns, rows }) => {
  const defs = useMemo<ColumnDef<Record<string, unknown>>[]>(
    () =>
      columns.map((key) => ({
        id: key,
        accessorKey: key,
        header: key,
        cell: (info) => {
          const value = info.getValue();
          if (value === null || value === undefined) return "∅";
          if (typeof value === "object") return JSON.stringify(value);
          return String(value);
        }
      })),
    [columns]
  );

  const table = useReactTable({
    data: rows,
    columns: defs,
    getCoreRowModel: getCoreRowModel()
  });

  if (!rows.length) {
    return <div style={{ padding: 12, color: "#94a3b8" }}>No rows returned</div>;
  }

  return (
    <div className="table-wrapper">
      <table style={{ width: "100%", borderCollapse: "collapse", minWidth: "400px" }}>
        <thead>
          {table.getHeaderGroups().map((hg) => (
            <tr key={hg.id} style={{ background: "#f8fafc" }}>
              {hg.headers.map((header) => (
                <th
                  key={header.id}
                  style={{
                    textAlign: "left",
                    padding: "8px 10px",
                    borderBottom: "1px solid #e2e8f0",
                    fontSize: 13,
                    fontWeight: 700
                  }}
                >
                  {flexRender(header.column.columnDef.header, header.getContext())}
                </th>
              ))}
            </tr>
          ))}
        </thead>
        <tbody>
          {table.getRowModel().rows.map((row) => (
            <tr key={row.id}>
              {row.getVisibleCells().map((cell) => (
                <td
                  key={cell.id}
                  style={{
                    padding: "8px 10px",
                    borderBottom: "1px solid #f1f5f9",
                    fontSize: 13
                  }}
                >
                  {flexRender(cell.column.columnDef.cell, cell.getContext())}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
};
