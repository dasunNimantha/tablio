import { QueryResult } from "../../lib/tauri";
import "./QueryConsole.css";

interface Props {
  result: QueryResult;
}

export function ResultTable({ result }: Props) {
  return (
    <div className="result-table-wrapper">
      <table className="grid-table result-table">
        <thead>
          <tr>
            <th className="grid-row-number">#</th>
            {result.columns.map((col, i) => (
              <th key={i}>{col}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {result.rows.map((row, rowIdx) => (
            <tr key={rowIdx}>
              <td className="grid-row-number">{rowIdx + 1}</td>
              {row.map((val, colIdx) => {
                const isNull = val === null || val === undefined;
                return (
                  <td key={colIdx} className={isNull ? "cell-null" : ""}>
                    <span className="cell-value">
                      {isNull ? "NULL" : String(val)}
                    </span>
                  </td>
                );
              })}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
