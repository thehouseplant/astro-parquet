import { useState, useEffect } from 'react';
import * as duckdb from '@duckdb/duckdb-wasm';
import duckdb_wasm from '@duckdb/duckdb-wasm/dist/duckdb-mvp.wasm?url';
import mvp_worker from '@duckdb/duckdb-wasm/dist/duckdb-browser-mvp.worker.js?url';

async function initializeDuckDB() {
  const DUCKDB_BUNDLES = {
    mvp: {
      mainModule: duckdb_wasm,
      mainWorker: mvp_worker,
    }
  };

  const worker = new Worker(DUCKDB_BUNDLES.mvp.mainWorker);
  const logger = new duckdb.ConsoleLogger();
  const db = new duckdb.AsyncDuckDB(logger, worker);
  await db.instantiate(DUCKDB_BUNDLES.mvp.mainModule);
  return db;
}

export default function ParquetViewer() {
  const [data, setData] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    async function loadData() {
      try {
        const db = await initializeDuckDB();

        const conn = await db.connect();
        await conn.query(`
          INSTALL httpfs;
          LOAD httpfs;
          SET s3_region='us-east-1';
        `);

        const result = await conn.query(`
          SELECT *
          FROM read_parquet('s3://your-bucket/path/to/data.parquet')
          LIMIT 100;
        `);

        setData(result.toArray());
        await conn.close();
        await db.terminate();
      } catch (err) {
        setError(err.message);
      } finally {
        setLoading(false);
      }
    }

    loadData();
  }, []);

  if (loading) return <div>Loading data...</div>;
  if (error) return <div>Error: {error}</div>;

  return (
    <div className="overflow-x-auto">
      <table className="min-w-full table-auto">
        <thead>
          <tr>
            {Object.keys(data[0] || {}).map((header) => (
              <th key={header} className="px-4 py-2 bg-gray-100">
                {header}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {data.map((row, i) => (
            <tr key={i}>
              {Object.values(row).map((value, j) => (
                <td key={j} className="border px-4 py-2">
                  {value?.toString()}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
