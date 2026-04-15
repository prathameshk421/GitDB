import React, { useEffect, useMemo, useState } from "react";
import { getCommits, getDiff, getSnapshot, getStatus, postCheckout } from "./api.js";

function Sidebar({ page, setPage }) {
  const items = [
    { id: "graph", label: "Commit Graph" },
    { id: "diff", label: "Diff Viewer" },
    { id: "schema", label: "Schema Browser" }
  ];
  return (
    <div className="w-64 shrink-0 border-r border-slate-200 bg-white p-4">
      <div className="text-xl font-semibold">GitDB</div>
      <div className="mt-6 flex flex-col gap-2">
        {items.map((it) => (
          <button
            key={it.id}
            className={`rounded px-3 py-2 text-left ${
              page === it.id ? "bg-slate-900 text-white" : "hover:bg-slate-100"
            }`}
            onClick={() => setPage(it.id)}
          >
            {it.label}
          </button>
        ))}
      </div>
    </div>
  );
}

function Card({ title, children }) {
  return (
    <div className="rounded-xl border border-slate-200 bg-white p-4 shadow-sm">
      <div className="text-sm font-semibold text-slate-700">{title}</div>
      <div className="mt-3">{children}</div>
    </div>
  );
}

function CommitGraphView({ commits, onCheckout }) {
  return (
    <div className="grid gap-4">
      <Card title="Commits">
        <div className="space-y-3">
          {commits.map((c) => (
            <div
              key={c.hash}
              className="flex items-center justify-between rounded-lg border border-slate-200 p-3"
            >
              <div>
                <div className="font-mono text-sm">{c.hash.slice(0, 12)}</div>
                <div className="text-sm text-slate-700">{c.message}</div>
                <div className="text-xs text-slate-500">
                  {c.full_name} ({c.username}) · {c.created_at}
                </div>
              </div>
              <button
                className="rounded bg-slate-900 px-3 py-2 text-sm text-white hover:bg-slate-800"
                onClick={() => onCheckout(c.hash)}
              >
                Checkout
              </button>
            </div>
          ))}
          {commits.length === 0 && (
            <div className="text-sm text-slate-600">No commits yet.</div>
          )}
        </div>
      </Card>
    </div>
  );
}

function DiffViewerView({ commits }) {
  const [h1, setH1] = useState("");
  const [h2, setH2] = useState("");
  const [mode, setMode] = useState("both");
  const [data, setData] = useState(null);
  const [err, setErr] = useState("");
  const [loading, setLoading] = useState(false);

  async function run() {
    setErr("");
    setLoading(true);
    try {
      const res = await getDiff(h1, h2);
      setData(res);
    } catch (e) {
      setErr(String(e));
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="grid gap-4">
      <Card title="Select commits">
        <div className="grid grid-cols-1 gap-3 md:grid-cols-4">
          <select
            className="rounded border border-slate-300 px-2 py-2"
            value={h1}
            onChange={(e) => setH1(e.target.value)}
          >
            <option value="">hash1…</option>
            {commits.map((c) => (
              <option key={c.hash} value={c.hash}>
                {c.hash.slice(0, 12)} — {c.message}
              </option>
            ))}
          </select>
          <select
            className="rounded border border-slate-300 px-2 py-2"
            value={h2}
            onChange={(e) => setH2(e.target.value)}
          >
            <option value="">hash2…</option>
            {commits.map((c) => (
              <option key={c.hash} value={c.hash}>
                {c.hash.slice(0, 12)} — {c.message}
              </option>
            ))}
          </select>
          <select
            className="rounded border border-slate-300 px-2 py-2"
            value={mode}
            onChange={(e) => setMode(e.target.value)}
          >
            <option value="both">Both</option>
            <option value="schema">Schema only</option>
            <option value="data">Data only</option>
          </select>
          <button
            className="rounded bg-slate-900 px-3 py-2 text-sm text-white disabled:opacity-50"
            onClick={run}
            disabled={!h1 || !h2 || loading}
          >
            {loading ? "Loading…" : "Diff"}
          </button>
        </div>
        {err && <div className="mt-3 text-sm text-red-700">{err}</div>}
      </Card>

      {data && data.warnings?.length > 0 && (
        <Card title="Warnings">
          <ul className="list-disc pl-5 text-sm text-amber-800">
            {data.warnings.map((w, i) => (
              <li key={i}>{w}</li>
            ))}
          </ul>
        </Card>
      )}

      {data && mode !== "data" && (
        <Card title="Schema SQL">
          <pre className="max-h-[360px] overflow-auto rounded bg-slate-950 p-3 text-xs text-slate-50">
            {(data.schema_sql ?? []).join("\n") || "(none)"}
          </pre>
        </Card>
      )}
      {data && mode !== "schema" && (
        <Card title="Data SQL">
          <pre className="max-h-[360px] overflow-auto rounded bg-slate-950 p-3 text-xs text-slate-50">
            {(data.data_sql ?? []).join("\n") || "(none)"}
          </pre>
        </Card>
      )}
    </div>
  );
}

function SchemaBrowserView({ commits }) {
  const [hash, setHash] = useState("");
  const [status, setStatus] = useState(null);
  const [tables, setTables] = useState(null);
  const [err, setErr] = useState("");

  useEffect(() => {
    (async () => {
      try {
        setErr("");
        setStatus(await getStatus());
      } catch (e) {
        setErr(String(e));
      }
    })();
  }, []);

  useEffect(() => {
    (async () => {
      if (!hash) {
        setTables(null);
        return;
      }
      try {
        setErr("");
        setTables(await getSnapshot(hash));
      } catch (e) {
        setErr(String(e));
      }
    })();
  }, [hash]);

  return (
    <div className="grid gap-4">
      <Card title="HEAD status">
        {err && <div className="text-sm text-red-700">{err}</div>}
        {status && (
          <pre className="overflow-auto rounded bg-slate-950 p-3 text-xs text-slate-50">
            {JSON.stringify(status, null, 2)}
          </pre>
        )}
        {!status && !err && (
          <div className="text-sm text-slate-600">Loading…</div>
        )}
      </Card>
      <Card title="Schema at commit">
        <div className="grid grid-cols-1 gap-3 md:grid-cols-2">
          <select
            className="rounded border border-slate-300 px-2 py-2"
            value={hash}
            onChange={(e) => setHash(e.target.value)}
          >
            <option value="">Select commit…</option>
            {commits.map((c) => (
              <option key={c.hash} value={c.hash}>
                {c.hash.slice(0, 12)} — {c.message}
              </option>
            ))}
          </select>
          <div className="text-sm text-slate-600">
            {tables ? `${tables.length} tables` : "—"}
          </div>
        </div>

        {tables && (
          <div className="mt-4 space-y-4">
            {tables
              .slice()
              .sort((a, b) => a.table_name.localeCompare(b.table_name))
              .map((t) => (
                <div key={t.table_name} className="rounded-lg border border-slate-200">
                  <div className="flex items-center justify-between border-b border-slate-200 bg-slate-50 px-3 py-2">
                    <div className="font-mono text-sm">{t.table_name}</div>
                    <div className="text-xs text-slate-600">{t.row_count} rows</div>
                  </div>
                  <div className="p-3">
                    <div className="overflow-auto">
                      <table className="min-w-full text-left text-xs">
                        <thead className="text-slate-600">
                          <tr>
                            <th className="py-1 pr-3">name</th>
                            <th className="py-1 pr-3">type</th>
                            <th className="py-1 pr-3">nullable</th>
                            <th className="py-1 pr-3">key</th>
                            <th className="py-1 pr-3">default</th>
                            <th className="py-1 pr-3">extra</th>
                          </tr>
                        </thead>
                        <tbody className="text-slate-800">
                          {(t.ddl?.columns ?? []).map((c) => (
                            <tr key={c.name} className="border-t border-slate-100">
                              <td className="py-1 pr-3 font-mono">{c.name}</td>
                              <td className="py-1 pr-3 font-mono">{c.type}</td>
                              <td className="py-1 pr-3">{String(c.nullable)}</td>
                              <td className="py-1 pr-3 font-mono">{c.key}</td>
                              <td className="py-1 pr-3 font-mono">
                                {c.default === null ? "null" : String(c.default)}
                              </td>
                              <td className="py-1 pr-3 font-mono">{c.extra}</td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                    <details className="mt-3">
                      <summary className="cursor-pointer text-xs text-slate-600">
                        raw DDL
                      </summary>
                      <pre className="mt-2 max-h-[240px] overflow-auto rounded bg-slate-950 p-3 text-xs text-slate-50">
                        {t.ddl?.raw_ddl ?? "(missing)"}
                      </pre>
                    </details>
                  </div>
                </div>
              ))}
          </div>
        )}
      </Card>
    </div>
  );
}

export default function App() {
  const [page, setPage] = useState("graph");
  const [commits, setCommits] = useState([]);
  const [err, setErr] = useState("");

  async function refresh() {
    setErr("");
    try {
      setCommits(await getCommits());
    } catch (e) {
      setErr(String(e));
    }
  }

  useEffect(() => {
    refresh();
  }, []);

  async function onCheckout(hash) {
    try {
      await postCheckout(hash);
      await refresh();
      alert("Checkout complete.");
    } catch (e) {
      alert(`Checkout failed: ${String(e)}`);
    }
  }

  return (
    <div className="flex min-h-screen bg-slate-50">
      <Sidebar page={page} setPage={setPage} />
      <div className="flex-1 p-6">
        <div className="mb-4 flex items-center justify-between">
          <div className="text-sm text-slate-600">
            API: <span className="font-mono">http://127.0.0.1:5000</span>
          </div>
          <button
            className="rounded border border-slate-300 bg-white px-3 py-2 text-sm hover:bg-slate-100"
            onClick={refresh}
          >
            Refresh
          </button>
        </div>
        {err && (
          <div className="mb-4 rounded border border-red-200 bg-red-50 p-3 text-sm text-red-800">
            {err}
          </div>
        )}
        {page === "graph" && (
          <CommitGraphView commits={commits} onCheckout={onCheckout} />
        )}
        {page === "diff" && <DiffViewerView commits={commits} />}
        {page === "schema" && <SchemaBrowserView commits={commits} />}
      </div>
    </div>
  );
}

