import React, { useEffect, useState } from "react";
import { getCommits, getDiff, getSnapshot, getStatus, postCheckout } from "./api.js";

function Sidebar({ page, setPage }) {
  const items = [
    { id: "graph", label: "Commit Graph" },
    { id: "diff", label: "Diff Viewer" },
    { id: "schema", label: "Schema Browser" }
  ];
  return (
    <aside className="app-sidebar">
      <div className="brand-wrap">
        <div className="brand-mark" aria-hidden="true">
          G
        </div>
        <div>
          <div className="brand-name">GitDB</div>
          <div className="brand-subtitle">Versioned SQL snapshots</div>
        </div>
      </div>
      <div className="nav-list">
        {items.map((it) => (
          <button
            key={it.id}
            className={`nav-button ${
              page === it.id ? "nav-button-active" : "nav-button-idle"
            }`}
            onClick={() => setPage(it.id)}
          >
            {it.label}
          </button>
        ))}
      </div>
      <div className="sidebar-footnote">
        Keep API integration stable while iterating on schema history.
      </div>
    </aside>
  );
}

function Card({ title, children }) {
  return (
    <section className="panel">
      <div className="panel-title">{title}</div>
      <div className="panel-body">{children}</div>
    </section>
  );
}

function CommitGraphView({ commits, onCheckout }) {
  return (
    <div className="stack">
      <Card title="Commits">
        <div className="commit-list">
          {commits.map((c) => (
            <div
              key={c.hash}
              className="commit-item"
            >
              <div>
                <div className="hash-chip">{c.hash.slice(0, 12)}</div>
                <div className="commit-message">{c.message}</div>
                <div className="commit-meta">
                  {c.full_name} ({c.username}) · {c.created_at}
                </div>
              </div>
              <button className="btn btn-primary" onClick={() => onCheckout(c.hash)}>
                Checkout
              </button>
            </div>
          ))}
          {commits.length === 0 && (
            <div className="empty-state">No commits yet.</div>
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
    <div className="stack">
      <Card title="Select commits">
        <div className="form-grid">
          <select
            className="field"
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
            className="field"
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
            className="field"
            value={mode}
            onChange={(e) => setMode(e.target.value)}
          >
            <option value="both">Both</option>
            <option value="schema">Schema only</option>
            <option value="data">Data only</option>
          </select>
          <button className="btn btn-primary" onClick={run} disabled={!h1 || !h2 || loading}>
            {loading ? "Loading…" : "Diff"}
          </button>
        </div>
        {err && <div className="error-inline">{err}</div>}
      </Card>

      {data && data.warnings?.length > 0 && (
        <Card title="Warnings">
          <ul className="warning-list">
            {data.warnings.map((w, i) => (
              <li key={i}>{w}</li>
            ))}
          </ul>
        </Card>
      )}

      {data && mode !== "data" && (
        <Card title="Schema SQL">
          <pre className="sql-block">
            {(data.schema_sql ?? []).join("\n") || "(none)"}
          </pre>
        </Card>
      )}
      {data && mode !== "schema" && (
        <Card title="Data SQL">
          <pre className="sql-block">
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
    <div className="stack">
      <Card title="HEAD status">
        {err && <div className="error-inline">{err}</div>}
        {status && (
          <pre className="json-block">
            {JSON.stringify(status, null, 2)}
          </pre>
        )}
        {!status && !err && (
          <div className="empty-state">Loading…</div>
        )}
      </Card>
      <Card title="Schema at commit">
        <div className="schema-head">
          <select
            className="field"
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
          <div className="schema-summary">
            {tables ? `${tables.length} tables` : "—"}
          </div>
        </div>

        {tables && (
          <div className="table-stack">
            {tables
              .slice()
              .sort((a, b) => a.table_name.localeCompare(b.table_name))
              .map((t) => (
                <div key={t.table_name} className="table-card">
                  <div className="table-card-head">
                    <div className="table-name">{t.table_name}</div>
                    <div className="table-count">{t.row_count} rows</div>
                  </div>
                  <div className="table-card-body">
                    <div className="table-wrap">
                      <table className="schema-table">
                        <thead>
                          <tr>
                            <th>name</th>
                            <th>type</th>
                            <th>nullable</th>
                            <th>key</th>
                            <th>default</th>
                            <th>extra</th>
                          </tr>
                        </thead>
                        <tbody>
                          {(t.ddl?.columns ?? []).map((c) => (
                            <tr key={c.name}>
                              <td className="mono">{c.name}</td>
                              <td className="mono">{c.type}</td>
                              <td>{String(c.nullable)}</td>
                              <td className="mono">{c.key}</td>
                              <td className="mono">
                                {c.default === null ? "null" : String(c.default)}
                              </td>
                              <td className="mono">{c.extra}</td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                    <details className="ddl-wrap">
                      <summary className="ddl-summary">
                        raw DDL
                      </summary>
                      <pre className="sql-block compact">
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
  const API_BASE = import.meta.env.VITE_API_BASE ?? "http://127.0.0.1:5000";

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
    <div className="app-shell">
      <Sidebar page={page} setPage={setPage} />
      <main className="app-main">
        <div className="topbar">
          <div>
            <div className="topbar-title">Data Version Browser</div>
            <div className="topbar-subtitle">
              API endpoint: <span className="mono">{API_BASE}</span>
            </div>
          </div>
          <button className="btn btn-secondary" onClick={refresh}>
            Refresh
          </button>
        </div>
        {err && (
          <div className="error-banner">
            {err}
          </div>
        )}
        {page === "graph" && (
          <CommitGraphView commits={commits} onCheckout={onCheckout} />
        )}
        {page === "diff" && <DiffViewerView commits={commits} />}
        {page === "schema" && <SchemaBrowserView commits={commits} />}
      </main>
    </div>
  );
}

