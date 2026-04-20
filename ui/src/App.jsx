import React, { useEffect, useState } from "react";
import { flushSync } from "react-dom";
import { ReactFlow, Background, Controls } from "@xyflow/react";
import { DiffView, DiffModeEnum } from "@git-diff-view/react";
import { generateDiffFile } from "@git-diff-view/file";
import { getCommits, getDiff, getSnapshot, getStatus, postCheckout, login, logout, getSession, getRepositories } from "./api.js";
import CommitNode from "./components/CommitNode.jsx";
import { buildGraphElements, applyDagreLayout } from "./components/graphUtils.js";

const nodeTypes = { commitNode: CommitNode };
const VIEW_TRANSITION_MS = 600;

function snapshotToDDLText(snapshot) {
  return snapshot
    .slice()
    .sort((a, b) => a.table_name.localeCompare(b.table_name))
    .map((table) => table.ddl?.raw_ddl ?? "")
    .filter(Boolean)
    .join("\n\n");
}

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

function ThemeIcon({ theme }) {
  if (theme === "dark") {
    return (
      <svg viewBox="0 0 24 24" width="20" height="20" aria-hidden="true">
        <circle cx="12" cy="12" r="4.2" fill="currentColor" />
        <path
          d="M12 1.8V4.3M12 19.7V22.2M4.8 4.8L6.6 6.6M17.4 17.4L19.2 19.2M1.8 12H4.3M19.7 12H22.2M4.8 19.2L6.6 17.4M17.4 6.6L19.2 4.8"
          stroke="currentColor"
          strokeWidth="1.8"
          strokeLinecap="round"
        />
      </svg>
    );
  }

  return (
    <svg viewBox="0 0 24 24" width="20" height="20" aria-hidden="true">
      <path
        d="M20.4 14.8A8.6 8.6 0 1 1 9.2 3.6a7.1 7.1 0 1 0 11.2 11.2z"
        fill="currentColor"
      />
    </svg>
  );
}

function CommitGraphView({ commits, onCheckout }) {
  const [nodes, setNodes] = useState([]);
  const [edges, setEdges] = useState([]);

  useEffect(() => {
    const { nodes: rawNodes, edges: rawEdges } = buildGraphElements(commits, onCheckout);
    const { nodes: layoutedNodes, edges: layoutedEdges } = applyDagreLayout(rawNodes, rawEdges);
    setNodes(layoutedNodes);
    setEdges(layoutedEdges);
  }, [commits, onCheckout]);

  return (
    <div className="stack">
      <Card title="Commit Graph">
        <div className="react-flow-wrapper">
          <ReactFlow
            nodes={nodes}
            edges={edges}
            nodeTypes={nodeTypes}
            fitView
          >
            <Background gap={18} size={1} />
            <Controls />
          </ReactFlow>
        </div>
        {commits.length === 0 && <div className="empty-state">No commits yet.</div>}
      </Card>
    </div>
  );
}

function DiffViewerView({ commits }) {
  const [h1, setH1] = useState("");
  const [h2, setH2] = useState("");
  const [mode, setMode] = useState("both");
  const [diffFile, setDiffFile] = useState(null);
  const [sqlDiff, setSqlDiff] = useState(null);
  const [err, setErr] = useState("");
  const [loading, setLoading] = useState(false);

  async function run() {
    setErr("");
    setLoading(true);
    try {
      const [snapshot1, snapshot2, sqlRes] = await Promise.all([
        getSnapshot(h1),
        getSnapshot(h2),
        getDiff(h1, h2)
      ]);

      const oldText = snapshotToDDLText(snapshot1);
      const newText = snapshotToDDLText(snapshot2);

      const file = generateDiffFile(
        `schema@${h1.slice(0, 12)}`,
        oldText,
        `schema@${h2.slice(0, 12)}`,
        newText,
        "sql",
        "sql"
      );
      file.init();
      file.buildSplitDiffLines();

      setDiffFile(file);
      setSqlDiff(sqlRes);
    } catch (e) {
      setErr(String(e));
      setDiffFile(null);
      setSqlDiff(null);
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

      {sqlDiff && sqlDiff.warnings?.length > 0 && (
        <div className="warning-banner">
          <ul className="warning-list">
            {sqlDiff.warnings.map((w, i) => (
              <li key={i}>{w}</li>
            ))}
          </ul>
        </div>
      )}

      {diffFile && (
        <Card title="Visual Diff">
          <DiffView
            diffFile={diffFile}
            diffViewMode={DiffModeEnum.Split}
          />
        </Card>
      )}

      {sqlDiff && mode !== "data" && (
        <details className="diff-details">
          <summary>Schema SQL</summary>
          <pre className="sql-block">
            {(sqlDiff.schema_sql ?? []).join("\n") || "(none)"}
          </pre>
        </details>
      )}

      {sqlDiff && mode !== "schema" && (
        <details className="diff-details">
          <summary>Data SQL</summary>
          <pre className="sql-block">
            {(sqlDiff.data_sql ?? []).join("\n") || "(none)"}
          </pre>
        </details>
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
  const [theme, setTheme] = useState(() => {
    const saved = window.localStorage.getItem("gitdb-theme");
    return saved === "dark" ? "dark" : "light";
  });
  const [user, setUser] = useState(null);
  const [repos, setRepos] = useState([]);
  const [repoId, setRepoId] = useState(null);
  const [loginState, setLoginState] = useState({ username: "", password: "", error: "" });
  const API_BASE = import.meta.env.VITE_API_BASE ?? "http://127.0.0.1:5000";

  // Session check on mount
  useEffect(() => {
    (async () => {
      try {
        const session = await getSession();
        if (session.user) {
          setUser(session.user);
          const repoList = await getRepositories();
          setRepos(repoList);
          if (repoList.length > 0) setRepoId(repoList[0].repo_id);
        }
      } catch {}
    })();
  }, []);

  // Repo change triggers commit refresh
  useEffect(() => {
    if (!repoId) return;
    refresh();
    // eslint-disable-next-line
  }, [repoId]);

  useEffect(() => {
    document.documentElement.setAttribute("data-theme", theme);
    window.localStorage.setItem("gitdb-theme", theme);
  }, [theme]);

  async function refresh() {
    setErr("");
    try {
      setCommits(await getCommits());
    } catch (e) {
      setErr(String(e));
    }
  }

  async function handleLogin(e) {
    e.preventDefault();
    setLoginState((s) => ({ ...s, error: "" }));
    try {
      const res = await login(loginState.username, loginState.password);
      setUser(res.user);
      const repoList = await getRepositories();
      setRepos(repoList);
      setRepoId(repoList.length > 0 ? repoList[0].repo_id : null);
    } catch (e) {
      setLoginState((s) => ({ ...s, error: e.message.replace(/^Error: /, "") }));
    }
  }

  async function handleLogout() {
    await logout();
    setUser(null);
    setRepos([]);
    setRepoId(null);
    setCommits([]);
  }

  async function onCheckout(hash) {
    try {
      await postCheckout(hash);
      await refresh();
      alert("Checkout complete.");
    } catch (e) {
      alert(`Checkout failed: ${String(e)}`);
    }
  }

  function toggleTheme(event) {
    const nextTheme = theme === "light" ? "dark" : "light";
    const x = event.clientX;
    const y = event.clientY;
    const endRadius = Math.hypot(
      Math.max(x, window.innerWidth - x),
      Math.max(y, window.innerHeight - y)
    );
    const applyTheme = () => {
      flushSync(() => {
        setTheme(nextTheme);
      });
    };

    if (!document.startViewTransition) {
      applyTheme();
      return;
    }

    const transition = document.startViewTransition(() => {
      applyTheme();
    });

    transition.ready
      .then(() => {
        document.documentElement.animate(
          {
            clipPath: [
              `circle(0px at ${x}px ${y}px)`,
              `circle(${endRadius}px at ${x}px ${y}px)`
            ]
          },
          {
            duration: VIEW_TRANSITION_MS,
            easing: "cubic-bezier(0.4, 0, 0.2, 1)",
            pseudoElement: "::view-transition-new(root)"
          }
        );
      })
      .catch(() => {
        applyTheme();
      });
  }

  // Login form if not logged in
  if (!user) {
    return (
      <div className="login-shell">
        <form className="login-form" onSubmit={handleLogin}>
          <h2>Login to GitDB</h2>
          <input
            className="field"
            type="text"
            placeholder="Username"
            value={loginState.username}
            onChange={e => setLoginState(s => ({ ...s, username: e.target.value }))}
            autoFocus
            required
          />
          <input
            className="field"
            type="password"
            placeholder="Password"
            value={loginState.password}
            onChange={e => setLoginState(s => ({ ...s, password: e.target.value }))}
            required
          />
          <button className="btn btn-primary" type="submit">Login</button>
          {loginState.error && <div className="error-inline">{loginState.error}</div>}
        </form>
      </div>
    );
  }

  // Repo selector if multiple repos
  const repoSelector = (
    <div className="repo-selector">
      <label htmlFor="repo-select">Repository:</label>
      <select
        id="repo-select"
        className="field"
        value={repoId || ""}
        onChange={e => setRepoId(Number(e.target.value))}
      >
        {repos.map(r => (
          <option key={r.repo_id} value={r.repo_id}>
            {r.repo_name} ({r.db_name}@{r.db_host}:{r.db_port})
          </option>
        ))}
      </select>
    </div>
  );

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
            {repos.length > 1 && repoSelector}
          </div>
          <div className="topbar-actions">
            <span className="user-info">{user.username}</span>
            <button className="btn btn-secondary" onClick={handleLogout}>
              Logout
            </button>
            <button
              className="theme-toggle"
              onClick={toggleTheme}
              aria-label={`Switch to ${theme === "light" ? "dark" : "light"} mode`}
              title={`Switch to ${theme === "light" ? "dark" : "light"} mode`}
            >
              <ThemeIcon theme={theme} />
            </button>
            <button className="btn btn-secondary" onClick={refresh}>
              Refresh
            </button>
          </div>
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

