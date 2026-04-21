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
    { id: "graph", label: "Graph", icon: "◎", shortcut: "1" },
    { id: "diff", label: "Diff", icon: "⇵", shortcut: "2" },
    { id: "schema", label: "Schema", icon: "≋", shortcut: "3" }
  ];
  return (
    <aside className="app-sidebar">
      <div className="brand-wrap">
        <div className="brand-mark" aria-hidden="true">
          G
        </div>
        <div>
          <div className="brand-name">GitDB</div>
          <div className="brand-subtitle">Version Control</div>
        </div>
      </div>
      <nav className="nav-list">
        {items.map((it) => (
          <button
            key={it.id}
            className={`nav-button ${
              page === it.id ? "nav-button-active" : "nav-button-idle"
            }`}
            onClick={() => setPage(it.id)}
            title={`${it.label} (press ${it.shortcut})`}
          >
            <span className="nav-icon">{it.icon}</span>
            <span>{it.label}</span>
            <kbd className="nav-shortcut">{it.shortcut}</kbd>
          </button>
        ))}
      </nav>
      <div className="sidebar-footnote">
        <kbd>⌘R</kbd> Refresh · <kbd>T</kbd> Toggle theme
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

function DiffViewerView({ commits, repoId }) {
  const [h1, setH1] = useState("");
  const [h2, setH2] = useState("");
  const [mode, setMode] = useState("both");
  const [diffFile, setDiffFile] = useState(null);
  const [sqlDiff, setSqlDiff] = useState(null);
  const [err, setErr] = useState("");
  const [loading, setLoading] = useState(false);
  const [showSql, setShowSql] = useState({ schema: true, data: true });

  useEffect(() => {
    setH1("");
    setH2("");
    setDiffFile(null);
    setSqlDiff(null);
    setErr("");
  }, [repoId]);

  async function run() {
    setErr("");
    setLoading(true);
    try {
      const [snapshot1, snapshot2, sqlRes] = await Promise.all([
        getSnapshot(h1, repoId),
        getSnapshot(h2, repoId),
        getDiff(h1, h2, repoId)
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

  const swapCommits = () => {
    const temp = h1;
    setH1(h2);
    setH2(temp);
  };

  return (
    <div className="stack">
      <Card title="Select commits">
        <div className="diff-select-grid">
          <div className="diff-select-box">
            <label className="diff-select-label">From</label>
            <select
              className="field"
              value={h1}
              onChange={(e) => setH1(e.target.value)}
            >
              <option value="">Select commit…</option>
              {commits.map((c) => (
                <option key={c.hash} value={c.hash}>
                  {c.hash.slice(0, 12)} — {c.message}
                </option>
              ))}
            </select>
          </div>
          <button className="btn btn-ghost swap-btn" onClick={swapCommits} title="Swap commits">
            ⇄
          </button>
          <div className="diff-select-box">
            <label className="diff-select-label">To</label>
            <select
              className="field"
              value={h2}
              onChange={(e) => setH2(e.target.value)}
            >
              <option value="">Select commit…</option>
              {commits.map((c) => (
                <option key={c.hash} value={c.hash}>
                  {c.hash.slice(0, 12)} — {c.message}
                </option>
              ))}
            </select>
          </div>
          <select
            className="field mode-select"
            value={mode}
            onChange={(e) => setMode(e.target.value)}
          >
            <option value="both">All</option>
            <option value="schema">Schema</option>
            <option value="data">Data</option>
          </select>
          <button className="btn btn-primary" onClick={run} disabled={!h1 || !h2 || loading}>
            {loading ? <span className="loading-spinner"></span> : "Compare"}
          </button>
        </div>
        {err && <div className="error-inline">{err}</div>}
      </Card>

      {sqlDiff && sqlDiff.warnings?.length > 0 && (
        <div className="warning-banner">
          {sqlDiff.warnings.map((w, i) => (
            <span key={i}>{w}</span>
          ))}
        </div>
      )}

      {diffFile && mode !== "data" && (
        <Card title="Schema Diff">
          <DiffView
            diffFile={diffFile}
            diffViewMode={DiffModeEnum.Split}
          />
        </Card>
      )}

      <div className="sql-toggle-row">
        {sqlDiff && mode !== "data" && (
          <details className="diff-details" open={showSql.schema}>
            <summary onClick={() => setShowSql(s => ({ ...s, schema: !s.schema }))}>
              Schema SQL ({(sqlDiff.schema_sql ?? []).length} statements)
            </summary>
            <pre className="sql-block">
              {(sqlDiff.schema_sql ?? []).join("\n") || "(none)"}
            </pre>
          </details>
        )}

        {sqlDiff && mode !== "schema" && (
          <details className="diff-details" open={showSql.data}>
            <summary onClick={() => setShowSql(s => ({ ...s, data: !s.data }))}>
              Data SQL ({(sqlDiff.data_sql ?? []).length} statements)
            </summary>
            <pre className="sql-block">
              {(sqlDiff.data_sql ?? []).join("\n") || "(none)"}
            </pre>
          </details>
        )}
      </div>
    </div>
  );
}

function SchemaBrowserView({ commits, repoId }) {
  const [hash, setHash] = useState("");
  const [status, setStatus] = useState(null);
  const [tables, setTables] = useState(null);
  const [err, setErr] = useState("");
  const [search, setSearch] = useState("");
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    setHash("");
    setTables(null);
    setSearch("");
    (async () => {
      if (!repoId) return;
      try {
        setErr("");
        setStatus(await getStatus(repoId));
      } catch (e) {
        setErr(String(e));
      }
    })();
  }, [repoId]);

  useEffect(() => {
    (async () => {
      if (!hash) {
        setTables(null);
        return;
      }
      setLoading(true);
      try {
        setErr("");
        setTables(await getSnapshot(hash, repoId));
      } catch (e) {
        setErr(String(e));
      } finally {
        setLoading(false);
      }
    })();
  }, [hash]);

  const filteredTables = tables?.filter(t => 
    t.table_name.toLowerCase().includes(search.toLowerCase()) ||
    t.ddl?.columns?.some(c => c.name.toLowerCase().includes(search.toLowerCase()))
  ).sort((a, b) => a.table_name.localeCompare(b.table_name));

  return (
    <div className="stack">
      <Card title="Working Directory Status">
        {err && <div className="error-inline">{err}</div>}
        {status ? (
          <div className="status-grid">
            {status.modified_tables?.length > 0 && (
              <div className="status-item modified">
                <span className="status-label">Modified</span>
                <span className="status-value">{status.modified_tables.join(", ")}</span>
              </div>
            )}
            {status.added_tables?.length > 0 && (
              <div className="status-item added">
                <span className="status-label">Added</span>
                <span className="status-value">{status.added_tables.join(", ")}</span>
              </div>
            )}
            {status.dropped_tables?.length > 0 && (
              <div className="status-item dropped">
                <span className="status-label">Dropped</span>
                <span className="status-value">{status.dropped_tables.join(", ")}</span>
              </div>
            )}
            {(!status.modified_tables?.length && !status.added_tables?.length && !status.dropped_tables?.length) && (
              <div className="status-clean">
                ✓ Working directory clean
              </div>
            )}
          </div>
        ) : (
          <div className="empty-state">Loading…</div>
        )}
      </Card>
      <Card title="Schema Browser">
        <div className="schema-controls">
          <select
            className="field"
            value={hash}
            onChange={(e) => setHash(e.target.value)}
          >
            <option value="">Select commit to view schema…</option>
            {commits.map((c) => (
              <option key={c.hash} value={c.hash}>
                {c.hash.slice(0, 12)} — {c.message}
              </option>
            ))}
          </select>
          <input
            className="field search-field"
            type="text"
            placeholder="Search tables or columns..."
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            disabled={!tables}
          />
          <div className="schema-count">
            {tables ? `${filteredTables?.length ?? 0} / ${tables.length} tables` : "—"}
          </div>
        </div>

        {loading && (
          <div className="schema-loading">
            <div className="loading-spinner"></div>
            <span>Loading schema...</span>
          </div>
        )}

        {filteredTables && !loading && (
          <div className="table-grid">
            {filteredTables.map((t) => (
              <div key={t.table_name} className="table-card">
                <div className="table-card-head">
                  <div className="table-info">
                    <div className="table-name">{t.table_name}</div>
                    <div className="table-count">{t.row_count} rows</div>
                  </div>
                  <div className="table-actions">
                    <span className="column-count">{t.ddl?.columns?.length ?? 0} cols</span>
                  </div>
                </div>
                <div className="table-card-body">
                  <div className="column-list">
                    {t.ddl?.columns?.map((c) => (
                      <div key={c.name} className="column-item">
                        <code className="column-name">{c.name}</code>
                        <span className="column-type">{c.type}</span>
                        {c.key && <span className="column-key">{c.key}</span>}
                      </div>
                    ))}
                  </div>
                  <details className="ddl-wrap">
                    <summary className="ddl-summary">View DDL</summary>
                    <pre className="sql-block compact">
                      {t.ddl?.raw_ddl ?? "(missing)"}
                    </pre>
                  </details>
                </div>
              </div>
            ))}
            {filteredTables.length === 0 && (
              <div className="empty-state">No tables match your search</div>
            )}
          </div>
        )}
        
        {!tables && !loading && (
          <div className="empty-state">Select a commit to view its schema</div>
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

  useEffect(() => {
    if (!repoId) return;
    refresh();
  }, [repoId]);

  useEffect(() => {
    document.documentElement.setAttribute("data-theme", theme);
    window.localStorage.setItem("gitdb-theme", theme);
  }, [theme]);

  // Keyboard shortcuts
  useEffect(() => {
    const handleKeyDown = (e) => {
      if (e.target.tagName === "INPUT" || e.target.tagName === "TEXTAREA") return;
      
      if (e.key === "1" && !e.ctrlKey && !e.metaKey) {
        e.preventDefault();
        setPage("graph");
      } else if (e.key === "2" && !e.ctrlKey && !e.metaKey) {
        e.preventDefault();
        setPage("diff");
      } else if (e.key === "3" && !e.ctrlKey && !e.metaKey) {
        e.preventDefault();
        setPage("schema");
      } else if ((e.key === "r" || e.key === "R") && (e.ctrlKey || e.metaKey)) {
        e.preventDefault();
        refresh();
      } else if (e.key === "t" && !e.ctrlKey && !e.metaKey) {
        e.preventDefault();
        setTheme(t => t === "light" ? "dark" : "light");
      }
    };
    
    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, []);

  async function refresh() {
    setErr("");
    try {
      setCommits(await getCommits(repoId));
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
      await postCheckout(hash, repoId);
      await refresh();
      alert("Checkout complete.");
    } catch (e) {
      alert(`Checkout failed: ${String(e)}`);
    }
  }

  const toggleTheme = (event) => {
    const nextTheme = theme === "light" ? "dark" : "light";
    if (!event) {
      setTheme(nextTheme);
      return;
    }
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
  };

  // Login form if not logged in
  if (!user) {
    return (
      <div className="login-shell">
        <div className="login-card">
          <div className="login-header">
            <div className="brand-mark">G</div>
            <h1>GitDB</h1>
            <p>Database Version Control</p>
          </div>
          <form className="login-form" onSubmit={handleLogin}>
            <div className="form-group">
              <label htmlFor="username">Username</label>
              <input
                id="username"
                className="field"
                type="text"
                placeholder="Enter username"
                value={loginState.username}
                onChange={e => setLoginState(s => ({ ...s, username: e.target.value }))}
                autoFocus
                required
              />
            </div>
            <div className="form-group">
              <label htmlFor="password">Password</label>
              <input
                id="password"
                className="field"
                type="password"
                placeholder="Enter password"
                value={loginState.password}
                onChange={e => setLoginState(s => ({ ...s, password: e.target.value }))}
                required
              />
            </div>
            {loginState.error && <div className="error-inline">{loginState.error}</div>}
            <button className="btn btn-primary btn-lg" type="submit">Sign In</button>
          </form>
          <div className="login-footer">
            Press <kbd>1</kbd> <kbd>2</kbd> <kbd>3</kbd> to switch views
          </div>
        </div>
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
            {repos.length >= 1 && repoSelector}
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
        {page === "diff" && <DiffViewerView commits={commits} repoId={repoId} />}
        {page === "schema" && <SchemaBrowserView commits={commits} repoId={repoId} />}
      </main>
    </div>
  );
}

