const API_BASE = import.meta.env.VITE_API_BASE ?? `http://${window.location.hostname}:5001`;

// Auth/session helpers
export async function login(username, password) {
  const r = await fetch(`${API_BASE}/login`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    credentials: "include",
    body: JSON.stringify({ username, password })
  });
  if (!r.ok) throw new Error(await r.text());
  return await r.json();
}

export async function logout() {
  const r = await fetch(`${API_BASE}/logout`, {
    method: "POST",
    credentials: "include"
  });
  if (!r.ok) throw new Error(await r.text());
  return await r.json();
}

export async function getSession() {
  const r = await fetch(`${API_BASE}/me`, {
    credentials: "include"
  });
  if (!r.ok) return { user: null };
  return await r.json();
}

export async function getRepositories() {
  const r = await fetch(`${API_BASE}/repositories`, {
    credentials: "include"
  });
  if (!r.ok) throw new Error(await r.text());
  return await r.json();
}
export async function getCommits(repoId) {
  const params = repoId ? `?repo_id=${repoId}` : "";
  const r = await fetch(`${API_BASE}/commits${params}`);
  if (!r.ok) throw new Error(await r.text());
  return await r.json();
}

export async function getDiff(hash1, hash2, repoId) {
  const params = repoId ? `?repo_id=${repoId}` : "";
  const r = await fetch(`${API_BASE}/diff/${hash1}/${hash2}${params}`);
  if (!r.ok) throw new Error(await r.text());
  return await r.json();
}

export async function postCheckout(hash, repoId) {
  const params = repoId ? `?repo_id=${repoId}` : "";
  const r = await fetch(`${API_BASE}/checkout/${hash}${params}`, { method: "POST" });
  if (!r.ok) throw new Error(await r.text());
  return await r.json();
}

export async function getStatus(repoId) {
  const params = repoId ? `?repo_id=${repoId}` : "";
  const r = await fetch(`${API_BASE}/status${params}`);
  if (!r.ok) throw new Error(await r.text());
  return await r.json();
}

export async function getSnapshot(hash, repoId) {
  const params = repoId ? `?repo_id=${repoId}` : "";
  const r = await fetch(`${API_BASE}/snapshot/${hash}${params}`);
  if (!r.ok) throw new Error(await r.text());
  return await r.json();
}

