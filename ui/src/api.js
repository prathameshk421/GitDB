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
const API_BASE = import.meta.env.VITE_API_BASE ?? "http://127.0.0.1:5000";

export async function getCommits() {
  const r = await fetch(`${API_BASE}/commits`);
  if (!r.ok) throw new Error(await r.text());
  return await r.json();
}

export async function getDiff(hash1, hash2) {
  const r = await fetch(`${API_BASE}/diff/${hash1}/${hash2}`);
  if (!r.ok) throw new Error(await r.text());
  return await r.json();
}

export async function postCheckout(hash) {
  const r = await fetch(`${API_BASE}/checkout/${hash}`, { method: "POST" });
  if (!r.ok) throw new Error(await r.text());
  return await r.json();
}

export async function getStatus() {
  const r = await fetch(`${API_BASE}/status`);
  if (!r.ok) throw new Error(await r.text());
  return await r.json();
}

export async function getSnapshot(hash) {
  const r = await fetch(`${API_BASE}/snapshot/${hash}`);
  if (!r.ok) throw new Error(await r.text());
  return await r.json();
}

