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

