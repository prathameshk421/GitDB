from __future__ import annotations

import hashlib

from .snapshot import TableSnapshot, snapshot_to_json


def compute_commit_hash(snapshot: dict[str, TableSnapshot], parent_hash: str | None) -> str:
    json_bytes = snapshot_to_json(snapshot).encode("utf-8")
    payload = json_bytes + (parent_hash or "").encode("utf-8")
    return hashlib.sha256(payload).hexdigest()

