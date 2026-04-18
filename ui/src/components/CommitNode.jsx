import React from "react";
import { Handle, Position } from "@xyflow/react";

export default function CommitNode({ data }) {
  return (
    <div className={`commit-node ${data.isHead ? "is-head" : ""}`}>
      <Handle type="target" position={Position.Top} />
      <div className="commit-node-body">
        <div className="commit-node-row">
          <span className="hash-chip">{data.hash.slice(0, 12)}</span>
          {data.isHead && <span className="head-badge">HEAD</span>}
        </div>
        <div className="commit-message">{data.message}</div>
        <div className="commit-meta">
          {data.full_name} ({data.username})
        </div>
        <div className="commit-meta">{data.created_at}</div>
      </div>
      <div className="commit-node-actions">
        <button className="btn btn-primary" onClick={() => data.onCheckout(data.hash)}>
          Checkout
        </button>
      </div>
      <Handle type="source" position={Position.Bottom} />
    </div>
  );
}
