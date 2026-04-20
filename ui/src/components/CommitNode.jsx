import React, { useState } from "react";
import { Handle, Position } from "@xyflow/react";

export default function CommitNode({ data }) {
  const [copied, setCopied] = useState(false);
  const [isHovered, setIsHovered] = useState(false);

  const copyHash = async () => {
    await navigator.clipboard.writeText(data.hash);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  return (
    <div 
      className={`commit-node ${data.isHead ? "is-head" : ""} ${isHovered ? "is-hovered" : ""}`}
      onMouseEnter={() => setIsHovered(true)}
      onMouseLeave={() => setIsHovered(false)}
    >
      <Handle type="target" position={Position.Top} className="handle" />
      <div className="commit-node-body">
        <div className="commit-node-header">
          <code className="hash-chip" onClick={copyHash} title="Click to copy full hash">
            {data.hash.slice(0, 12)}
            {copied && <span className="copy-tooltip">Copied!</span>}
          </code>
          {data.isHead && <span className="head-badge">HEAD</span>}
        </div>
        <div className="commit-message" title={data.message}>
          {data.message.length > 40 ? data.message.slice(0, 40) + "…" : data.message}
        </div>
        <div className="commit-meta">
          <span className="author-avatar">{data.full_name?.[0]?.toUpperCase()}</span>
          <span>{data.full_name} ({data.username})</span>
        </div>
        <div className="commit-meta timestamp">{data.created_at}</div>
      </div>
      <div className="commit-node-actions">
        <button 
          className="btn btn-sm btn-secondary" 
          onClick={copyHash}
          title="Copy hash"
        >
          {copied ? "✓" : "⎘"}
        </button>
        <button 
          className="btn btn-sm btn-primary" 
          onClick={() => data.onCheckout(data.hash)}
          disabled={data.isHead}
        >
          {data.isHead ? "Current" : "Checkout"}
        </button>
      </div>
      <Handle type="source" position={Position.Bottom} className="handle" />
    </div>
  );
}