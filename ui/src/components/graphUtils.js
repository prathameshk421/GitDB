import dagre from "dagre";
import { Position } from "@xyflow/react";

const NODE_WIDTH = 280;
const NODE_HEIGHT = 110;

export function buildGraphElements(commits, onCheckout) {
  const parentHashes = new Set(
    commits
      .map((commit) => commit.parent_hash)
      .filter(Boolean)
  );

  const nodes = commits.map((commit) => ({
    id: commit.hash,
    type: "commitNode",
    position: { x: 0, y: 0 },
    sourcePosition: Position.Bottom,
    targetPosition: Position.Top,
    data: {
      ...commit,
      isHead: !parentHashes.has(commit.hash),
      onCheckout
    }
  }));

  const edges = commits
    .filter((commit) => commit.parent_hash)
    .map((commit) => ({
      id: `${commit.parent_hash}->${commit.hash}`,
      source: commit.parent_hash,
      target: commit.hash,
      type: "smoothstep"
    }));

  return { nodes, edges };
}

export function applyDagreLayout(nodes, edges) {
  const dagreGraph = new dagre.graphlib.Graph();
  dagreGraph.setDefaultEdgeLabel(() => ({}));
  dagreGraph.setGraph({ rankdir: "TB", ranksep: 65, nodesep: 50 });

  nodes.forEach((node) => {
    dagreGraph.setNode(node.id, { width: NODE_WIDTH, height: NODE_HEIGHT });
  });

  edges.forEach((edge) => {
    dagreGraph.setEdge(edge.source, edge.target);
  });

  dagre.layout(dagreGraph);

  const layoutedNodes = nodes.map((node) => {
    const position = dagreGraph.node(node.id);
    return {
      ...node,
      position: {
        x: position.x - NODE_WIDTH / 2,
        y: position.y - NODE_HEIGHT / 2
      }
    };
  });

  return { nodes: layoutedNodes, edges };
}
