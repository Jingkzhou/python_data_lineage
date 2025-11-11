import dagre from 'dagre'

import type { LineageGraphDTO } from '@/types/lineage'

const NODE_WIDTH = 240
const BASE_HEIGHT = 80
const ROW_HEIGHT = 28

export const computeNodePositions = (graph: LineageGraphDTO) => {
  const g = new dagre.graphlib.Graph()
  g.setGraph({
    rankdir: 'LR',
    nodesep: 120,
    ranksep: 200,
    marginx: 60,
    marginy: 60,
  })
  g.setDefaultEdgeLabel(() => ({}))

  const heights = new Map<string, number>()

  graph.nodes.forEach((node) => {
    const height = BASE_HEIGHT + Math.max(node.fields.length, 1) * ROW_HEIGHT
    heights.set(node.id, height)
    g.setNode(node.id, { width: NODE_WIDTH, height })
  })

  graph.edges.forEach((edge) => {
    g.setEdge(edge.source, edge.target)
  })

  dagre.layout(g)

  const positions = new Map<string, { x: number; y: number }>()

  g.nodes().forEach((nodeId) => {
    const node = g.node(nodeId)
    if (!node) return
    const height = heights.get(nodeId) ?? BASE_HEIGHT
    positions.set(nodeId, {
      x: node.x - NODE_WIDTH / 2,
      y: node.y - height / 2,
    })
  })

  return positions
}
