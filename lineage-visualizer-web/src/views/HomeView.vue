<script setup lang="ts">
import { Background } from '@vue-flow/background'
import type { Edge, Node, NodeTypes } from '@vue-flow/core'
import { MarkerType, VueFlow } from '@vue-flow/core'
import { Controls } from '@vue-flow/controls'
import { MiniMap } from '@vue-flow/minimap'
import { computed, ref, watch } from 'vue'

import TableNode from '../components/nodes/TableNode.vue'

type TableNodeData = {
  tableName: string
  variant?: 'green' | 'magenta' | 'orange'
  fields: Array<{ name: string; direction?: 'in' | 'out' | 'both' }>
}

type InteractiveTableNodeData = TableNodeData & {
  active?: boolean
  dimmed?: boolean
  selectedField?: string
  highlightedFields?: Set<string>
  onTableSelect?: (nodeId: string) => void
  onFieldSelect?: (nodeId: string, field: string) => void
}

type LineageEdgeData = {
  sourceField: string
  targetField: string
}

type HighlightResult = {
  nodeIds: Set<string>
  fieldKeys: Set<string>
  edgeIds: Set<string>
}

const nodeTypes: NodeTypes = {
  tableNode: TableNode,
}

type SelectionState = {
  nodeId: string
  field?: string
} | null

const selectedContext = ref<SelectionState>(null)

const selectTable = (nodeId: string) => {
  selectedContext.value = { nodeId }
}

const selectField = (nodeId: string, field: string) => {
  selectedContext.value = { nodeId, field }
}

const clearSelection = () => {
  selectedContext.value = null
}

const enhanceNode = (node: Node<TableNodeData>): Node<InteractiveTableNodeData> => ({
  ...node,
  data: {
    ...node.data,
    onTableSelect: selectTable,
    onFieldSelect: selectField,
  },
})

const nodes = ref<Node<InteractiveTableNodeData>[]>([
  {
    id: 'customers',
    type: 'tableNode',
    position: { x: -520, y: -40 },
    data: {
      tableName: 'customers',
      variant: 'green',
      fields: [
        { name: 'credit_limit', direction: 'out' },
        { name: 'cust_email', direction: 'out' },
      ],
    },
  },
  {
    id: 'orders',
    type: 'tableNode',
    position: { x: -520, y: 200 },
    data: {
      tableName: 'orders',
      variant: 'green',
      fields: [
        { name: 'order_id', direction: 'out' },
        { name: 'customer_id', direction: 'out' },
        { name: 'order_total', direction: 'out' },
        { name: 'sales_rep_id', direction: 'out' },
      ],
    },
  },
  {
    id: 'insert_select',
    type: 'tableNode',
    position: { x: -120, y: 80 },
    data: {
      tableName: 'INSERT-SELECT-1',
      variant: 'magenta',
      fields: [
        { name: 'oid', direction: 'both' },
        { name: 'cid', direction: 'both' },
        { name: 'ottl', direction: 'both' },
        { name: 'sid', direction: 'both' },
        { name: 'cl', direction: 'both' },
        { name: 'cem', direction: 'both' },
      ],
    },
  },
  ...['small_orders', 'medium_orders', 'special_orders', 'large_orders'].map(
    (name, index) => ({
      id: name,
      type: 'tableNode',
      position: { x: 320, y: -80 + index * 110 },
      data: {
        tableName: name,
        variant: 'green',
        fields: [
          { name: 'oid', direction: 'in' },
          { name: 'ottl', direction: 'in' },
          { name: 'sid', direction: 'in' },
          { name: 'cid', direction: 'in' },
        ],
      },
    })
  ),
  {
    id: 'scott_dept',
    type: 'tableNode',
    position: { x: -520, y: 420 },
    data: {
      tableName: 'scott.dept',
      variant: 'green',
      fields: [{ name: 'deptno', direction: 'out' }],
    },
  },
  {
    id: 'scott_emp',
    type: 'tableNode',
    position: { x: -320, y: 520 },
    data: {
      tableName: 'scott.emp',
      variant: 'green',
      fields: [
        { name: 'sal', direction: 'out' },
        { name: 'deptno', direction: 'out' },
      ],
    },
  },
  {
    id: 'result_a',
    type: 'tableNode',
    position: { x: 100, y: 460 },
    data: {
      tableName: 'RESULT_OF_A-1',
      variant: 'orange',
      fields: [
        { name: 'deptno', direction: 'both' },
        { name: 'num_emp', direction: 'both' },
        { name: 'sal_sum', direction: 'both' },
      ],
    },
  },
  {
    id: 'result_b',
    type: 'tableNode',
    position: { x: 100, y: 640 },
    data: {
      tableName: 'RESULT_OF_B-1',
      variant: 'orange',
      fields: [{ name: 'total_count', direction: 'out' }],
    },
  },
  {
    id: 'rs_dashboard',
    type: 'tableNode',
    position: { x: 460, y: 560 },
    data: {
      tableName: 'RS-1',
      variant: 'magenta',
      fields: [
        { name: 'Department', direction: 'in' },
        { name: 'Employees', direction: 'in' },
        { name: 'Salary', direction: 'in' },
      ],
    },
  },
].map(enhanceNode))

const createFieldEdge = (
  source: string,
  sourceField: string,
  target: string,
  targetField: string
): Edge<LineageEdgeData> => ({
  id: `${source}-${sourceField}__${target}-${targetField}`,
  source,
  target,
  sourceHandle: `${source}-${sourceField}-out`,
  targetHandle: `${target}-${targetField}-in`,
  type: 'smoothstep',
  markerEnd: MarkerType.ArrowClosed,
  animated: true,
  data: { sourceField, targetField },
})

const baseEdges: Edge<LineageEdgeData>[] = [
  createFieldEdge('customers', 'credit_limit', 'insert_select', 'cl'),
  createFieldEdge('customers', 'cust_email', 'insert_select', 'cem'),
  createFieldEdge('orders', 'order_id', 'insert_select', 'oid'),
  createFieldEdge('orders', 'customer_id', 'insert_select', 'cid'),
  createFieldEdge('orders', 'order_total', 'insert_select', 'ottl'),
  createFieldEdge('orders', 'sales_rep_id', 'insert_select', 'sid'),
  ...['small_orders', 'medium_orders', 'special_orders', 'large_orders'].flatMap((target) =>
    ['oid', 'ottl', 'sid', 'cid'].map((field) => createFieldEdge('insert_select', field, target, field))
  ),
  createFieldEdge('insert_select', 'cl', 'special_orders', 'cid'),
  createFieldEdge('insert_select', 'cem', 'special_orders', 'cid'),
  createFieldEdge('scott_dept', 'deptno', 'result_a', 'deptno'),
  createFieldEdge('scott_emp', 'deptno', 'result_a', 'deptno'),
  createFieldEdge('scott_emp', 'sal', 'result_a', 'sal_sum'),
  createFieldEdge('result_a', 'deptno', 'rs_dashboard', 'Department'),
  createFieldEdge('result_a', 'num_emp', 'rs_dashboard', 'Employees'),
  createFieldEdge('result_a', 'sal_sum', 'rs_dashboard', 'Salary'),
  createFieldEdge('result_b', 'total_count', 'rs_dashboard', 'Employees'),
]

type Neighbor = { key: string; edgeId: string }
const downstreamMap = new Map<string, Neighbor[]>()
const upstreamMap = new Map<string, Neighbor[]>()

const fieldKey = (nodeId: string, field: string) => `${nodeId}:${field}`

const registerNeighbor = (map: Map<string, Neighbor[]>, fromKey: string, toKey: string, edgeId: string) => {
  const list = map.get(fromKey) ?? []
  list.push({ key: toKey, edgeId })
  map.set(fromKey, list)
}

baseEdges.forEach((edge) => {
  const sourceField = edge.data?.sourceField
  const targetField = edge.data?.targetField
  if (!sourceField || !targetField) return

  const sourceKey = fieldKey(edge.source, sourceField)
  const targetKey = fieldKey(edge.target, targetField)
  registerNeighbor(downstreamMap, sourceKey, targetKey, edge.id)
  registerNeighbor(upstreamMap, targetKey, sourceKey, edge.id)
})

const edges = ref<Edge<LineageEdgeData>[]>([])

const highlights = computed(() => [
  { label: '同步频率', value: '15 次/分钟' },
  { label: '表级节点', value: `${nodes.value.length}` },
  { label: '最新更新', value: '5 分钟前' },
])

const getFieldsForNode = (nodeId: string) => {
  const target = nodes.value.find((node) => node.id === nodeId)
  return target ? target.data.fields.map((field) => field.name) : []
}

const collectStartFieldKeys = (): string[] => {
  if (!selectedContext.value) return []
  const { nodeId, field } = selectedContext.value
  if (field) {
    return [fieldKey(nodeId, field)]
  }
  return getFieldsForNode(nodeId).map((name) => fieldKey(nodeId, name))
}

const traverseGraph = (startKeys: string[]): HighlightResult => {
  const fieldKeys = new Set<string>()
  const nodeIds = new Set<string>()
  const edgeIds = new Set<string>()

  if (!startKeys.length) {
    return { fieldKeys, nodeIds, edgeIds }
  }

  const queue = [...startKeys]
  startKeys.forEach((key) => {
    fieldKeys.add(key)
    nodeIds.add(key.split(':')[0])
  })

  const visitNeighbors = (key: string, map: Map<string, Neighbor[]>) => {
    const neighbors = map.get(key) ?? []
    neighbors.forEach(({ key: nextKey, edgeId }) => {
      edgeIds.add(edgeId)
      if (!fieldKeys.has(nextKey)) {
        fieldKeys.add(nextKey)
        nodeIds.add(nextKey.split(':')[0])
        queue.push(nextKey)
      }
    })
  }

  while (queue.length) {
    const key = queue.shift() as string
    visitNeighbors(key, downstreamMap)
    visitNeighbors(key, upstreamMap)
  }

  return { fieldKeys, nodeIds, edgeIds }
}

const computeHighlight = (): HighlightResult => {
  if (!selectedContext.value) {
    return { fieldKeys: new Set(), nodeIds: new Set(), edgeIds: new Set() }
  }
  const startKeys = collectStartFieldKeys()
  return traverseGraph(startKeys)
}

const applyNodeHighlight = (highlight: HighlightResult) => {
  const selectedField = selectedContext.value?.field
  const selectedNodeId = selectedContext.value?.nodeId
  nodes.value = nodes.value.map((node) => {
    const isActive = highlight.nodeIds.has(node.id)
    const highlightedFields = new Set<string>()
    highlight.fieldKeys.forEach((key) => {
      const [nodeId, fieldName] = key.split(':')
      if (nodeId === node.id && fieldName) {
        highlightedFields.add(fieldName)
      }
    })

    return {
      ...node,
      data: {
        ...node.data,
        active: isActive,
        dimmed: Boolean(selectedContext.value) && !isActive,
        highlightedFields,
        selectedField: node.id === selectedNodeId ? selectedField : undefined,
      },
    }
  })
}

const applyEdgeHighlight = (highlight: HighlightResult) => {
  edges.value = baseEdges.map((edge) => {
    const active = highlight.edgeIds.has(edge.id)
    const dimmed = Boolean(selectedContext.value) && !active
    return {
      ...edge,
      style: {
        stroke: active ? '#111827' : '#cdd5e2',
        strokeWidth: active ? 3 : 1.4,
        opacity: dimmed ? 0.35 : 1,
      },
      animated: active,
    }
  })
}

const updateHighlights = () => {
  const highlight = computeHighlight()
  applyNodeHighlight(highlight)
  applyEdgeHighlight(highlight)
}

watch(
  () => selectedContext.value,
  () => {
    updateHighlights()
  },
  { deep: true, immediate: true }
)
</script>

<template>
  <section class="home">
    <div class="home__intro">
      <div>
        <p class="home__eyebrow">数据血缘总览</p>
        <h1>从源到指标的端到端可视化</h1>
        <p class="home__desc">
          VueFlow 画布用来展示各层数据表之间的依赖关系，帮助数据团队快速定位影响范围、验证任务状态并发现潜在风险。
        </p>
      </div>

      <ul class="home__metrics">
        <li v-for="item in highlights" :key="item.label">
          <span class="home__metrics-label">{{ item.label }}</span>
          <strong class="home__metrics-value">{{ item.value }}</strong>
        </li>
      </ul>
    </div>

    <div class="home__canvas">
      <VueFlow
        v-model:nodes="nodes"
        v-model:edges="edges"
        class="home__flow"
        :node-types="nodeTypes"
        :fit-view-on-init="true"
        :min-zoom="0.2"
        :max-zoom="1.5"
        :default-viewport="{ x: -80, y: 0, zoom: 0.9 }"
        @pane-click="clearSelection"
      >
        <MiniMap pannable zoomable mask-color="#11111a22" />
        <Background color="#e5e7eb" gap="16" />
        <Controls position="bottom-right" />
      </VueFlow>
    </div>
  </section>
</template>

<style scoped>
.home {
  display: flex;
  flex-direction: column;
  gap: 1.5rem;
  height: 100%;
}

.home__intro {
  display: flex;
  flex-wrap: wrap;
  justify-content: space-between;
  gap: 1.5rem;
  background: linear-gradient(135deg, #f6f8fb, #edf2ff);
  border: 1px solid #e4e9f2;
  border-radius: 16px;
  padding: 1.5rem;
  box-shadow: 0 10px 30px rgba(15, 23, 42, 0.08);
}

.home__eyebrow {
  font-size: 0.85rem;
  text-transform: uppercase;
  color: #475569;
  letter-spacing: 0.15em;
  margin-bottom: 0.3rem;
}

.home__intro h1 {
  margin: 0;
  font-size: 1.8rem;
  color: #0f172a;
}

.home__desc {
  margin: 0.5rem 0 0;
  color: #475569;
  max-width: 38rem;
}

.home__metrics {
  list-style: none;
  display: flex;
  gap: 1.25rem;
  padding: 0;
  margin: 0;
}

.home__metrics li {
  min-width: 9rem;
  padding: 0.75rem 1rem;
  border-radius: 12px;
  background: #fff;
  border: 1px solid #e2e8f0;
  box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.4);
}

.home__metrics-label {
  display: block;
  font-size: 0.8rem;
  color: #64748b;
  margin-bottom: 0.25rem;
}

.home__metrics-value {
  font-size: 1.3rem;
  color: #111827;
}

.home__canvas {
  flex: 1;
  min-height: 520px;
  height: clamp(420px, 60vh, 720px);
  border-radius: 18px;
  border: 1px solid #e4e7ec;
  background: radial-gradient(circle at top, #f9fafb, #f1f5f9);
  overflow: hidden;
  box-shadow: 0 15px 45px rgba(15, 23, 42, 0.12);
  position: relative;
}

.home__flow {
  width: 100%;
  height: 100%;
  position: absolute;
  inset: 0;
}
</style>
