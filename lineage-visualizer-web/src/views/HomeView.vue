<script setup lang="ts">
import { Background } from '@vue-flow/background'
import type { Edge, Node, NodeTypes } from '@vue-flow/core'
import { MarkerType, VueFlow, useVueFlow } from '@vue-flow/core'
import { Controls } from '@vue-flow/controls'
import { MiniMap } from '@vue-flow/minimap'
import { computed, nextTick, onMounted, ref, watch } from 'vue'

import TableNode from '../components/nodes/TableNode.vue'
import { fetchLineageGraph } from '@/services/lineageClient'
import { computeNodePositions } from '@/utils/lineageLayout'
import type { LineageEdgeDTO, LineageField, LineageGraphDTO } from '@/types/lineage'

type TableNodeData = {
  tableName: string
  variant?: 'green' | 'magenta' | 'orange'
  fields: LineageField[]
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

type SelectionState = {
  nodeId: string
  field?: string
} | null

type Neighbor = { key: string; edgeId: string }

const nodeTypes: NodeTypes = {
  tableNode: TableNode,
}

const variantPalette: TableNodeData['variant'][] = ['green', 'magenta', 'orange']
const pickVariant = (index: number) => variantPalette[index % variantPalette.length]

const nodes = ref<Node<InteractiveTableNodeData>[]>([])
const edges = ref<Edge<LineageEdgeData>[]>([])

const selectedContext = ref<SelectionState>(null)
const isLoading = ref(true)
const loadError = ref<string | null>(null)

const datasetStats = ref({
  fileCount: 0,
  nodeCount: 0,
  edgeCount: 0,
  updatedAt: '',
})

let baseEdges: Edge<LineageEdgeData>[] = []
const downstreamMap = new Map<string, Neighbor[]>()
const upstreamMap = new Map<string, Neighbor[]>()

const { fitView } = useVueFlow()

const selectTable = (nodeId: string) => {
  selectedContext.value = { nodeId }
}

const selectField = (nodeId: string, field: string) => {
  selectedContext.value = { nodeId, field }
}

const clearSelection = () => {
  selectedContext.value = null
}

const attachInteractions = (node: Node<TableNodeData>): Node<InteractiveTableNodeData> => ({
  ...node,
  data: {
    ...node.data,
    onTableSelect: selectTable,
    onFieldSelect: selectField,
  },
})

const createEdgeFromDTO = (edge: LineageEdgeDTO): Edge<LineageEdgeData> => ({
  id: edge.id,
  source: edge.source,
  target: edge.target,
  sourceHandle: `${edge.source}-${edge.sourceField}-out`,
  targetHandle: `${edge.target}-${edge.targetField}-in`,
  type: 'smoothstep',
  markerEnd: MarkerType.ArrowClosed,
  data: {
    sourceField: edge.sourceField,
    targetField: edge.targetField,
  },
})

const registerNeighbor = (map: Map<string, Neighbor[]>, fromKey: string, toKey: string, edgeId: string) => {
  const list = map.get(fromKey) ?? []
  list.push({ key: toKey, edgeId })
  map.set(fromKey, list)
}

const rebuildAdjacency = () => {
  downstreamMap.clear()
  upstreamMap.clear()
  baseEdges.forEach((edge) => {
    const sourceField = edge.data?.sourceField
    const targetField = edge.data?.targetField
    if (!sourceField || !targetField) return

    const sourceKey = fieldKey(edge.source, sourceField)
    const targetKey = fieldKey(edge.target, targetField)
    registerNeighbor(downstreamMap, sourceKey, targetKey, edge.id)
    registerNeighbor(upstreamMap, targetKey, sourceKey, edge.id)
  })
}

const fieldKey = (nodeId: string, field: string) => `${nodeId}:${field}`

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

const traverseDirectional = (startKeys: string[], map: Map<string, Neighbor[]>): HighlightResult => {
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
    visitNeighbors(key, map)
  }

  return { fieldKeys, nodeIds, edgeIds }
}

const computeHighlight = (): HighlightResult => {
  if (!selectedContext.value) {
    return { fieldKeys: new Set(), nodeIds: new Set(), edgeIds: new Set() }
  }
  const startKeys = collectStartFieldKeys()
  const downstream = traverseDirectional(startKeys, downstreamMap)
  const upstream = traverseDirectional(startKeys, upstreamMap)

  const mergeSets = <T>(...sets: Set<T>[]) => {
    const result = new Set<T>()
    sets.forEach((set) => set.forEach((value) => result.add(value)))
    return result
  }

  return {
    fieldKeys: mergeSets(downstream.fieldKeys, upstream.fieldKeys),
    nodeIds: mergeSets(downstream.nodeIds, upstream.nodeIds),
    edgeIds: mergeSets(downstream.edgeIds, upstream.edgeIds),
  }
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

const highlights = computed(() => [
  { label: 'CSV 文件', value: datasetStats.value.fileCount.toString() },
  { label: '表级节点', value: datasetStats.value.nodeCount.toString() },
  { label: '字段连线', value: datasetStats.value.edgeCount.toString() },
])

const hydrateNodes = (graph: LineageGraphDTO): Node<InteractiveTableNodeData>[] => {
  const positions = computeNodePositions(graph)
  return graph.nodes.map((node, index) =>
    attachInteractions({
      id: node.id,
      type: 'tableNode',
      position: positions.get(node.id) ?? { x: index * 260, y: index * 80 },
      data: {
        tableName: node.label,
        variant: pickVariant(index),
        fields: node.fields,
      },
    })
  )
}

const loadLineage = async () => {
  isLoading.value = true
  loadError.value = null
  try {
    const graph = await fetchLineageGraph()
    datasetStats.value = {
      fileCount: graph.fileCount,
      nodeCount: graph.nodes.length,
      edgeCount: graph.edges.length,
      updatedAt: graph.generatedAt,
    }
    nodes.value = hydrateNodes(graph)
    baseEdges = graph.edges.map((edge) => createEdgeFromDTO(edge))
    rebuildAdjacency()
    selectedContext.value = null
    updateHighlights()
    await nextTick()
    try {
      fitView({ duration: 500, padding: 0.25 })
    } catch {
      // VueFlow 还未准备好时忽略
    }
  } catch (error) {
    loadError.value = (error as Error).message || '读取血缘数据失败'
    nodes.value = []
    baseEdges = []
    edges.value = []
  } finally {
    isLoading.value = false
  }
}

const handleRetry = () => {
  loadLineage()
}

onMounted(() => {
  loadLineage()
})
</script>

<template>
  <section class="home">
    <div class="home__intro">
      <div>
        <p class="home__eyebrow">数据血缘总览</p>
        <h1>从 result CSV 自动构建血缘图</h1>
        <p class="home__desc">
          VueFlow 画布基于 result 目录下的 CSV 文件实时生成血缘关系，新增或删除文件后刷新即可查看最新依赖。
        </p>
        <p v-if="datasetStats.updatedAt" class="home__desc home__desc--muted">
          最近生成时间：{{ new Date(datasetStats.updatedAt).toLocaleString() }}
        </p>
      </div>

      <ul class="home__metrics">
        <li v-for="item in highlights" :key="item.label">
          <span class="home__metrics-label">{{ item.label }}</span>
          <strong class="home__metrics-value">{{ item.value }}</strong>
        </li>
      </ul>
    </div>

    <div v-if="isLoading" class="home__status">
      正在读取 CSV 文件并计算血缘...
    </div>
    <div v-else-if="loadError" class="home__status home__status--error">
      <p>{{ loadError }}</p>
      <button type="button" class="home__status-btn" @click="handleRetry">重新加载</button>
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

.home__desc--muted {
  color: #94a3b8;
  font-size: 0.9rem;
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

.home__status {
  padding: 0.9rem 1.1rem;
  border-radius: 12px;
  border: 1px solid #d6dae6;
  background: #fff;
  color: #1e1b4b;
  box-shadow: 0 8px 20px rgba(15, 23, 42, 0.08);
}

.home__status--error {
  border-color: #fecaca;
  color: #b91c1c;
  background: #fff1f2;
}

.home__status-btn {
  margin-top: 0.6rem;
  padding: 0.4rem 0.9rem;
  border-radius: 999px;
  border: none;
  background: #4f46e5;
  color: #fff;
  font-size: 0.85rem;
  cursor: pointer;
}

.home__status-btn:hover {
  background: #4338ca;
}
</style>
