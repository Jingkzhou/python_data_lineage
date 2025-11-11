<script setup lang="ts">
import type { CSSProperties } from 'vue'
import { computed } from 'vue'
import type { NodeProps } from '@vue-flow/core'
import { Handle, Position } from '@vue-flow/core'

import type { LineageField } from '@/types/lineage'

type TableNodeData = {
  tableName: string
  variant?: 'green' | 'magenta' | 'orange'
  fields: LineageField[]
}

const props = defineProps<
  NodeProps<
    TableNodeData & {
      active?: boolean
      dimmed?: boolean
      selectedField?: string
      highlightedFields?: Set<string>
      onTableSelect?: (nodeId: string) => void
      onFieldSelect?: (nodeId: string, field: string) => void
    }
  >
>()

const colors = computed(() => {
  const palette: Record<
    NonNullable<TableNodeData['variant']>,
    { header: string; border: string }
  > = {
    green: { header: '#7eb252', border: '#6a9a45' },
    magenta: { header: '#b04a7e', border: '#9a3e6c' },
    orange: { header: '#c7623e', border: '#a85234' },
  }
  return palette[props.data?.variant ?? 'green']
})

const boxStyle = computed<CSSProperties>(() => ({
  borderColor: colors.value.border,
}))

const headerStyle = computed<CSSProperties>(() => ({
  background: colors.value.header,
}))

const makeHandleId = (field: LineageField, dir: 'in' | 'out') => `${props.id}-${field.name}-${dir}`

const allowsIn = (field: LineageField) => (field.direction ?? 'both') !== 'out'
const allowsOut = (field: LineageField) => (field.direction ?? 'both') !== 'in'
</script>

<template>
  <div
    class="table-node"
    :class="{
      'table-node--active': props.data?.active,
      'table-node--dimmed': props.data?.dimmed,
    }"
    :style="boxStyle"
  >
    <header
      class="table-node__header"
      :style="headerStyle"
      @click.stop="props.data?.onTableSelect?.(props.id)"
    >
      {{ props.data?.tableName }}
    </header>
    <ul class="table-node__fields">
      <li
        v-for="field in props.data?.fields"
        :key="field.name"
        :class="{
          'table-node__field': true,
          'table-node__field--highlighted': props.data?.highlightedFields?.has(field.name),
          'table-node__field--selected': props.data?.selectedField === field.name,
        }"
        @click.stop="props.data?.onFieldSelect?.(props.id, field.name)"
      >
        <Handle
          v-if="allowsIn(field)"
          class="table-node__handle table-node__handle--left"
          type="target"
          :id="makeHandleId(field, 'in')"
          :position="Position.Left"
        />
        <span>{{ field.name }}</span>
        <Handle
          v-if="allowsOut(field)"
          class="table-node__handle table-node__handle--right"
          type="source"
          :id="makeHandleId(field, 'out')"
          :position="Position.Right"
        />
      </li>
    </ul>
  </div>
</template>

<style scoped>
.table-node {
  min-width: 210px;
  background: #fff;
  border: 2px solid;
  border-radius: 6px;
  box-shadow: 0 10px 30px rgba(15, 23, 42, 0.08);
  transition: transform 0.2s ease, box-shadow 0.2s ease, opacity 0.2s ease;
}

.table-node--active {
  box-shadow: 0 15px 35px rgba(15, 23, 42, 0.22);
  transform: translateY(-2px);
}

.table-node--dimmed {
  opacity: 0.45;
}

.table-node__header {
  margin: 0;
  color: #fff;
  font-weight: 600;
  padding: 0.65rem 1rem;
  border-radius: 4px 4px 0 0;
  text-transform: lowercase;
  cursor: pointer;
}

.table-node__fields {
  list-style: none;
  margin: 0;
  padding: 0.3rem 0;
}

.table-node__field {
  position: relative;
  padding: 0.35rem 1rem;
  border-top: 1px solid #ecf0f5;
  font-size: 0.9rem;
  color: #0f172a;
  cursor: pointer;
  transition: background 0.15s ease;
}

.table-node__field--highlighted {
  background: rgba(79, 70, 229, 0.16);
}

.table-node__field--selected {
  background: rgba(79, 70, 229, 0.28);
  font-weight: 600;
}

.table-node__handle {
  position: absolute;
  top: 50%;
  transform: translateY(-50%);
  width: 10px;
  height: 10px;
  border-radius: 999px;
  background: #cbd5f5;
  border: 2px solid #fff;
}

.table-node__handle--left {
  left: -4px;
}

.table-node__handle--right {
  right: -4px;
}
</style>
