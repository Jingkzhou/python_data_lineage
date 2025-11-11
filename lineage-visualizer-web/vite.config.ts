import { fileURLToPath, URL } from 'node:url'
import path from 'node:path'
import { readFile, readdir } from 'node:fs/promises'
import type { IncomingMessage, ServerResponse } from 'node:http'

import { defineConfig } from 'vite'
import type { Plugin } from 'vite'
import vue from '@vitejs/plugin-vue'
import vueJsx from '@vitejs/plugin-vue-jsx'
import vueDevTools from 'vite-plugin-vue-devtools'
import { parse } from 'csv-parse/sync'

import type { LineageGraphDTO, LineageEdgeDTO, LineageField, LineageNodeDTO } from './src/types/lineage'

const RESULT_DIR = fileURLToPath(new URL('../result', import.meta.url))
const API_PATH = '/api/lineage'

type TableAccumulator = {
  id: string
  label: string
  fields: Map<string, LineageField>
}

const sanitize = (value?: string | number) => (value ?? '').toString().trim()

const extractEndpoint = (record: Record<string, string>, prefix: 'SOURCE' | 'TARGET') => {
  const tableRaw = sanitize(record[`${prefix}_TABLE`])
  const db = sanitize(record[`${prefix}_DB`])
  const schema = sanitize(record[`${prefix}_SCHEMA`])
  const tableIdField = sanitize(record[`${prefix}_TABLE_ID`])
  const columnRaw = sanitize(record[`${prefix}_COLUMN`])
  const columnIdField = sanitize(record[`${prefix}_COLUMN_ID`])

  const tableId = tableRaw || [db, schema, tableIdField].filter(Boolean).join('.') || tableIdField
  const columnName = columnRaw || columnIdField

  if (!tableId || !columnName) {
    return null
  }

  return {
    tableId,
    tableLabel: tableRaw || tableId,
    columnName,
  }
}

const ensureTable = (tables: Map<string, TableAccumulator>, info: { tableId: string; tableLabel: string }) => {
  if (!tables.has(info.tableId)) {
    tables.set(info.tableId, {
      id: info.tableId,
      label: info.tableLabel,
      fields: new Map(),
    })
  }
  return tables.get(info.tableId)!
}

const registerField = (
  tables: Map<string, TableAccumulator>,
  info: ReturnType<typeof extractEndpoint>,
  role: 'in' | 'out'
) => {
  if (!info) return
  const table = ensureTable(tables, info)
  const existing = table.fields.get(info.columnName)
  if (!existing) {
    table.fields.set(info.columnName, { name: info.columnName, direction: role })
    return
  }
  if (existing.direction === role || existing.direction === 'both') {
    return
  }
  table.fields.set(info.columnName, { ...existing, direction: 'both' })
}

const buildLineagePayload = async (): Promise<LineageGraphDTO> => {
  const tables = new Map<string, TableAccumulator>()
  const edges: LineageEdgeDTO[] = []

  let files: string[] = []
  try {
    files = await readdir(RESULT_DIR)
  } catch {
    files = []
  }

  const csvFiles = files.filter((filename) => filename.toLowerCase().endsWith('.csv'))
  let edgeCount = 0

  for (const file of csvFiles) {
    const filePath = path.join(RESULT_DIR, file)
    let content: string
    try {
      content = await readFile(filePath, 'utf-8')
    } catch {
      continue
    }

    let records: Record<string, string>[] = []
    try {
      records = parse(content, {
        columns: true,
        skip_empty_lines: true,
      }) as Record<string, string>[]
    } catch {
      continue
    }

    for (const record of records) {
      const source = extractEndpoint(record, 'SOURCE')
      const target = extractEndpoint(record, 'TARGET')
      if (!source || !target) continue

      registerField(tables, source, 'out')
      registerField(tables, target, 'in')

      edges.push({
        id: `edge-${edgeCount++}`,
        source: source.tableId,
        target: target.tableId,
        sourceField: source.columnName,
        targetField: target.columnName,
        relationType: sanitize(record.RELATION_TYPE),
        effectType: sanitize(record.EFFECTTYPE),
        sourceFile: file,
      })
    }
  }

  const nodes: LineageNodeDTO[] = Array.from(tables.values()).map((table) => ({
    id: table.id,
    label: table.label,
    fields: Array.from(table.fields.values())
      .map((field) => ({
        ...field,
        direction: field.direction ?? 'both',
      }))
      .sort((a, b) => a.name.localeCompare(b.name)),
  }))

  return {
    nodes,
    edges,
    fileCount: csvFiles.length,
    generatedAt: new Date().toISOString(),
  }
}

const lineageDataPlugin = (): Plugin => {
  const handler = async (_req: IncomingMessage, res: ServerResponse) => {
    try {
      const payload = await buildLineagePayload()
      res.statusCode = 200
      res.setHeader('Content-Type', 'application/json')
      res.setHeader('Cache-Control', 'no-store')
      res.end(JSON.stringify(payload))
    } catch (error) {
      res.statusCode = 500
      res.end(JSON.stringify({ message: 'Failed to build lineage graph', details: (error as Error).message }))
    }
  }

  const attach = (server: { middlewares: { use: (...args: any[]) => void } }) => {
    server.middlewares.use(async (req: IncomingMessage, res: ServerResponse, next: () => void) => {
      if (req.url?.startsWith(API_PATH)) {
        if (req.method && req.method !== 'GET') {
          res.statusCode = 405
          res.end('Method Not Allowed')
          return
        }
        await handler(req, res)
        return
      }
      next()
    })
  }

  return {
    name: 'lineage-data-plugin',
    configureServer(server) {
      attach(server)
    },
    configurePreviewServer(server) {
      attach(server)
    },
  }
}

// https://vite.dev/config/
export default defineConfig({
  plugins: [
    lineageDataPlugin(),
    vue(),
    vueJsx(),
    vueDevTools(),
  ],
  resolve: {
    alias: {
      '@': fileURLToPath(new URL('./src', import.meta.url))
    },
  },
  server: {
    fs: {
      allow: [
        fileURLToPath(new URL('.', import.meta.url)),
        RESULT_DIR,
      ],
    },
  },
})
