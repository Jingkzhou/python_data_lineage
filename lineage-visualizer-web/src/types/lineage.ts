export type LineageField = {
  name: string
  direction: 'in' | 'out' | 'both'
}

export type LineageNodeDTO = {
  id: string
  label: string
  fields: LineageField[]
}

export type LineageEdgeDTO = {
  id: string
  source: string
  target: string
  sourceField: string
  targetField: string
  relationType?: string
  effectType?: string
  sourceFile?: string
}

export type LineageGraphDTO = {
  nodes: LineageNodeDTO[]
  edges: LineageEdgeDTO[]
  fileCount: number
  generatedAt: string
}
