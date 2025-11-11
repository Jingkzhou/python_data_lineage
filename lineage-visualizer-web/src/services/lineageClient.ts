import type { LineageGraphDTO } from '@/types/lineage'

const ENDPOINT = '/api/lineage'

export const fetchLineageGraph = async (): Promise<LineageGraphDTO> => {
  const response = await fetch(ENDPOINT, {
    headers: {
      Accept: 'application/json',
    },
  })

  if (!response.ok) {
    const message = await response.text()
    throw new Error(message || '无法获取血缘数据')
  }

  return (await response.json()) as LineageGraphDTO
}
