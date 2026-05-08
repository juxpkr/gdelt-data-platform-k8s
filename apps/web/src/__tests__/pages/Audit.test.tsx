import { render, screen, fireEvent, waitFor } from '@testing-library/react'
import { vi, describe, it, expect, beforeEach } from 'vitest'
import { Audit } from '@/pages/Audit'
import * as client from '@/api/client'

vi.mock('@/api/client')
vi.mock('@/pages/Dashboard', () => ({
  PageHeader: ({ title, sub }: { title: string; sub: string }) => (
    <div>
      <h1>{title}</h1>
      <p>{sub}</p>
    </div>
  ),
}))

const mockRun = (overrides = {}) => ({
  batch_id: '20260508100000',
  stage: 'bronze',
  status: 'success',
  input_rows: 8647,
  output_rows: 8647,
  duration_seconds: 45.6,
  error_message: null,
  created_at: '2026-05-08 10:00:50.000000',
  ...overrides,
})

beforeEach(() => {
  vi.clearAllMocks()
})

describe('Audit page', () => {
  it('renders run rows with batch_id', async () => {
    vi.mocked(client.fetchAuditRuns).mockResolvedValue({
      runs: [mockRun()],
    })

    render(<Audit />)
    await screen.findByText('20260508100000')
  })

  it('shows failed status badge', async () => {
    vi.mocked(client.fetchAuditRuns).mockResolvedValue({
      runs: [mockRun({ status: 'failed', error_message: 'Spark error' })],
    })

    render(<Audit />)
    await screen.findByText('failed')
  })

  it('shows NO DATA when runs are empty', async () => {
    vi.mocked(client.fetchAuditRuns).mockResolvedValue({ runs: [] })

    render(<Audit />)
    await screen.findByText('NO DATA')
  })

  it('calls fetchAuditRuns again on refresh button click', async () => {
    vi.mocked(client.fetchAuditRuns).mockResolvedValue({ runs: [mockRun()] })

    render(<Audit />)
    await screen.findByText('20260508100000')

    fireEvent.click(screen.getByRole('button', { name: 'refresh' }))
    await waitFor(() => {
      expect(client.fetchAuditRuns).toHaveBeenCalledTimes(2)
    })
  })
})
