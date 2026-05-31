import { render, screen } from '@testing-library/react'
import { vi, describe, it, expect, beforeEach } from 'vitest'
import { EventDetailPanel } from '@/components/EventDetailPanel'
import * as client from '@/api/client'

vi.mock('@/api/client')

const mockDetail = (overrides = {}) => ({
  global_event_id: '123456789',
  event_date: '2026-05-10',
  action_geo_fullname: 'Seoul, South Korea',
  event_code: '010',
  event_code_name: 'MAKE STATEMENT',
  avg_tone: -3.5,
  goldstein_scale: -2.0,
  num_mentions: 42,
  num_articles: 10,
  source_url: 'https://example.com/article',
  actor1_name: 'United States',
  actor1_country_code: 'USA',
  actor2_name: 'Russia',
  actor2_country_code: 'RUS',
  mention_source_name: 'Reuters',
  mention_doc_tone: -4.2,
  v2_persons: 'Biden;Putin',
  v2_organizations: 'NATO;UN',
  v2_enhanced_themes: 'MILITARY;DIPLOMACY;CONFLICT',
  llm_content_text: 'raw llm context that must not be rendered',
  ...overrides,
})

beforeEach(() => {
  vi.clearAllMocks()
})

describe('EventDetailPanel', () => {
  it('renders nothing when eventId is null', () => {
    render(<EventDetailPanel eventId={null} onClose={() => {}} />)
    expect(screen.queryByText('EVENT DETAIL')).not.toBeInTheDocument()
  })

  it('shows loading state while fetching', () => {
    vi.mocked(client.fetchEventDetail).mockReturnValue(new Promise(() => {}))
    render(<EventDetailPanel eventId="123" onClose={() => {}} />)
    expect(screen.getByText('LOADING...')).toBeInTheDocument()
  })

  it('shows error when fetch fails', async () => {
    vi.mocked(client.fetchEventDetail).mockRejectedValue(new Error('500: Internal Server Error'))
    render(<EventDetailPanel eventId="123" onClose={() => {}} />)
    await screen.findByText('ERROR: 500: Internal Server Error')
  })

  it('renders event type name as title', async () => {
    vi.mocked(client.fetchEventDetail).mockResolvedValue(mockDetail())
    render(<EventDetailPanel eventId="123" onClose={() => {}} />)
    await screen.findByText('MAKE STATEMENT')
  })

  it('renders actor1 and actor2 with country codes', async () => {
    vi.mocked(client.fetchEventDetail).mockResolvedValue(mockDetail())
    render(<EventDetailPanel eventId="123" onClose={() => {}} />)

    await screen.findByText('United States')
    expect(screen.getByText('[USA]')).toBeInTheDocument()
    expect(screen.getByText('Russia')).toBeInTheDocument()
    expect(screen.getByText('[RUS]')).toBeInTheDocument()
  })

  it('renders location', async () => {
    vi.mocked(client.fetchEventDetail).mockResolvedValue(mockDetail())
    render(<EventDetailPanel eventId="123" onClose={() => {}} />)
    await screen.findByText('Seoul, South Korea')
  })

  it('splits v2_persons by semicolon into individual badges', async () => {
    vi.mocked(client.fetchEventDetail).mockResolvedValue(mockDetail())
    render(<EventDetailPanel eventId="123" onClose={() => {}} />)

    await screen.findByText('Biden')
    expect(screen.getByText('Putin')).toBeInTheDocument()
  })

  it('splits v2_organizations by semicolon into individual badges', async () => {
    vi.mocked(client.fetchEventDetail).mockResolvedValue(mockDetail())
    render(<EventDetailPanel eventId="123" onClose={() => {}} />)

    await screen.findByText('NATO')
    expect(screen.getByText('UN')).toBeInTheDocument()
  })

  it('splits v2_enhanced_themes by semicolon into individual badges', async () => {
    vi.mocked(client.fetchEventDetail).mockResolvedValue(mockDetail())
    render(<EventDetailPanel eventId="123" onClose={() => {}} />)

    await screen.findByText('MILITARY')
    expect(screen.getByText('DIPLOMACY')).toBeInTheDocument()
    expect(screen.getByText('CONFLICT')).toBeInTheDocument()
  })

  it('does not render llm_content_text raw', async () => {
    vi.mocked(client.fetchEventDetail).mockResolvedValue(mockDetail())
    render(<EventDetailPanel eventId="123" onClose={() => {}} />)

    await screen.findByText('MAKE STATEMENT')
    expect(screen.queryByText('raw llm context that must not be rendered')).not.toBeInTheDocument()
  })

  it('does not render unfinished summary controls', async () => {
    vi.mocked(client.fetchEventDetail).mockResolvedValue(mockDetail())
    render(<EventDetailPanel eventId="123" onClose={() => {}} />)
    await screen.findByText('MAKE STATEMENT')
    expect(screen.queryByText(/요약/)).not.toBeInTheDocument()
  })

  it('shows 미상 when actor1_name is null', async () => {
    vi.mocked(client.fetchEventDetail).mockResolvedValue(mockDetail({ actor1_name: null }))
    render(<EventDetailPanel eventId="123" onClose={() => {}} />)
    await screen.findByText('미상')
  })

  it('renders source name and mention tone', async () => {
    vi.mocked(client.fetchEventDetail).mockResolvedValue(mockDetail())
    render(<EventDetailPanel eventId="123" onClose={() => {}} />)

    await screen.findByText('Reuters')
    expect(screen.getByText('tone -4.20')).toBeInTheDocument()
  })

  it('renders source link when source_url exists', async () => {
    vi.mocked(client.fetchEventDetail).mockResolvedValue(mockDetail())
    render(<EventDetailPanel eventId="123" onClose={() => {}} />)

    const link = await screen.findByRole('link')
    expect(link).toHaveAttribute('href', 'https://example.com/article')
  })

  it('hides entities section when all entity fields are null', async () => {
    vi.mocked(client.fetchEventDetail).mockResolvedValue(
      mockDetail({ v2_persons: null, v2_organizations: null, v2_enhanced_themes: null })
    )
    render(<EventDetailPanel eventId="123" onClose={() => {}} />)

    await screen.findByText('MAKE STATEMENT')
    expect(screen.queryByText('인물')).not.toBeInTheDocument()
    expect(screen.queryByText('기관')).not.toBeInTheDocument()
  })
})
