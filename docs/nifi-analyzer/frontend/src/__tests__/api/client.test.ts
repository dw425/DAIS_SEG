import { describe, it, expect, vi, beforeEach } from 'vitest';

// Mock fetch globally
const mockFetch = vi.fn();
(globalThis as Record<string, unknown>).fetch = mockFetch;

// Mock the auth store before importing client
vi.mock('../../store/auth', () => ({
  useAuthStore: {
    getState: () => ({ token: null }),
  },
}));

// Import after mocking
const clientModule = await import('../../api/client');

describe('API Client', () => {
  beforeEach(() => {
    mockFetch.mockReset();
  });

  it('parseFlow should POST with FormData', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: () => Promise.resolve({ platform: 'nifi', processors: [] }),
    });

    const file = new File(['{}'], 'test.json', { type: 'application/json' });
    const result = await clientModule.parseFlow(file);
    expect(result.platform).toBe('nifi');
    expect(mockFetch).toHaveBeenCalledTimes(1);
    expect(mockFetch.mock.calls[0][0]).toBe('/api/parse');
  });

  it('parseFlow should throw on failure', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: false,
      status: 400,
      statusText: 'Bad Request',
      text: () => Promise.resolve('Invalid file'),
    });

    const file = new File(['bad'], 'test.json');
    await expect(clientModule.parseFlow(file)).rejects.toThrow('Parse failed 400');
  });

  it('analyzeFlow should POST parsed data', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: () => Promise.resolve({ dependencyGraph: {} }),
    });

    const result = await clientModule.analyzeFlow({ processors: [] });
    expect(result).toBeDefined();
    expect(mockFetch).toHaveBeenCalledTimes(1);
  });

  it('assessFlow should POST parsed and analysis', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: () => Promise.resolve({ mappings: [] }),
    });

    const result = await clientModule.assessFlow({}, {});
    expect(result).toBeDefined();
  });

  it('generateNotebook should POST data', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: () => Promise.resolve({ cells: [] }),
    });

    const result = await clientModule.generateNotebook({});
    expect(result).toBeDefined();
  });

  it('validateNotebook should POST data', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: () => Promise.resolve({ overallScore: 85 }),
    });

    const result = await clientModule.validateNotebook({});
    expect(result).toBeDefined();
  });

  it('runFullPipeline should POST FormData', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: () => Promise.resolve({ success: true }),
    });

    const file = new File(['{}'], 'test.json');
    const result = await clientModule.runFullPipeline(file);
    expect(result.success).toBe(true);
  });

  it('request should throw on non-ok response', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: false,
      status: 500,
      statusText: 'Internal Server Error',
      text: () => Promise.resolve('Server error'),
    });

    await expect(clientModule.analyzeFlow({})).rejects.toThrow('API 500');
  });

  it('listRuns should GET with pagination', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: () => Promise.resolve({ runs: [], total: 0 }),
    });

    const result = await clientModule.listRuns(10, 0);
    expect(result.total).toBe(0);
    expect(mockFetch.mock.calls[0][0]).toContain('/api/history');
  });

  it('getDashboard should GET dashboard data', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: () => Promise.resolve({ totalRuns: 5 }),
    });

    const result = await clientModule.getDashboard();
    expect(result).toBeDefined();
  });

  it('compareFlows should POST flow data', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: () => Promise.resolve({ flows: [], comparisonMatrix: {} }),
    });

    const result = await clientModule.compareFlows([{}, {}], ['a', 'b']);
    expect(result).toBeDefined();
  });

  it('exportPDF should return HTML string', async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      text: () => Promise.resolve('<html>report</html>'),
    });

    const result = await clientModule.exportPDF({ title: 'Test' });
    expect(result).toContain('html');
  });
});
