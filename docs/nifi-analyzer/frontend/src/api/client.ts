// ── FastAPI backend client ──

import type { FullROIReport } from '../types/roi';
import { useAuthStore } from '../store/auth';

const BASE = '/api';

/** Default request timeout for non-pipeline requests (5 minutes). */
const DEFAULT_TIMEOUT_MS = 300_000;

/** Pipeline step requests have no timeout — the watchdog handles liveness. */
const NO_TIMEOUT = 0;

function getAuthHeaders(): Record<string, string> {
  const token = useAuthStore.getState().token;
  return token ? { Authorization: `Bearer ${token}` } : {};
}

/**
 * Create an AbortSignal that fires after `ms` milliseconds.
 * If the caller already provides a signal, it is combined via AbortSignal.any.
 */
function withTimeout(ms: number, existingSignal?: AbortSignal | null): AbortSignal {
  const timeoutSignal = AbortSignal.timeout(ms);
  if (existingSignal) {
    return AbortSignal.any([timeoutSignal, existingSignal]);
  }
  return timeoutSignal;
}

async function request<T>(path: string, options: RequestInit = {}, timeoutMs: number = DEFAULT_TIMEOUT_MS): Promise<T> {
  const authHeaders = getAuthHeaders();
  const fetchOptions: RequestInit = {
    headers: { 'Content-Type': 'application/json', ...authHeaders, ...options.headers as Record<string, string> },
    ...options,
  };
  if (timeoutMs > 0) {
    fetchOptions.signal = withTimeout(timeoutMs, options.signal);
  } else if (options.signal) {
    fetchOptions.signal = options.signal;
  }
  let res: Response;
  try {
    res = await fetch(`${BASE}${path}`, fetchOptions);
  } catch (err) {
    if (err instanceof DOMException && err.name === 'TimeoutError') {
      throw new Error(`Request to ${path} timed out after ${timeoutMs / 1000}s`);
    }
    if (err instanceof DOMException && err.name === 'AbortError') {
      throw new Error(`Request to ${path} was cancelled`);
    }
    throw err;
  }
  if (!res.ok) {
    const body = await res.text().catch(() => '');
    throw new Error(`API ${res.status}: ${body || res.statusText}`);
  }
  return res.json();
}

/** Pipeline step requests — no client-side timeout; the watchdog monitors liveness. */
function pipelineRequest<T>(path: string, options: RequestInit = {}, abortSignal?: AbortSignal): Promise<T> {
  return request<T>(path, { ...options, signal: abortSignal }, NO_TIMEOUT);
}

// ── Heartbeat ──

export interface HeartbeatResponse {
  active: boolean;
  step: string | null;
  elapsedSeconds: number;
  lastProgressSeconds: number;
  processorCount: number;
}

export async function heartbeat(): Promise<HeartbeatResponse> {
  return request<HeartbeatResponse>('/health/heartbeat', {}, 5_000);
}

export async function parseFlow(file: File) {
  const form = new FormData();
  form.append('file', file);
  const signal = withTimeout(DEFAULT_TIMEOUT_MS);
  let res: Response;
  try {
    res = await fetch(`${BASE}/parse`, { method: 'POST', body: form, headers: getAuthHeaders(), signal });
  } catch (err) {
    if (err instanceof DOMException && err.name === 'TimeoutError') {
      throw new Error(`Parse request timed out after ${DEFAULT_TIMEOUT_MS / 1000}s`);
    }
    throw err;
  }
  if (!res.ok) {
    const body = await res.text().catch(() => '');
    throw new Error(`Parse failed ${res.status}: ${body || res.statusText}`);
  }
  return res.json();
}

export async function analyzeFlow(parsed: unknown) {
  return pipelineRequest('/analyze', {
    method: 'POST',
    body: JSON.stringify({ parsed }),
  });
}

export async function assessFlow(parsed: unknown, analysis: unknown) {
  return pipelineRequest('/assess', {
    method: 'POST',
    body: JSON.stringify({ parsed, analysis }),
  });
}

export async function generateNotebook(data: unknown) {
  return pipelineRequest('/generate', {
    method: 'POST',
    body: JSON.stringify(data),
  });
}

export async function validateNotebook(data: unknown) {
  return pipelineRequest('/validate', {
    method: 'POST',
    body: JSON.stringify(data),
  });
}

export async function generateReport(data: unknown) {
  return pipelineRequest('/report', {
    method: 'POST',
    body: JSON.stringify(data),
  });
}

export async function generateROIReport(data: unknown): Promise<FullROIReport> {
  return request<FullROIReport>('/report', {
    method: 'POST',
    body: JSON.stringify({ ...data as Record<string, unknown>, type: 'roi' }),
  });
}

/** Pipeline timeout: 5 minutes to allow for large flows. */
const PIPELINE_TIMEOUT_MS = 300_000;

export async function runFullPipeline(file: File) {
  const form = new FormData();
  form.append('file', file);
  const signal = withTimeout(PIPELINE_TIMEOUT_MS);
  let res: Response;
  try {
    res = await fetch(`${BASE}/pipeline/run`, { method: 'POST', body: form, headers: getAuthHeaders(), signal });
  } catch (err) {
    if (err instanceof DOMException && err.name === 'TimeoutError') {
      throw new Error(`Pipeline request timed out after ${PIPELINE_TIMEOUT_MS / 1000}s`);
    }
    throw err;
  }
  if (!res.ok) {
    const body = await res.text().catch(() => '');
    throw new Error(`Pipeline failed ${res.status}: ${body || res.statusText}`);
  }
  return res.json();
}

export async function getPlatforms() {
  return request<{ platforms: unknown[] }>('/platforms');
}

export async function getAdminLogs() {
  return request<{ logs: string[] }>('/admin/logs');
}

// ── Versioning ──

export async function saveVersion(projectId: string, data: unknown) {
  return request(`/projects/${projectId}/versions`, {
    method: 'POST',
    body: JSON.stringify(data),
  });
}

export async function listVersions(projectId: string) {
  return request<{ versions: unknown[] }>(`/projects/${projectId}/versions`);
}

export async function diffVersions(projectId: string, v1: string, v2: string) {
  return request(`/projects/${projectId}/versions/diff?v1=${v1}&v2=${v2}`);
}

// ── Comparison ──

export async function compareFlows(flows: unknown[], labels: string[]) {
  return request('/compare', {
    method: 'POST',
    body: JSON.stringify({ flows, labels }),
  });
}

// ── Comments ──

export async function createComment(data: unknown) {
  return request('/comments', {
    method: 'POST',
    body: JSON.stringify(data),
  });
}

export async function listComments(targetType?: string, targetId?: string) {
  const params = new URLSearchParams();
  if (targetType) params.set('target_type', targetType);
  if (targetId) params.set('target_id', targetId);
  return request<{ comments: unknown[] }>(`/comments?${params}`);
}

export async function deleteComment(id: string) {
  return request(`/comments/${id}`, { method: 'DELETE' });
}

// ── Run History ──

export async function listRuns(limit = 50, offset = 0) {
  return request<{ runs: unknown[]; total: number }>(`/history?limit=${limit}&offset=${offset}`);
}

export async function getRunDetail(id: string) {
  return request(`/history/${id}`);
}

export async function recordRun(data: unknown) {
  return request('/history', {
    method: 'POST',
    body: JSON.stringify(data),
  });
}

// ── Dashboard ──

export async function getDashboard() {
  return request('/dashboard');
}

// ── DAB Export ──

export async function exportDAB(data: unknown): Promise<Blob> {
  const signal = withTimeout(DEFAULT_TIMEOUT_MS);
  let res: Response;
  try {
    res = await fetch(`${BASE}/export/dab`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', ...getAuthHeaders() },
      body: JSON.stringify(data),
      signal,
    });
  } catch (err) {
    if (err instanceof DOMException && err.name === 'TimeoutError') {
      throw new Error(`DAB export timed out after ${DEFAULT_TIMEOUT_MS / 1000}s`);
    }
    throw err;
  }
  if (!res.ok) {
    const body = await res.text().catch(() => '');
    throw new Error(`DAB export failed ${res.status}: ${body || res.statusText}`);
  }
  return res.blob();
}

// ── PDF Export ──

export async function exportPDF(data: unknown): Promise<string> {
  const signal = withTimeout(DEFAULT_TIMEOUT_MS);
  let res: Response;
  try {
    res = await fetch(`${BASE}/export/pdf`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', ...getAuthHeaders() },
      body: JSON.stringify(data),
      signal,
    });
  } catch (err) {
    if (err instanceof DOMException && err.name === 'TimeoutError') {
      throw new Error(`PDF export timed out after ${DEFAULT_TIMEOUT_MS / 1000}s`);
    }
    throw err;
  }
  if (!res.ok) throw new Error(`Export failed: ${res.status}`);
  return res.text();
}

// ── Schedules ──

export async function listSchedules() {
  return request<{ schedules: unknown[] }>('/schedules');
}

export async function createSchedule(data: unknown) {
  return request('/schedules', { method: 'POST', body: JSON.stringify(data) });
}

// ── Webhooks ──

export async function listWebhooks() {
  return request<{ webhooks: unknown[] }>('/webhooks');
}

export async function createWebhook(data: unknown) {
  return request('/webhooks', { method: 'POST', body: JSON.stringify(data) });
}

// ── Tags ──

export async function listTags(targetType?: string, targetId?: string) {
  const params = new URLSearchParams();
  if (targetType) params.set('target_type', targetType);
  if (targetId) params.set('target_id', targetId);
  return request<{ tags: unknown[] }>(`/tags?${params}`);
}

export async function createTag(data: unknown) {
  return request('/tags', { method: 'POST', body: JSON.stringify(data) });
}

// ── Favorites ──

export async function listFavorites() {
  return request<{ favorites: unknown[] }>('/favorites');
}

export async function createFavorite(data: unknown) {
  return request('/favorites', { method: 'POST', body: JSON.stringify(data) });
}

// ── Shares ──

export async function createShare(data: unknown) {
  return request('/shares', { method: 'POST', body: JSON.stringify(data) });
}

export async function resolveShare(token: string) {
  return request(`/shares/${token}`);
}
