// ── FastAPI backend client ──

const BASE = '/api';

async function request<T>(path: string, options: RequestInit = {}): Promise<T> {
  const res = await fetch(`${BASE}${path}`, {
    headers: { 'Content-Type': 'application/json', ...options.headers as Record<string, string> },
    ...options,
  });
  if (!res.ok) {
    const body = await res.text().catch(() => '');
    throw new Error(`API ${res.status}: ${body || res.statusText}`);
  }
  return res.json();
}

export async function parseFlow(file: File) {
  const form = new FormData();
  form.append('file', file);
  const res = await fetch(`${BASE}/parse`, { method: 'POST', body: form });
  if (!res.ok) {
    const body = await res.text().catch(() => '');
    throw new Error(`Parse failed ${res.status}: ${body || res.statusText}`);
  }
  return res.json();
}

export async function analyzeFlow(parsed: unknown) {
  return request('/analyze', {
    method: 'POST',
    body: JSON.stringify({ parsed }),
  });
}

export async function assessFlow(parsed: unknown, analysis: unknown) {
  return request('/assess', {
    method: 'POST',
    body: JSON.stringify({ parsed, analysis }),
  });
}

export async function generateNotebook(data: unknown) {
  return request('/generate', {
    method: 'POST',
    body: JSON.stringify(data),
  });
}

export async function validateNotebook(data: unknown) {
  return request('/validate', {
    method: 'POST',
    body: JSON.stringify(data),
  });
}

export async function generateReport(data: unknown) {
  return request('/report', {
    method: 'POST',
    body: JSON.stringify(data),
  });
}

export async function runFullPipeline(file: File) {
  const form = new FormData();
  form.append('file', file);
  const res = await fetch(`${BASE}/pipeline/run`, { method: 'POST', body: form });
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
